"""One HDF5 file per node for fdistribu[species, x, y, vx, vy], written from the chunks each node holds."""

import socket
from collections import defaultdict
from itertools import accumulate
from pathlib import Path

import dask
import dask.array as da
import xarray as xr

DIMS = ("species", "x", "y", "vx", "vy")


def _write_node_file(path, chunks, starts, meshes, attrs, mode="w"):
    """Write chunks to one file, one group per chunk with its mesh coordinates and its
    global start indices."""
    for chunk, start in zip(chunks, starts):
        coords = {"species": range(start[0], start[0] + chunk.shape[0])}
        for dim, s, n in zip(DIMS[1:], start[1:], chunk.shape[1:]):
            coords[dim] = meshes[dim][s:s + n]
        dataset = xr.Dataset({"fdistribu": (DIMS, chunk)}, coords=coords, attrs=dict(attrs, global_start=list(start)))
        dataset.to_netcdf(path, mode=mode, group="block_" + "_".join(map(str, start)), engine="h5netcdf")
        mode = "a"
    return str(path)


def _append_chunk(chunk, out_dir, prefix, start, meshes, attrs):
    """Append one chunk to the file of the node running this task, under that file's lock."""
    from distributed import Lock

    path = Path(out_dir) / f"{prefix}_{socket.gethostname()}.h5"
    with Lock(f"per-node-hdf5:{path}"):
        _write_node_file(path, [chunk], [start], meshes, attrs, mode="a")
    return str(path)


def write_per_node(client, fdistribu, meshes, out_dir, prefix, attrs):
    """Write fdistribu with one file per node, <out_dir>/<prefix>_<hostname>.h5.

    meshes maps x, y, vx, vy to their (dask) mesh arrays. Each chunk is written by its own
    task, which the scheduler runs where the chunk is, into the file of that node; a lock
    per file makes the workers of a node append to it one at a time. Blocks until all
    chunks are written and returns the file paths.
    """
    from distributed import futures_of

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for stale in out_dir.glob(f"{prefix}_*.h5"):
        stale.unlink()
    meshes = dict(zip(meshes, da.compute(*meshes.values())))
    offsets = [list(accumulate((0,) + sizes[:-1])) for sizes in fdistribu.chunks]

    writes = [
        client.submit(
            _append_chunk,
            future,
            str(out_dir),
            prefix,
            tuple(offsets[d][i] for d, i in enumerate(future.key[1:])),
            meshes,
            attrs,
            pure=False,
        )
        for future in futures_of(client.persist(fdistribu))
    ]
    return sorted(set(client.gather(writes)))


def _write_node_file_ray(path, starts, meshes, attrs, *chunks):
    # Ray hands over numpy arrays only for references passed as top-level arguments
    return _write_node_file(path, list(chunks), starts, meshes, attrs)


def submit_per_node_ray(fdistribu, meshes, out_dir, prefix, attrs):
    """Start writing fdistribu with one file per node, <out_dir>/<prefix>_<hostname>.h5.

    fdistribu must be an array as received from deisa-ray, whose chunks are still in the
    object store of the node that sent them. Each node's file is written by one Ray task
    pinned to that node. Returns at once with the references of the write tasks, which
    ray.get turns into the file paths.
    """
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    meshes = dict(zip(meshes, da.compute(*meshes.values())))
    offsets = [list(accumulate((0,) + sizes[:-1])) for sizes in fdistribu.chunks]

    graph = fdistribu.__dask_graph__()
    keys = list(dask.core.flatten(fdistribu.__dask_keys__()))
    # A deisa-ray ChunkRef with DEISA_DISTRIBUTED_SCHEDULING=1
    refs = [getattr(graph[key], "ref", graph[key]) for key in keys]
    locations = ray.experimental.get_object_locations(refs)
    # Gives a map from Ray node ID to hostname for alive nodes
    hostnames = {node["NodeID"]: node["NodeManagerHostname"] for node in ray.nodes() if node["Alive"]}

    per_node = defaultdict(list)
    for key, ref in zip(keys, refs):
        location = locations.get(ref, {})
        node_ids = location.get("node_ids") or [location.get("spilled_node_id")]
        if node_ids[0] not in hostnames:
            raise RuntimeError(f"no alive node holds chunk {key[1:]} of {fdistribu.name}")
        per_node[node_ids[0]].append((key, ref))

    write = ray.remote(num_cpus=1)(_write_node_file_ray)
    writes = []
    for node_id, pieces in per_node.items():
        writes.append(write.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
        ).remote(
            str(out_dir / f"{prefix}_{hostnames[node_id]}.h5"),
            [tuple(offsets[d][i] for d, i in enumerate(key[1:])) for key, _ in pieces],
            meshes,
            attrs,
            *(ref for _, ref in pieces),
        ))
    return writes
