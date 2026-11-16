"""One HDF5 file per node for fdistribu[species, x, y, vx, vy], written from the chunks each node holds."""

import socket
from collections import defaultdict
from itertools import accumulate
from pathlib import Path

import dask.array as da
import xarray as xr
from distributed import futures_of, wait

DIMS = ("species", "x", "y", "vx", "vy")


def _write_node_file(path, chunks, starts, meshes, attrs):
    """Write the chunks held by one node to one file, one group per chunk with its mesh
    coordinates and its global start indices."""
    mode = "w"
    for chunk, start in zip(chunks, starts):
        coords = {"species": range(start[0], start[0] + chunk.shape[0])}
        for dim, s, n in zip(DIMS[1:], start[1:], chunk.shape[1:]):
            coords[dim] = meshes[dim][s:s + n]
        dataset = xr.Dataset({"fdistribu": (DIMS, chunk)}, coords=coords, attrs=dict(attrs, global_start=list(start)))
        dataset.to_netcdf(path, mode=mode, group="block_" + "_".join(map(str, start)), engine="h5netcdf")
        mode = "a"
    return str(path)


def write_per_node(client, fdistribu, meshes, out_dir, prefix, attrs):
    """Write fdistribu with one file per node, <out_dir>/<prefix>_<hostname>.h5.

    meshes maps x, y, vx, vy to their (dask) mesh arrays. Each node's file is written by
    one of its workers from the chunks already in that node's memory. Blocks until all
    files are written and returns their paths.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    meshes = dict(zip(meshes, da.compute(*meshes.values())))
    offsets = [list(accumulate((0,) + sizes[:-1])) for sizes in fdistribu.chunks]

    fdistribu = client.persist(fdistribu)
    wait(fdistribu)
    futures = futures_of(fdistribu)
    holders = client.who_has(futures)
    hostnames = client.run(socket.gethostname)

    per_node = defaultdict(list)
    for future in futures:
        address = holders[future.key][0]
        per_node[hostnames[address]].append((address, future))

    writes = []
    for hostname, pieces in per_node.items():
        writes.append(client.submit(
            _write_node_file,
            str(out_dir / f"{prefix}_{hostname}.h5"),
            [future for _, future in pieces],
            [tuple(offsets[d][i] for d, i in enumerate(future.key[1:])) for _, future in pieces],
            meshes,
            attrs,
            workers=[pieces[0][0]],
            pure=False,
        ))
    return client.gather(writes)
