"""Hyperblock of fdistribu: a 4D (x, y, vx, vy) block, aligned to the MPI decomposition.

Along each dimension the hyperblock covers about HYPERBLOCK_FRACTION of the points,
around the middle of the domain. When the local chunks are larger than that along a
dimension, the hyperblock lies inside one chunk; otherwise its extent is rounded to
whole local chunks. All the ranks holding a part of the hyperblock thus hold pieces of
the same shape, which deisa assembles into one array.
"""

import math

# Placeholders, to be tuned
HYPERBLOCK_FRACTION = 0.1
HYPERBLOCK_SPECIES = 0


def _extent(n_global, n_local, fraction):
    """Start and size of the hyperblock along one dimension, and the size of each rank's piece."""
    size = max(1, math.ceil(fraction * n_global)) # target: 10% of the domain
    nchunks = n_global // n_local                 # ranks along that dimension
    if size <= n_local: 
        # fits in one rank's chunk
        start = (nchunks - 1) // 2 * n_local + (n_local - size) // 2
        return start, size, size
    # larger than one rank's chunk: round to a whole chunk, and the start is moved to a boundary
    count = max(1, round(size / n_local))
    return (nchunks - count) // 2 * n_local, count * n_local, n_local


def hyperblock_slices(global_shape, local_shape, fraction=HYPERBLOCK_FRACTION):
    """Global slices of the hyperblock along x, y, vx and vy."""
    slices = []
    for n_global, n_local in zip(global_shape[1:], local_shape[1:]):
        start, size, _ = _extent(n_global, n_local, fraction)
        slices.append(slice(start, start + size))
    return tuple(slices)


def hyperblock_piece(global_shape, local_shape, local_starts, fraction=HYPERBLOCK_FRACTION):
    """This rank's piece of the hyperblock: its deisa layout and its selection in the local
    fdistribu, or (None, None) when the rank holds no part of the hyperblock."""
    species = HYPERBLOCK_SPECIES - local_starts[0]
    if not 0 <= species < local_shape[0]:
        return None, None
    selection = [species]
    layout = {"global_shape": [], "chunk_shape": [], "chunk_position": []}
    for n_global, n_local, local_start in zip(global_shape[1:], local_shape[1:], local_starts[1:]):
        start, size, piece = _extent(n_global, n_local, fraction)
        low, high = max(start, local_start), min(start + size, local_start + n_local)
        if low >= high:
            return None, None
        selection.append(slice(low - local_start, high - local_start))
        layout["global_shape"].append(size)
        layout["chunk_shape"].append(piece)
        layout["chunk_position"].append((low - start) // piece)
    return {key: tuple(value) for key, value in layout.items()}, tuple(selection)
