import asyncio
import logging

import dask.array as da
import numpy as np
from distributed import Future

# Instead of "from deisa.dask import Timestep", to keep the __init__.py without circular include
from typing import Hashable
Timestep = Hashable

from dataclasses import dataclass
import math
from dask.highlevelgraph import HighLevelGraph
from collections import defaultdict

logger = logging.getLogger(__name__)

@dataclass
class ChunkRef:
    """
    Represents a chunk of an array in a Dask task graph.

    This class is used as a placeholder in Dask task graphs to represent a
    chunk of data. The task corresponding to this object must be scheduled
    by the actor who has the actual data. This class is used since Dask
    tends to inline simple tuples, which would prevent proper scheduling.

    Parameters
    ----------
    ref : dask.Future
        <Dask Future ?> that eventually points to the chunk data. This is a
        ``ref`` of a ``ref`` produced by the patched Dask scheduler.
    actorid : int
        The ID of the scheduling actor that owns this chunk.
    array_name : str
        The real name of the array, without the timestep suffix.
    timestep : Timestep
        The timestep this chunk belongs to.
    bridge_id : int
        Identifier of the bridge that produced this chunk.

    Notes
    -----
    This class is used to prevent Dask from inlining simple tuples in the
    task graph, which would break the scheduling mechanism. The behavior
    may change in newer versions of Dask.
    """

    ref: Future
    actorid: int
    array_name: str
    timestep: Timestep
    bridge_id: int


@dataclass(frozen=True)
class DeisaArray:
    dask: da.Array
    t: int

    # Enables both "darr, t = obj" and "deisarr = obj", exposed to the user
    def __iter__(self):
        yield self.dask
        yield self.t


class DaskArrayData:
    """
    Information about a Dask array being built.

    Tracks metadata and per-timestep state for a Dask array assembled from
    chunks sent by scheduling actors.

    Parameters
    ----------
    name : str
        Array name registered with the head actor.

    Attributes
    ----------
    name : str
        Array name without timestep suffix.
    fully_defined : asyncio.Event
        Set when every chunk owner has been registered.
    nb_chunks_per_dim : tuple[int, ...] or None
        Number of chunks per dimension. Set when first chunk owner is
        registered.
    nb_chunks : int or None
        Total number of chunks in the array. Set when first chunk owner
        is registered.
    chunks_size : list[list[int | None]] or None
        For each dimension, the size of chunks in that dimension. None
        values indicate unknown chunk sizes.
    dtype : np.dtype or None
        The numpy dtype of the array chunks. Set when first chunk owner
        is registered.
    position_to_node_actorID : dict[tuple[int, ...], int]
        Mapping from chunk position to the scheduling actor responsible
        for that chunk.
    position_to_bridgeID : dict[tuple, int]
        Mapping from chunk position to the producing bridge ID.
    nb_scheduling_actors : int or None
        Number of unique scheduling actors owning chunks of this array.
        Set when all chunk owners are known.
    chunk_refs : dict[Timestep, list[dask.Future]]
        For each timestep, the list of per-actor references that keep
        chunk payloads alive in the object store.
    pos_to_ref_by_timestep : defaultdict
        For each timestep, the (position, ref) pairs provided by scheduling
        actors. Used when distributed scheduling is disabled.
    """

    def __init__(self, name) -> None:
        """
        Initialise per-array metadata containers.

        Parameters
        ----------
        name : str
            Array name as registered with the head actor.

        """
        self.name = name

        # This will be set when we know, for each chunk, the scheduling actor in charge of it.
        self.fully_defined: asyncio.Event = asyncio.Event()

        # This will be set when the first chunk is added
        self.nb_chunks_per_dim: tuple[int, ...] | None = None
        self.nb_chunks: int | None = None

        # For each dimension, the size of the chunks in this dimension
        self.chunks_size: list[list[int | None]] | None = None

        # Type of the numpy arrays
        self.dtype: np.dtype | None = None

        # ID of the scheduling actor in charge of the chunk at each position
        self.position_to_node_actorID: dict[tuple[int, ...], int] = {}
        self.position_to_bridgeID: dict[tuple, int] = {}

        # Number of scheduling actors owning chunks of this array.
        self.nb_scheduling_actors: int | None = None

        # Each reference comes from one scheduling actor. The reference a list of
        # Futures, each Future corresponding to a chunk. These references
        # shouldn't be used directly. They exists only to release the memory
        # automatically.
        # When the array is buit, these references are put in the object store, and the
        # global reference is added to the Dask graph. Then, the list is cleared.
        self.chunk_refs: dict[Timestep, list[Future]] = {}

        self.pos_to_ref_by_timestep: dict[Timestep, list[tuple[tuple, Future]]] = defaultdict(list)

    def update_meta(
        self,
        nb_chunks_per_dim: tuple[int, ...],
        dtype: np.dtype,
        position: tuple[int, ...],
        size: tuple[int, ...],
        node_actor_id: int,
        bridge_id: int,
    ) -> None:
        """
        Register a scheduling actor as the owner of a chunk at a specific position.

        This method records which scheduling actor is responsible for a chunk
        and updates the array metadata. If this is the first chunk registered,
        it initializes the array dimensions and dtype.

        Parameters
        ----------
        nb_chunks_per_dim : tuple[int, ...]
            Number of chunks per dimension in the array decomposition.
        dtype : np.dtype
            The numpy dtype of the chunk.
        position : tuple[int, ...]
            The position of the chunk in the array decomposition.
        size : tuple[int, ...]
            The size of the chunk along each dimension.
        node_actor_id : int
            Scheduling actor that owns this chunk.
        bridge_id : int
            Bridge identifier that produced the chunk (used for lookups).

        Raises
        ------
        AssertionError
            If the chunk position is out of bounds, or if subsequent chunks
            have inconsistent dimensions, dtype, or sizes compared to the
            first chunk.
        """
        # TODO should be done just once
        if self.nb_chunks_per_dim is None:
            self.nb_chunks_per_dim = nb_chunks_per_dim
            self.nb_chunks = math.prod(nb_chunks_per_dim)

            self.dtype = dtype
            self.chunks_size = [[None for _ in range(n)] for n in nb_chunks_per_dim]
        else:
            assert self.nb_chunks_per_dim == nb_chunks_per_dim
            assert self.dtype == dtype
            assert self.chunks_size is not None

        # TODO this actually should be done each time
        self.position_to_node_actorID[position] = node_actor_id
        self.position_to_bridgeID[position] = bridge_id
        for i, pos in enumerate(position):
            if self.chunks_size[i][pos] is None:
                # NOTE : Verify this
                self.chunks_size[i][pos] = size[i]
            else:
                assert self.chunks_size[i][pos] == size[i]

    # Duplicated logic of Deisa.get_array() ? Should use this one from inside Deisa.get_array() ?
    def get_full_array(self, timestep: Timestep, *, distributing_scheduling_enabled: bool) -> da.Array:
        """
        Return the full Dask array for a given timestep.

        Parameters
        ----------
        timestep : Timestep
            The timestep for which the full array should be returned.

        Returns
        -------
        da.Array
            A Dask array representing the full decomposed array for the given
            timestep. The array is constructed from chunk references stored
            in <Dask ?>'s object store.

        Raises
        ------
        AssertionError
            If not all chunk owners have been registered, or if the number
            of chunks is inconsistent.

        Notes
        -----
        When distributed scheduling is enabled the graph uses :class:`ChunkRef`
        placeholders that keep data owner information. Otherwise the concrete
        chunk payloads are inlined. Chunk reference lists are deleted after
        embedding in the graph to avoid leaking memory.
        """
        assert len(self.position_to_node_actorID) == self.nb_chunks
        assert self.nb_chunks is not None and self.nb_chunks_per_dim is not None

        # We need to add the timestep since the same name can be used several times for different
        # timesteps
        dask_name = f"{self.name}_{timestep}"

        del self.chunk_refs[timestep]

        if distributing_scheduling_enabled:
            graph = {
                (dask_name,) + position: ChunkRef(
                    ref=ref,
                    actorid=self.position_to_node_actorID[
                        next((pos for pos, r in self.pos_to_ref_by_timestep[timestep] if r == ref), None)
                    ],
                    array_name=self.name,
                    timestep=timestep,
                    bridge_id=self.position_to_bridgeID[position],
                )
                for position, ref in self.pos_to_ref_by_timestep[timestep]
            }
        else:
            graph = {(dask_name,) + position: ref for position, ref in self.pos_to_ref_by_timestep[timestep]}

        # Needed for prepare iteration otherwise key lookup fails since iteration does not yet exist
        # TODO ensure flow is as expected
        self.pos_to_ref_by_timestep.pop(timestep, None)

        dsk = HighLevelGraph.from_collections(dask_name, graph, dependencies=())

        full_array = da.Array(
            dsk,
            dask_name,
            chunks=self.chunks_size,
            dtype=self.dtype,
        )

        return full_array
