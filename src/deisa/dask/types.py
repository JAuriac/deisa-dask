import logging

import dask.array as da

from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeisaArray:
    dask: da.Array
    t: int

    # Enables both "darr, t = obj" and "deisarr = obj", exposed to the user
    def __iter__(self):
        yield self.dask
        yield self.t
