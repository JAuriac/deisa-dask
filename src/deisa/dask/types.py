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


@dataclass(frozen=True)
class DeisaArray:
    dask: da.Array
    t: int

    # Enables both "darr, t = obj" and "deisarr = obj", exposed to the user
    def __iter__(self):
        yield self.dask
        yield self.t
