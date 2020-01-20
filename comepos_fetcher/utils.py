import re
from datetime import datetime
from functools import partial
from itertools import islice

from cachetools import cachedmethod
from cachetools.keys import hashkey
from pandas import DataFrame

import pendulum


def window(seq, n=2):
    "Returns a sliding window (of width n) over data from the iterable"
    "   s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...                   "
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result


def _shared_cachemethod_meta(key):
    def cache_meta(func):
        return cachedmethod(lambda self: self.cache_meta, key=partial(hashkey, key))(
            func
        )

    return cache_meta


def _infer_datetime(dt):
    if isinstance(dt, datetime):
        return int(dt.timestamp() * 1000)
    if isinstance(dt, str):
        return int(pendulum.parse(dt).timestamp() * 1000)
    return dt


def camel_to_snake(name):
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def ensure_camel_columns(data):
    if isinstance(data, DataFrame):
        data = data.copy()
        data.columns = [camel_to_snake(col) for col in data.columns]
    else:
        data = {camel_to_snake(key): value for key, value in data.items()}
    return data
