import pendulum
import warnings
from cachetools import cachedmethod
from cachetools.keys import hashkey
import re
from functools import partial
from datetime import datetime


class dotdict(dict):
    """dot.notation access to dictionary attributes"""

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def _shared_cachemethod_meta(key):
    def cache_meta(func):
        return cachedmethod(lambda self: self.cache_meta, key=partial(hashkey, key))(
            func
        )

    return cache_meta


def _infer_datetime(dt):
    warnings.warn("start and end datetime not implemented yet.")
    return None
    if isinstance(dt, datetime):
        return int(dt.timestamp())
    if isinstance(dt, str):
        return int(pendulum.parse(dt).timestamp())
    return dt


def camel_to_snake(name):
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def ensure_camel_columns(df):
    df = df.copy()
    df.columns = [camel_to_snake(col) for col in df.columns]
    return df
