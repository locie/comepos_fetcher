#!/usr/bin/env python
# coding=utf-8

from functools import partial
from warnings import catch_warnings, filterwarnings, warn

import attr
import pandas as pd
from appdirs import user_data_dir
from pandas.io.pytables import PerformanceWarning
from path import Path
from slugify import slugify
from tqdm import tqdm, tqdm_notebook

from box import Box

from .io import VestaWebClient
from .utils import window

MAX_LINE_PER_REQUEST = 100000

appname = "comepos_fetcher"
slugify = partial(slugify, separator="_")


progress_bar = tqdm


def enable_notebook():
    """setup the tqdm progress bar for a notebook session."""
    global progress_bar
    progress_bar = tqdm_notebook


def disable_notebook():
    """setup the tqdm progress bar for a non-interactive session."""
    global progress_bar
    progress_bar = tqdm


def _from_cache_or_fetch(store, key, fetch, *, format="fixed", **kwargs):
    try:
        df = store[key]
    except KeyError:
        df = fetch(**kwargs)
        with catch_warnings():
            filterwarnings("ignore", category=PerformanceWarning)
            store.put(key, df, format=format)
    return df


@attr.s
class Sensor:
    zone = attr.ib()
    device = attr.ib()
    label = attr.ib(repr=False)
    type = attr.ib(repr=False)
    service_name = attr.ib()
    variable_name = attr.ib()
    unique_id = attr.ib(repr=False)
    unit = attr.ib(repr=False)
    historics = attr.ib(type=bool, convert=bool, repr=False)
    slug = attr.ib(repr=False)
    building_id = attr.ib(repr=False)
    building_status = attr.ib(repr=False)
    client = attr.ib(repr=False)
    store = attr.ib(repr=False)

    @property
    def data(self):
        return self._get_data()

    @property
    def last_retrieved_value(self):
        try:
            return self.store[self.key].sort_index().index[-1]
        except KeyError:
            pass

    def refresh(self):
        self.store.append(self.key, self.fetch_new_data())

    def get_online_length(self, start=None):
        return self.client.get_variable_history_size(
            self.building_id, self.service_name, self.variable_name, start
        )

    @property
    def online_length(self):
        return self.client.get_variable_history_size(
            self.building_id, self.service_name, self.variable_name
        )

    @property
    def key(self):
        return f"/{slugify(self.building_id)}/sensors/{self.slug}"

    def __len__(self):
        return len(self.data)

    def _fetch_data(self, since=None):
        f"""Fetch the sensor data.

        If the historic size is more than MAX_LINE_PER_REQUEST ({MAX_LINE_PER_REQUEST})
        the requested period will be sliced to suit this limit.
        """
        n_values = self.get_online_length(start=since)
        if n_values < MAX_LINE_PER_REQUEST:
            return self.client.get_variable_history(
                self.building_id, self.service_name, self.variable_name
            )
        if since is not None:
            period_start = since
        else:
            period_start = self.building_status["first_measurement_date"]
        period_end = self.building_status["last_variable_value_changed_date"]
        n_slices = n_values // MAX_LINE_PER_REQUEST + 1

        date_range = pd.date_range(start=period_start, end=period_end, periods=n_slices)

        all_data = [
            self.client.get_variable_history(
                self.building_id,
                self.service_name,
                self.variable_name,
                slice_start,
                slice_end,
            )
            for slice_start, slice_end in progress_bar(
                window(date_range), total=n_slices - 1, desc=self.slug,
            )
        ]
        return pd.concat(all_data)

    def _get_data(self):
        data = _from_cache_or_fetch(
            store=self.store, key=self.key, fetch=self._fetch_data, format="table",
        )
        data = data.rename(columns={"value": self.slug})
        return data

    def _fetch_new_data(self):
        new_data = self._fetch_data(self.last_retrieved_value)
        new_data = new_data.rename(columns={"value": self.slug})
        return new_data


@attr.s
class ComeposDB:
    username = attr.ib(type=str)
    password = attr.ib(type=str, repr=False)
    web_client = attr.ib(init=False, repr=False)
    store_location = attr.ib(type=Path, default=user_data_dir(appname), convert=Path)
    store = attr.ib(init=False, repr=False)

    @web_client.default
    def client_init(self):
        return VestaWebClient(self.username, self.password)

    @store.default
    def store_init(self):
        self.store_location.makedirs_p()
        return pd.HDFStore(self.store_location / "store.h5")

    @property
    def buildings(self):
        return self.web_client.buildings

    def get_building_db(self, building_id):
        return BuildingDB(
            username=self.username,
            password=self.password,
            building_id=building_id,
            store_location=self.store_location,
        )

    def clean(self):
        self.store.filename.remove()


@attr.s
class BuildingDB:
    username = attr.ib(type=str)
    password = attr.ib(type=str, repr=False)
    building_id = attr.ib(type=str)
    web_client = attr.ib(init=False, repr=False)
    store_location = attr.ib(type=Path, default=user_data_dir(appname), convert=Path)
    store = attr.ib(init=False, repr=False)
    building_info = attr.ib(init=False, repr=False)
    building_status = attr.ib(init=False, repr=False)
    sensors_info = attr.ib(init=False, repr=False)
    sensors = attr.ib(init=False, repr=False)

    @web_client.default
    def _client_init(self):
        return VestaWebClient(self.username, self.password)

    @store.default
    def _store_init(self):
        self.store_location.makedirs_p()
        return pd.HDFStore(self.store_location / "store.h5")

    @building_info.default
    def _building_info_init(self):
        building_info = _from_cache_or_fetch(
            store=self.store,
            key=f"/{slugify(self.building_id)}/building_info",
            fetch=lambda: self.web_client.buildings.loc[self.building_id],
        )
        return building_info

    @building_status.default
    def _building_status_init(self):
        building_status = self.web_client.get_building_status(self.building_id)
        return building_status

    @sensors_info.default
    def _sensors_info_init(self):
        sensors_info = _from_cache_or_fetch(
            store=self.store,
            key=f"/{slugify(self.building_id)}/sensors_info",
            fetch=lambda: self.web_client.get_sensor_list(self.building_id),
        )
        sensors_info["slug"] = sensors_info.unique_id.apply(slugify)
        return sensors_info

    @sensors.default
    def _sensors_init(self):
        for sensor_id, sensor in self.sensors_info.iterrows():
            sensors = Box(
                {
                    sensor.slug: Sensor(
                        **sensor,
                        building_id=self.building_id,
                        building_status=self.building_status,
                        client=self.web_client,
                        store=self.store,
                    )
                    for _, sensor in self.sensors_info.iterrows()
                }
            )
        return sensors

    def refresh_all_sensors(self):
        try:
            for sensor in progress_bar(
                self.sensors.values(), desc="fetch data for all sensors"
            ):
                sensor.refresh()
        except KeyboardInterrupt:
            warn("User interruption. Some data have not been updated.")

    def sensors_data(self):
        return {sensor.slug: sensor.data for sensor in self.sensors.values()}

    def clean(self):
        self.store.remove(f"/{slugify(self.building_id)}")
