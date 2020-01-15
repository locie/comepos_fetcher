#!/usr/bin/env python
# coding=utf-8

import attr
from functools import partial
from path import Path
from appdirs import user_data_dir
import pandas as pd
from slugify import slugify
from warnings import catch_warnings, filterwarnings
from pandas.io.pytables import PerformanceWarning

from .utils import dotdict
from .io import VestaWebClient

appname = "comepos_fetcher"
slugify = partial(slugify, separator="_")


def from_cache_or_fetch(store, key, fetch):
    try:
        df = store[key]
    except KeyError:
        df = fetch()
        with catch_warnings():
            filterwarnings("ignore", category=PerformanceWarning)
            store[key] = df
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
    date = attr.ib(repr=False)
    value = attr.ib(repr=False)
    unit = attr.ib(repr=False)
    historics = attr.ib(type=bool, convert=bool, repr=False)
    slug = attr.ib(repr=False)
    building_id = attr.ib(repr=False)
    client = attr.ib(repr=False)
    store = attr.ib(repr=False)

    @property
    def data(self):
        data = from_cache_or_fetch(
            store=self.store,
            key=f"/{slugify(self.building_id)}/sensors/{self.slug}",
            fetch=lambda: self.client.get_variable_history(
                self.building_id, self.service_name, self.variable_name
            ),
        )["value"]
        data.name = self.slug
        return data

    @property
    def online_length(self):
        return self.client.get_variable_history_size(
            self.building_id, self.service_name, self.variable_name
        )


@attr.s
class ComeposDB:
    username = attr.ib(type=str)
    password = attr.ib(type=str)
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
    password = attr.ib(type=str)
    building_id = attr.ib(type=str)
    web_client = attr.ib(init=False, repr=False)
    store_location = attr.ib(type=Path, default=user_data_dir(appname), convert=Path)
    store = attr.ib(init=False, repr=False)
    building_info = attr.ib(init=False, repr=False)
    sensors_info = attr.ib(init=False, repr=False)
    sensors = attr.ib(init=False, repr=False)

    @web_client.default
    def client_init(self):
        return VestaWebClient(self.username, self.password)

    @store.default
    def store_init(self):
        self.store_location.makedirs_p()
        return pd.HDFStore(self.store_location / "store.h5")

    @building_info.default
    def building_info_init(self):
        building_info = from_cache_or_fetch(
            store=self.store,
            key=f"/{slugify(self.building_id)}/building_info",
            fetch=lambda: self.web_client.buildings.loc[self.building_id],
        )
        return building_info

    @sensors_info.default
    def sensors_info_init(self):
        sensors_info = from_cache_or_fetch(
            store=self.store,
            key=f"/{slugify(self.building_id)}/sensors_info",
            fetch=lambda: self.web_client.get_sensor_list(self.building_id),
        )
        sensors_info["slug"] = sensors_info.unique_id.apply(slugify)
        return sensors_info

    @sensors.default
    def sensors_init(self):
        for sensor_id, sensor in self.sensors_info.iterrows():
            sensors = dotdict(
                {
                    sensor.slug: Sensor(
                        **sensor,
                        building_id=self.building_id,
                        client=self.web_client,
                        store=self.store,
                    )
                    for _, sensor in self.sensors_info.iterrows()
                }
            )
        return sensors

    def clean(self):
        self.store.remove(f"/{slugify(self.building_id)}")
