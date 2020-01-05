#!/usr/bin/env python
# coding=utf-8

import requests
from functools import partial
from urllib.parse import urljoin
from uplink import Consumer, get, returns, Query
import json
from pandas import read_json, DataFrame
from datetime import datetime
import pendulum
import warnings
from cachetools import cachedmethod
from cachetools.keys import hashkey


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


class VestaWebClient(Consumer):
    base_url = "http://37.187.134.115/VestaEnergy/Application/service/"

    def __init__(self, username: str, password: str):
        super(VestaWebClient, self).__init__(base_url=self.base_url)
        self.username = username
        self.password = password
        self.session.params["token"] = self._get_token()
        self.cache_meta = {}

    def _get_token(self) -> str:
        url = urljoin(self.base_url, "login.php")
        parameters = dict(login=self.username, password=self.password)
        response = requests.get(url=url, params=parameters)
        return response.text

    @property
    @_shared_cachemethod_meta(key="building_list")
    def building_list(self) -> DataFrame:
        """Available buildings as dataframe

        Returns:
            DataFrame -- available buildings
        """
        raw = self._get_building_list()
        return read_json(json.dumps(raw)).set_index("id")

    @_shared_cachemethod_meta(key="zone_list")
    def get_zone_list(self, building_id: str) -> DataFrame:
        """Get the zone for a specific building ID

        Arguments:
            building_id {str} -- id of the req. building (see client.building_list).

        Returns:
            DataFrame -- The zones available for the building id.
        """
        raw = self._get_zone_list(building_id)
        return read_json(json.dumps(raw)).set_index("id")

    @_shared_cachemethod_meta(key="sensor_list")
    def get_sensor_list(self, building_id: str) -> DataFrame:
        """[summary]

        Arguments:
            building_id {str} -- id of the req. building (see client.building_list).

        Returns:
            DataFrame -- The sensors available for the building id.
        """
        raw = self._get_sensor_list(building_id)
        return read_json(json.dumps(raw)).set_index("id")

    @_shared_cachemethod_meta(key="variable_history")
    def get_variable_history(
        self,
        building_id: str,
        service_name: str,
        variable_name: str,
        start: datetime = None,
        end: datetime = None,
    ) -> DataFrame:
        """[summary]

        Arguments:
            building_id {str} -- id of the req. building (see client.building_list).
            service_name {str} -- name of the service (as in client.get_sensor_list).
            variable_name {str} -- name of the variable (as in client.get_sensor_list).

        Keyword Arguments:
            start {datetime, optional} -- starting date for the req. data. Can be either
                a datetime, a parsable string (via pendulum.parse) or a timestamp.
            end {datetime, optional} -- end date for the req. data. Can be either
                a datetime, a parsable string (via pendulum.parse) or a timestamp.

        Returns:
            DataFrame -- Temporal serie for the requested variable.
        """
        start = _infer_datetime(start)
        end = _infer_datetime(end)
        try:
            raw = self._get_variable_history(
                building_id, service_name, variable_name, start, end
            )
            read_json(json.dumps(raw)).set_index("date")
        except json.JSONDecodeError:
            df = DataFrame(columns=["value"])
            df.index.name = "date"
            return df
        except KeyError:
            df = DataFrame(columns=["value"])
            df.index.name = "date"
        return df

    @_shared_cachemethod_meta(key="variable_history_size")
    def get_variable_history_size(
        self,
        building_id: str,
        service_name: str,
        variable_name: str,
        start: datetime = None,
        end: datetime = None,
    ) -> int:
        """[summary]

        Arguments:
            building_id {str} -- id of the req. building (see client.building_list).
            service_name {str} -- name of the service (as in client.get_sensor_list).
            variable_name {str} -- name of the variable (as in client.get_sensor_list).

        Keyword Arguments:
            start {datetime, optional} -- starting date for the req. data. Can be either
                a datetime, a parsable string (via pendulum.parse) or a timestamp.
            end {datetime, optional} -- end date for the req. data. Can be either
                a datetime, a parsable string (via pendulum.parse) or a timestamp.

        Returns:
            int -- number of entry for the requested variable.
        """
        start = _infer_datetime(start)
        end = _infer_datetime(end)
        return self._get_history_size(
            building_id, service_name, variable_name, start, end
        )

    @returns.json
    @get("getBuildingList.php")
    def _get_building_list(self):
        pass

    @returns.json
    @get("getZones.php")
    def _get_zone_list(self, building_id: Query("building")):
        pass

    @returns.json
    @get("getSensors.php")
    def _get_sensor_list(self, building_id: Query("building")):
        pass

    @returns.json
    @get("getSensorHistory.php")
    def _get_variable_history(
        self,
        building_id: Query("building"),
        service_name: Query("serviceName"),
        variable_name: Query("variableName"),
        start: Query = None,
        end: Query = None,
    ):
        pass

    @returns.json
    @get("getSensorHistorySize.php")
    def _get_history_size(
        self,
        building_id: Query("building"),
        service_name: Query("serviceName"),
        variable_name: Query("variableName"),
        start: Query = None,
        end: Query = None,
    ):
        pass
