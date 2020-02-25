#!/usr/bin/env python
# coding=utf-8

import json
from datetime import datetime
from urllib.parse import urljoin

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from pandas import DataFrame, read_json

from uplink import Consumer, Query, get, retry, returns

from .utils import _infer_datetime, _shared_cachemethod_meta, ensure_camel_columns


class VestaWebClient(Consumer):
    base_url = "http://37.187.134.115/VestaEnergy/Application/service/"

    def __init__(self, username: str, password: str):
        super(VestaWebClient, self).__init__(base_url=self.base_url)
        self.username = username
        self.password = password
        self._get_token()
        # after some time, the session end : this allow to request another token after
        # some time.
        self._scheduler = BackgroundScheduler()
        self._scheduler.add_job(self._get_token, "interval", minutes=30)
        self._scheduler.start()

        # data that do not change often are put in a cache to speed up the requests.
        self.cache_meta = {}

    def _get_token(self) -> str:
        url = urljoin(self.base_url, "login.php")
        parameters = dict(login=self.username, password=self.password)
        response = requests.get(url=url, params=parameters)
        self.session.params["token"] = response.text

    def __del__(self):
        self.logout()
        self._scheduler.shutdown()

    @property
    def token(self):
        return self.session.params["token"]

    @property
    @_shared_cachemethod_meta(key="buildings")
    def buildings(self) -> DataFrame:
        """Available buildings as dataframe

        Returns:
            DataFrame -- available buildings
        """
        raw = self._get_buildings()
        if raw:
            return ensure_camel_columns(read_json(json.dumps(raw)).set_index("id"))
        raise IOError("Empty response from web request.")

    @_shared_cachemethod_meta(key="zone_list")
    def get_zone_list(self, building_id: str) -> DataFrame:
        """Get the zone for a specific building ID

        Arguments:
            building_id {str} -- id of the req. building (see client.buildings).

        Returns:
            DataFrame -- The zones available for the building id.
        """
        raw = self._get_zone_list(building_id)
        if raw:
            return ensure_camel_columns(read_json(json.dumps(raw)).set_index("id"))
        raise IOError("Empty response from web request.")

    def get_building_status(self, building_id: str) -> DataFrame:
        """Get the building status (the date for the first / last acquisition
        and for the last parameter change)

        Arguments:
            building_id {str} -- id of the req. building (see client.buildings).

        Returns:
            DataFrame -- building status
        """
        raw = self._get_status(building_id)
        if raw:
            time_info = {
                key: datetime.fromtimestamp(int(value) / 1000)
                for key, value in raw[0].items()
            }
            return ensure_camel_columns(time_info)
        raise IOError("Empty response from web request.")

    def get_sensor_list(self, building_id: str) -> DataFrame:
        """Get the list of the available sensors avalaible in a building.

        Arguments:
            building_id {str} -- id of the req. building (see client.buildings).

        Returns:
            DataFrame -- The sensors available for the building id.
        """
        raw = self._get_sensor_list(building_id)
        if not raw:
            raise IOError("Empty response from web request.")
        df = ensure_camel_columns(read_json(json.dumps(raw)).set_index("id"))
        df = df.drop(["date", "value"], axis=1)
        return df

    def get_variable_history(
        self,
        building_id: str,
        service_name: str,
        variable_name: str,
        start: datetime = None,
        end: datetime = None,
    ) -> DataFrame:
        """ Get the sensor data historic.

        Arguments:
            building_id {str} -- id of the req. building (see client.buildings).
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
            if raw:
                df = read_json(
                    json.dumps(raw), dtype=float, convert_dates=False
                ).set_index("date")
                df.index = map(datetime.fromtimestamp, df.index / 1000)
            else:
                raise IOError("Empty response from web request.")
        except (json.JSONDecodeError, KeyError, IOError):
            df = DataFrame(columns=["value"], dtype=float)
            df.index.name = "date"

        return df

    def get_variable_history_size(
        self,
        building_id: str,
        service_name: str,
        variable_name: str,
        start: datetime = None,
        end: datetime = None,
    ) -> int:
        """Get the sensor data historic size.

        Arguments:
            building_id {str} -- id of the req. building (see client.buildings).
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

    @get("logout.php")
    def logout(self):
        pass

    @returns.json
    @retry(max_attempts=3)
    @get("getBuildingList.php")
    def _get_buildings(self):
        pass

    @returns.json
    @retry(max_attempts=3)
    @get("getStatus.php")
    def _get_status(self, building_id: Query("building")):
        pass

    @returns.json
    @retry(max_attempts=3)
    @get("getZones.php")
    def _get_zone_list(self, building_id: Query("building")):
        pass

    @returns.json
    @get("getSensors.php")
    def _get_sensor_list(self, building_id: Query("building")):
        pass

    @returns.json
    @retry(max_attempts=3)
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
    @retry(max_attempts=3)
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
