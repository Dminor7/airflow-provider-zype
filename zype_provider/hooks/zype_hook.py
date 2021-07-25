from typing import Any, Callable, Dict, Literal, Optional, Union

import requests
from sqlalchemy.ext.declarative import api
import tenacity
from requests.auth import HTTPBasicAuth

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from zype import Zype
from zype.zype_instantiator import ZypeClient

RESOURCE_API_ROOT = {"list_stream_hours": "https://analytics.zype.com"}


class ZypeHook(BaseHook):
    """
    zype Hook that interacts with an HTTP endpoint the Python requests library.

    :param zype_conn_id: The reference to zype api_key connection
    :type zype_conn_id: str
    """

    conn_name_attr = "zype_conn_id"
    default_conn_name = "zype_default"
    conn_type = "zype"
    hook_name = "Zype"

    def __init__(
        self,
        zype_conn_id: str = default_conn_name,
    ) -> None:
        super().__init__()
        self.zype_conn_id = zype_conn_id
        self.conn = self.get_connection(self.zype_conn_id)

    def get_conn(self, api_root: str = "https://api.zype.com") -> ZypeClient:
        """
        :param api_root: The zype base url defaults to https://api.zype.com
        :type api_root: str
        Returns ZypeClient.
        """
        if self.conn.password:
            return self._auth_via_api_key(api_root=api_root)
        raise NotImplementedError(
            "No Authentication method found for given Credentials! Define api_key under password"
        )

    def _auth_via_api_key(self, api_root: str) -> ZypeClient:
        """
        :param api_root: The zype base url defaults to https://api.zype.com
        :type api_root: str
        Returns ZypeClient.
        """
        client = Zype(api_key=self.conn.password, api_root=api_root)
        return client

    def run(
        self,
        resource: Literal[
            "list_consumers", "list_stream_hours", "list_subscriptions", "list_videos"
        ],
        max_pages: Optional[int] = None,
        **request_kwargs: Any,
    ) -> Any:
        r"""
        Performs the request

        :param resource: the endpoint to be called ['list_consumers','list_stream_hours', 'list_subscriptions', 'list_videos']
        :type resource: str

        """
        if resource in RESOURCE_API_ROOT.keys():
            api_root = RESOURCE_API_ROOT[resource]
            client = self.get_conn(api_root=api_root)
        else:
            client = self.get_conn()

        if request_kwargs:
            res = getattr(client, resource)().get(**request_kwargs)
        else:
            res = getattr(client, resource)().get()

        data = [resp().data for resp in res().pages(max_pages=max_pages)]
        return data
