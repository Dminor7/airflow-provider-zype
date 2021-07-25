"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python -m unittest tests.operators.test_zype_to_gcs.TestZypeToGCSOperator

"""
import os
import unittest
from glob import glob
from unittest import mock
import datetime
import pendulum


from airflow.models.dag import DAG

from zype_provider.operators.zype_to_gcs import ZypeToGCSOperator


class TestZypeToGCSOperator(unittest.TestCase):
    ts_nodash = pendulum.now().strftime("%Y%m%dT%H%M%S")
    _config = {
        "resource": "list_videos",
        "max_pages": 1,
        "dest_gcs": "gs://dummy/zype/",
    }
    _upload_config = {"bucket_name": "dummy", "object_name": f"zype/{ts_nodash}.json"}

    def setUp(self):
        args = {"owner": "airflow", "start_date": datetime.datetime(2021, 7, 25)}
        self.dag = DAG("test_dag_id", default_args=args)

    def test_init(self):
        operator = ZypeToGCSOperator(
            task_id="file_to_gcs_operator", dag=self.dag, **self._config
        )
        assert operator.resource == self._config["resource"]
        assert operator.max_pages == self._config["max_pages"]
        assert operator.dest_gcs == self._config["dest_gcs"]

    @mock.patch("zype_provider.operators.zype_to_gcs.ZypeHook", autospec=True)
    @mock.patch("zype_provider.operators.zype_to_gcs.GCSHook", autospec=True)
    def test_execute(self, gcs_mock_hook, zype_mock_hook):
        gcs_mock_instance = gcs_mock_hook.return_value
        zype_mock_instance = zype_mock_hook.return_value

        operator = ZypeToGCSOperator(
            task_id="file_to_gcs_operator", dag=self.dag, **self._config
        )

        operator.execute({"ts_nodash": self.ts_nodash})

        zype_mock_instance.run.assert_called_once_with(
            resource=self._config["resource"], max_pages=self._config["max_pages"]
        )

        gcs_mock_instance.upload.assert_called_once_with(
            data="", mime_type="application/json", timeout=120, **self._upload_config
        )
