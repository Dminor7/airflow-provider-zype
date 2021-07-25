"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python -m unittest tests.operators.test_zype_operator.TestZypeOperator

"""

import json
import logging
import os
import unittest
from unittest import mock

# Import Operator
from zype_provider.operators.zype_operator import ZypeOperator


log = logging.getLogger(__name__)


# Mock the `conn_zype` Airflow connection
@mock.patch.dict("os.environ", AIRFLOW_CONN_CONN_ZYPE="zype://:<YOUR_API_KEY>@")
class TestZypeOperator(unittest.TestCase):
    """
    Test Zype Operator.
    """

    def test_operator(self):
        per_page = 30

        operator = ZypeOperator(
            task_id="run_operator",
            zype_conn_id="conn_zype",
            resource="list_videos",
            request_kwargs={"params": {"per_page": per_page}},
        )

        # Airflow calls the operator's execute method at runtime with the task run's bespoke context dictionary
        data = operator.execute(context={})

        log.info("Execution Complete")

        # Assert the API call returns expected mocked payload
        assert len(data) == per_page


if __name__ == "__main__":
    unittest.main()
