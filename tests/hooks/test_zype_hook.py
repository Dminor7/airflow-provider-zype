"""
Unittest module to test Hooks.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python -m unittest tests.hooks.test_zype_hook.TestZypeHook

"""

import logging
import os
import unittest
from unittest import mock

# Import Hook
from zype_provider.hooks.zype_hook import ZypeHook


log = logging.getLogger(__name__)


# Mock the `conn_zype` Airflow connection
@mock.patch.dict("os.environ", AIRFLOW_CONN_CONN_ZYPE="zype://:<YOUR_API_KEY>@")
class TestZypeHook(unittest.TestCase):
    """
    Test Zype Hook.
    """

    def test_videos(self):

        # Instantiate hook
        hook = ZypeHook(zype_conn_id="conn_zype")

        # Zype Hook's run method executes an API call
        data = hook.run(resource="list_videos", max_pages=1)

        assert "_id" in data[0]

    def test_videos_with_params(self):

        # Instantiate hook
        hook = ZypeHook(zype_conn_id="conn_zype")

        # Zype Hook's run method executes an API call
        data = hook.run(resource="list_videos", max_pages=1, params={"per_page": 20})

        assert len(data) == 20


if __name__ == "__main__":
    unittest.main()
