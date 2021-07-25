from datetime import timedelta
import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from zype_provider.operators.zype_operator import ZypeOperator


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "zype",
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["zype_example"],
)
def zype_worflow():
    """
    ### zype DAG

    Showcases the zype provider package's operator.

    To run this example, create a connector with:
    - id: conn_zype
    - type: zype
    - password: api_key
    """

    task_get_videos = ZypeOperator(
        task_id="get_videos",
        zype_conn_id="conn_zype",
        resource="list_videos",
    )

    task_get_videos


zype_worflow_dag = zype_worflow()
