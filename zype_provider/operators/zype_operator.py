from typing import Any, Callable, Dict, Literal, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from zype_provider.hooks.zype_hook import ZypeHook


class ZypeOperator(BaseOperator):
    """
    Calls a resource on Zype system .

    :param zype_conn_id: connection to run the operator with
    :type zype_conn_id: str
    :param resource: the endpoint to be called ['list_consumers','list_stream_hours', 'list_subscriptions', 'list_videos']
    :type resource: str
    :request_kwargs: the request.Request arguments
    :type request_kwargs: Any
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        "resource",
        "request_kwargs",
    ]
    template_fields_renderers = {"request_kwargs": "json"}
    template_ext = ()
    ui_color = "#f4a460"

    @apply_defaults
    def __init__(
        self,
        *,
        resource: Literal[
            "list_consumers", "list_stream_hours", "list_subscriptions", "list_videos"
        ],
        zype_conn_id: str = "conn_zype",
        max_pages: Optional[int] = None,
        request_kwargs: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.zype_conn_id = zype_conn_id
        self.resource = resource
        self.request_kwargs = request_kwargs
        self.max_pages = max_pages
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> Any:

        hook = ZypeHook(zype_conn_id=self.zype_conn_id)

        self.log.info(f"Calling Zype {self.resource} resource")
        if self.request_kwargs:
            data = hook.run(resource=self.resource, max_pages=self.max_pages,**self.request_kwargs)
        else:
            data = hook.run(resource=self.resource, max_pages=self.max_pages)

        return data
