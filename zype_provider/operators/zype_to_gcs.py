from typing import Any, Callable, Dict, Iterable, Literal, Optional, Sequence, Union
import ndjson

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from zype_provider.hooks.zype_hook import ZypeHook
from airflow.providers.google.cloud.hooks.gcs import (
    GCSHook,
    gcs_object_is_directory,
    _parse_gcs_url,
)


class ZypeToGCSOperator(BaseOperator):
    """
    Calls a resource on Zype system .

    :param zype_conn_id: connection to run the operator with
    :type zype_conn_id: str
    :param resource: the endpoint to be called ['list_consumers','list_stream_hours', 'list_subscriptions', 'list_videos']
    :type resource: str
    :request_kwargs: the request.Request arguments
    :type request_kwargs: Any
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param dest_gcs: The destination Google Cloud Storage bucket and prefix
        where you want to store the files. (templated)
    :type dest_gcs: str
    :param dest_gcs_file: The destination Google Cloud Storage file name. (templated)
    :type dest_gcs_file: str
    :param delegate_to: Google account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param google_impersonation_chain: Optional Google service account to impersonate using
        short-term credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type google_impersonation_chain: Union[str, Sequence[str]]
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = (
        "dest_gcs",
        "dest_gcs_file",
        "google_impersonation_chain",
        "resource",
        "request_kwargs",
    )
    template_fields_renderers = {"request_kwargs": "json"}
    template_ext = ()
    ui_color = "#e09411"

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
        gcp_conn_id="google_cloud_default",
        dest_gcs=None,
        dest_gcs_file=None,
        delegate_to=None,
        google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs: Any,
    ) -> None:

        super().__init__(**kwargs)
        self.zype_conn_id = zype_conn_id
        self.resource = resource
        self.request_kwargs = request_kwargs
        self.max_pages = max_pages
        self.gcp_conn_id = gcp_conn_id
        self.dest_gcs = dest_gcs
        self.dest_gcs_file = dest_gcs_file
        self.delegate_to = delegate_to
        self.google_impersonation_chain = google_impersonation_chain

        if dest_gcs and not gcs_object_is_directory(self.dest_gcs):
            self.log.info(
                "Destination Google Cloud Storage path is not a valid "
                '"directory", define a path that ends with a slash "/" or '
                "leave it empty for the root of the bucket."
            )
            raise AirflowException(
                'The destination Google Cloud Storage path must end with a slash "/" or be empty.'
            )

        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> Any:
        data = []
        hook = ZypeHook(zype_conn_id=self.zype_conn_id)
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.google_impersonation_chain,
        )

        self.log.info(f"Calling Zype {self.resource} resource")
        if self.request_kwargs:
            data = hook.run(
                resource=self.resource, max_pages=self.max_pages, **self.request_kwargs
            )
        else:
            data = hook.run(resource=self.resource, max_pages=self.max_pages)

        if data:
            if not self.dest_gcs_file:
                self.dest_gcs_file = f"{context.get('ts_nodash')}.json"

            dest_gcs_bucket, dest_gcs_object_prefix = _parse_gcs_url(self.dest_gcs)
            dest_gcs_object = dest_gcs_object_prefix + self.dest_gcs_file

            gcs_hook.upload(
                bucket_name=dest_gcs_bucket,
                object_name=dest_gcs_object,
                data=ndjson.dumps(data),
                mime_type="application/json",
                timeout=120,
            )

            self.log.info(
                f"All done, uploaded data to bucket: {dest_gcs_bucket}, object: {dest_gcs_object}"
            )
