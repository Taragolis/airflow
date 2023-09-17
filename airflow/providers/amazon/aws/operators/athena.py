#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.triggers.athena import AthenaTrigger
from airflow.providers.amazon.aws.utils.mixin import Boto3Mixin

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AthenaOperator(Boto3Mixin[AthenaHook], BaseOperator):
    """
    An operator that submits a presto query to athena.

    .. note:: if the task is killed while it runs, it'll cancel the athena query that was launched,
        EXCEPT if running in deferrable mode.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AthenaOperator`

    :param query: Presto to be run on athena. (templated)
    :param database: Database to select. (templated)
    :param output_location: s3 path to write the query results into. (templated)
    :param client_request_token: Unique token created by user to avoid multiple executions of same query
    :param workgroup: Athena workgroup in which query will be run. (templated)
    :param query_execution_context: Context in which query need to be run
    :param result_configuration: Dict with path to store results in and config related to encryption
    :param sleep_time: Time (in seconds) to wait between two consecutive calls to check query status on Athena
    :param max_polling_attempts: Number of times to poll for query state before function exits
        To limit task execution time, use execution_timeout.
    :param log_query: Whether to log athena query and other execution params when it's executed.
        Defaults to *True*.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    ui_color = "#44b5e2"
    template_fields: Sequence[str] = ("query", "database", "output_location", "workgroup")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"query": "sql"}
    aws_hook_class = AthenaHook

    def __init__(
        self,
        *,
        query: str,
        database: str,
        output_location: str,
        client_request_token: str | None = None,
        workgroup: str = "primary",
        query_execution_context: dict[str, str] | None = None,
        result_configuration: dict[str, Any] | None = None,
        sleep_time: int = 30,
        max_polling_attempts: int | None = None,
        log_query: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.database = database
        self.output_location = output_location
        self.client_request_token = client_request_token
        self.workgroup = workgroup
        self.query_execution_context = query_execution_context or {}
        self.result_configuration = result_configuration or {}
        self.sleep_time = sleep_time
        self.max_polling_attempts = max_polling_attempts or 999999
        self.query_execution_id: str | None = None
        self.log_query: bool = log_query
        self.deferrable = deferrable

    @property
    def _hook_parameters(self):
        return {**super()._hook_parameters, "log_query": self.log_query}

    def execute(self, context: Context) -> str | None:
        """Run Presto Query on Athena."""
        self.query_execution_context["Database"] = self.database
        self.result_configuration["OutputLocation"] = self.output_location
        self.query_execution_id = self.hook.run_query(
            self.query,
            self.query_execution_context,
            self.result_configuration,
            self.client_request_token,
            self.workgroup,
        )

        if self.deferrable:
            self.defer(
                trigger=AthenaTrigger(
                    self.query_execution_id, self.sleep_time, self.max_polling_attempts, self.aws_conn_id
                ),
                method_name="execute_complete",
            )
        # implicit else:
        query_status = self.hook.poll_query_status(
            self.query_execution_id,
            max_polling_attempts=self.max_polling_attempts,
            sleep_time=self.sleep_time,
        )

        if query_status in AthenaHook.FAILURE_STATES:
            error_message = self.hook.get_state_change_reason(self.query_execution_id)
            raise Exception(
                f"Final state of Athena job is {query_status}, query_execution_id is "
                f"{self.query_execution_id}. Error: {error_message}"
            )
        elif not query_status or query_status in AthenaHook.INTERMEDIATE_STATES:
            raise Exception(
                f"Final state of Athena job is {query_status}. Max tries of poll status exceeded, "
                f"query_execution_id is {self.query_execution_id}."
            )

        return self.query_execution_id

    def execute_complete(self, context, event=None):
        if event["status"] != "success":
            raise AirflowException(f"Error while waiting for operation on cluster to complete: {event}")
        return event["value"]

    def on_kill(self) -> None:
        """Cancel the submitted athena query."""
        if self.query_execution_id:
            self.log.info("Received a kill signal.")
            response = self.hook.stop_query(self.query_execution_id)
            http_status_code = None
            try:
                http_status_code = response["ResponseMetadata"]["HTTPStatusCode"]
            except Exception:
                self.log.exception(
                    "Exception while cancelling query. Query execution id: %s", self.query_execution_id
                )
            finally:
                if http_status_code is None or http_status_code != 200:
                    self.log.error("Unable to request query cancel on athena. Exiting")
                else:
                    self.log.info(
                        "Polling Athena for query with id %s to reach final state", self.query_execution_id
                    )
                    self.hook.poll_query_status(self.query_execution_id, sleep_time=self.sleep_time)
