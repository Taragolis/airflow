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

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.providers.amazon.aws.utils.mixin import Boto3Mixin

if TYPE_CHECKING:
    from airflow.utils.context import Context


class _BaseEc2ResourceTypeOperator(Boto3Mixin[EC2Hook], BaseOperator):
    """Base Amazon Elastic Compute Cloud (EC2) Operator, use boto3.resource("ec2")."""

    aws_hook_class = EC2Hook

    def _hook_parameters(self):
        return {**super()._hook_parameters, "api_type": "resource_type"}


class _BaseEc2ClientTypeOperator(Boto3Mixin[EC2Hook], BaseOperator):
    """Base Amazon Elastic Compute Cloud (EC2) Operator, use boto3.client("ec2")."""

    aws_hook_class = EC2Hook

    def _hook_parameters(self):
        return {**super()._hook_parameters, "api_type": "client_type"}


class EC2StartInstanceOperator(_BaseEc2ResourceTypeOperator):
    """
    Start AWS EC2 instance using boto3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EC2StartInstanceOperator`

    :param instance_id: id of the AWS EC2 instance
    :param check_interval: time in seconds that the job should wait in
        between each instance state checks until operation is completed
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

    template_fields: Sequence[str] = ("instance_id", "region_name")
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(self, *, instance_id: str, check_interval: float = 15, **kwargs):
        super().__init__(**kwargs)
        self.instance_id = instance_id
        self.check_interval = check_interval

    def execute(self, context: Context):
        self.log.info("Starting EC2 instance %s", self.instance_id)
        self.hook.get_instance(instance_id=self.instance_id).start()
        self.hook.wait_for_state(
            instance_id=self.instance_id,
            target_state="running",
            check_interval=self.check_interval,
        )


class EC2StopInstanceOperator(_BaseEc2ResourceTypeOperator):
    """
    Stop AWS EC2 instance using boto3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EC2StopInstanceOperator`

    :param instance_id: id of the AWS EC2 instance
    :param check_interval: time in seconds that the job should wait in
        between each instance state checks until operation is completed
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

    template_fields: Sequence[str] = ("instance_id", "region_name")
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(self, *, instance_id: str, check_interval: float = 15, **kwargs):
        super().__init__(**kwargs)
        self.instance_id = instance_id
        self.check_interval = check_interval

    def execute(self, context: Context):
        self.log.info("Stopping EC2 instance %s", self.instance_id)
        self.hook.get_instance(instance_id=self.instance_id).stop()
        self.hook.wait_for_state(
            instance_id=self.instance_id,
            target_state="stopped",
            check_interval=self.check_interval,
        )


class EC2CreateInstanceOperator(_BaseEc2ClientTypeOperator):
    """
    Create and start a specified number of EC2 Instances using boto3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EC2CreateInstanceOperator`

    :param image_id: ID of the AMI used to create the instance.
    :param max_count: Maximum number of instances to launch. Defaults to 1.
    :param min_count: Minimum number of instances to launch. Defaults to 1.
    :param poll_interval: Number of seconds to wait before attempting to
        check state of instance. Only used if wait_for_completion is True. Default is 20.
    :param max_attempts: Maximum number of attempts when checking state of instance.
        Only used if wait_for_completion is True. Default is 20.
    :param config: Dictionary for arbitrary parameters to the boto3 run_instances call.
    :param wait_for_completion: If True, the operator will wait for the instance to be
        in the `running` state before returning.
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

    template_fields: Sequence[str] = (
        "image_id",
        "max_count",
        "min_count",
        "aws_conn_id",
        "region_name",
        "config",
        "wait_for_completion",
    )

    def __init__(
        self,
        image_id: str,
        max_count: int = 1,
        min_count: int = 1,
        poll_interval: int = 20,
        max_attempts: int = 20,
        config: dict | None = None,
        wait_for_completion: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.image_id = image_id
        self.max_count = max_count
        self.min_count = min_count
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self.config = config or {}
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context):
        instances = self.hook.conn.run_instances(
            ImageId=self.image_id,
            MinCount=self.min_count,
            MaxCount=self.max_count,
            **self.config,
        )["Instances"]
        instance_ids = []
        for instance in instances:
            instance_ids.append(instance["InstanceId"])
            self.log.info("Created EC2 instance %s", instance["InstanceId"])

            if self.wait_for_completion:
                self.hook.get_waiter("instance_running").wait(
                    InstanceIds=[instance["InstanceId"]],
                    WaiterConfig={
                        "Delay": self.poll_interval,
                        "MaxAttempts": self.max_attempts,
                    },
                )

        return instance_ids


class EC2TerminateInstanceOperator(_BaseEc2ClientTypeOperator):
    """
    Terminate EC2 Instances using boto3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EC2TerminateInstanceOperator`

    :param instance_id: ID of the instance to be terminated.
    :param poll_interval: Number of seconds to wait before attempting to
        check state of instance. Only used if wait_for_completion is True. Default is 20.
    :param max_attempts: Maximum number of attempts when checking state of instance.
        Only used if wait_for_completion is True. Default is 20.
    :param wait_for_completion: If True, the operator will wait for the instance to be
        in the `terminated` state before returning.
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

    template_fields: Sequence[str] = ("instance_ids", "region_name", "aws_conn_id", "wait_for_completion")

    def __init__(
        self,
        instance_ids: str | list[str],
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        poll_interval: int = 20,
        max_attempts: int = 20,
        wait_for_completion: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.instance_ids = instance_ids
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context):
        if isinstance(self.instance_ids, str):
            self.instance_ids = [self.instance_ids]
        self.hook.conn.terminate_instances(InstanceIds=self.instance_ids)

        for instance_id in self.instance_ids:
            self.log.info("Terminating EC2 instance %s", instance_id)
            if self.wait_for_completion:
                self.hook.get_waiter("instance_terminated").wait(
                    InstanceIds=[instance_id],
                    WaiterConfig={
                        "Delay": self.poll_interval,
                        "MaxAttempts": self.max_attempts,
                    },
                )
