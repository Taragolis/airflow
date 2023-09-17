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

"""
This module contains different internal mixin classes for internal of Amazon Provider.

.. warning::
    Only for internal usage, this module and all classes might be changed, renamed or removed in the future
    without any further notice.

:meta: private
"""

from __future__ import annotations

import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from deprecated.classic import deprecated
from typing_extensions import final

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

if TYPE_CHECKING:
    from botocore.config import Config


AwsHook = TypeVar("AwsHook", bound=AwsGenericHook)

REGION_MSG = "`region` is deprecated and will be removed in the future. Please use `region_name` instead."


class Boto3Mixin(Generic[AwsHook]):
    """Mixin class for AWS Operators, Sensors, etc.

    .. warning::
        Only for internal usage, this class might be changed, renamed or removed in the future
        without any further notice.

    Examples:
     .. code-block:: python

        from airflow.models import BaseOperator
        from airflow.providers.amazon.aws.hooks.foo_bar import FooBarThinHook, FooBarThickHook
        from airflow.sensors.base import BaseSensorOperator


        class AwsFooBarOperator(Boto3Mixin[FooBarThinHook], BaseOperator):
            aws_hook_class = FooBarThinHook


        class AwsFooBarSensor(Boto3Mixin[FooBarThickHook], BaseSensorOperator):
            aws_hook_class = FooBarThickHook

            def __init__(self, *, spam: str, **kwargs):
                super().__init__(**kwargs)
                self.spam = spam

            @property
            def _hook_parameters(self):
                return {**super()._hook_parameters, "spam": self.spam}

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
    :meta: private
    """

    aws_hook_class: type[AwsHook]

    def __init__(
        self,
        *,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: Config | None = None,
        **kwargs,
    ):
        if "region" in kwargs:
            warnings.warn(REGION_MSG, AirflowProviderDeprecationWarning, stacklevel=3)
            if region_name:
                raise AirflowException("Either `region_name` or `region` can be provided, not both.")
            region_name = kwargs.pop("region")

        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        """Mapping parameters for build boto3-related hook.

        Only required to be overwritten for thick-wrapped Hooks.
        """
        return {
            "aws_conn_id": self.aws_conn_id,
            "region_name": self.region_name,
            "verify": self.verify,
            "config": self.botocore_config,
        }

    @cached_property
    @final
    def hook(self) -> AwsHook:
        """
        Return AWS Provider's hook based on ``aws_hook_class``.

        This method implementation should be taken as a final, which a good for
        thin-wrapped Hooks around boto3, for thick-wrapped Hooks around boto3 developer
        should consider to overwrite ``_hook_parameters`` method instead.
        """
        return self.aws_hook_class(**self._hook_parameters)

    @property
    @deprecated(reason=REGION_MSG, category=AirflowProviderDeprecationWarning)
    @final
    def region(self) -> str | None:
        """Alias for ``region_name``, uses for compatibility (deprecated)."""
        return self.region_name
