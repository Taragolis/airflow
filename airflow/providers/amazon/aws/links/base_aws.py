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

import functools
from copy import deepcopy
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Dict, Optional, Type, TypeVar

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.utils.common import (
    AWS_CONSOLE_DOMAINS,
    DEFAULT_AWS_PARTITION,
    resolve_aws_partition,
)
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey
    from airflow.utils.context import Context


class BaseAwsLink(BaseOperatorLink):
    """Base Helper class for constructing AWS Console Link"""

    name: ClassVar[str]
    key: ClassVar[str]
    format_str: ClassVar[str]

    def format_link(self, **kwargs) -> str:
        """
        Format AWS Service Link

        Some AWS Service Link should require additional escaping
        in this case this method should be overridden.
        """
        return self.format_str.format(**kwargs)

    def get_link(
        self,
        operator,
        dttm: Optional[datetime] = None,
        ti_key: Optional["TaskInstanceKey"] = None,
    ) -> str:
        """
        Link to Amazon Web Services Console.

        :param operator: airflow operator
        :param ti_key: TaskInstance ID to return link for
        :param dttm: execution date. Uses for compatibility with Airflow 2.2
        :return: link to external system
        """
        if ti_key is not None:
            conf = XCom.get_value(key=self.key, ti_key=ti_key)
        elif not dttm:
            conf = {}
        else:
            conf = XCom.get_one(
                key=self.key,
                dag_id=operator.dag.dag_id,
                task_id=operator.task_id,
                execution_date=dttm,
            )

        if not conf:
            return ""

        try:
            aws_partition = conf.get("aws_partition", DEFAULT_AWS_PARTITION)
            conf["AWS_CONSOLE_LINK"] = f"https://console.{AWS_CONSOLE_DOMAINS[aws_partition]}"
            return self.format_link(**conf)
        except Exception:
            return ""

    @classmethod
    def persist(
        cls,
        context: "Context",
        operator: "BaseOperator",
        region_name: str,
        aws_partition: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Store link information into XCom"""
        if not operator.do_xcom_push:
            return

        if not region_name:
            raise ValueError("'region_name' should be provided.")

        operator.xcom_push(
            context,
            key=cls.key,
            value={
                "region_name": region_name,
                "aws_partition": resolve_aws_partition(region_name, aws_partition),
                **kwargs,
            },
        )


T = TypeVar("T", bound=Callable)


def aws_link(link_class: Type[BaseAwsLink], *, cached: bool = True):

    if not issubclass(link_class, BaseAwsLink):
        raise TypeError(
            f"aws_link decorator only supports links subclassed from BaseAwsLink, "
            f" got:{link_class.__module__}.{link_class.__qualname__}."
        )

    def decorated(method: Callable) -> Callable:
        def _parse_result(
            self,
            *,
            region_name: Optional[str] = None,
            aws_partition: Optional[str] = None,
            hook: Optional[AwsBaseHook] = None,
            **kwargs,
        ) -> Dict[str, Any]:
            if not region_name:
                try:
                    hook = hook or getattr(self, "hook")
                except AttributeError:
                    raise AirflowException(
                        f"region_name={region_name} not provided."
                        f"You need provide them or specify 'hook' keyword argument in method:"
                        f"{method!r} of class {self!r}."
                    )
                region_name = hook.conn_region_name

            aws_partition = resolve_aws_partition(region_name=region_name, aws_partition=aws_partition)

            kwargs = deepcopy(kwargs)
            kwargs.pop("context", None)

            return {**kwargs, "region_name": region_name, "aws_partition": aws_partition}

        @functools.wraps(method)
        def wrapped(self: BaseOperator, context: "Context", **kwargs) -> Optional[Dict[str, Any]]:
            if cached:
                if method._cache_aws_link:  # type: ignore
                    return None
                method._cache_aws_link = True  # type: ignore

            if not isinstance(self, BaseOperator):
                raise ValueError(
                    f"'aws_link' decorator should only be applied to methods "
                    f"of BaseOperator inheritance, got:{self}."
                )
            elif isinstance(self, BaseSensorOperator) and self.reschedule:
                self.log.warning(
                    "Sensor in mode='reschedule', in this mode XCom based Extra Link %r disabled.",
                    BaseAwsLink.name,
                )
                return None

            if not self.do_xcom_push:
                self.log.warning(
                    "'do_xcom_push' set to False - XCom based Extra Link %r disabled.", BaseAwsLink.name
                )
                return None

            if link_class.name not in self.extra_links:
                self.log.warning("Extra Link %r not associated with operator.", BaseAwsLink.name)
                return None

            try:
                result = method(self, context, **kwargs) or {}
                if result and not isinstance(result, dict):
                    raise TypeError(f"Result of decorated method expected None or dict got {type(result)}")
            except Exception as ex:
                self.log.warning(
                    "Error happen during execution %s.%s.: %s", self.__class__.__name__, method.__name__, ex
                )
                result = None

            if result is None:
                return None

            try:
                result = _parse_result(self, **result)
            except Exception as ex:
                self.log.warning(
                    "Error happen during parse result of execution %s.%s.: %r %s",
                    self.__class__.__name__,
                    method.__name__,
                    result,
                    ex,
                )
                return None

            try:
                link_class.persist(context=context, operator=self, **result)
            except Exception as ex:
                self.log.warning(
                    "Error happen during push to XCom by external link {}: %r %s", link_class.name, result, ex
                )
                return None

            self.log.info("Store External Link %r data to XCom %r", link_class.name, link_class.key)
            return result

        if hasattr(method, "_decorated_aws_link"):
            raise AirflowException(
                f"Expected that method '{method.__name__}' decorate by 'aws_link' only once"
                f" however found that this method already decorated."
            )
        else:
            setattr(method, "_decorated_aws_link", True)

        if cached:
            setattr(method, "_cache_aws_link", False)

        return wrapped

    return decorated
