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
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Type

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.links.base_aws import BaseAwsLink
    from airflow.utils.context import Context


def persist_aws_link(link_class: Type['BaseAwsLink']) -> Callable:

    print("Actual decorator will required mandatory 'link_class'")
    if link_class:
        print(
            f"Link class {link_class.__module__}.{link_class.__qualname__},"
            f" has classmethod persist: {hasattr(link_class, 'persist')}"
        )

        print(f"Link Name: {link_class.name}, Link Key {link_class.key}")

    def decorated(method: Callable) -> Callable:
        @functools.wraps(method)
        def wrapped(self: BaseOperator, context: "Context", **kwargs) -> Optional[Dict[str, Any]]:
            if not isinstance(self, BaseOperator):
                raise ValueError(
                    f"amazon_link decorator should only be "
                    f"applied to methods of BaseOperator inheritance,"
                    f" got:{self}."
                )

            if not self.do_xcom_push:
                self.log.warning("do_xcom_push sets to False. Extra Link disabled")
                return None

            result: Dict[str, Any] = method(self, context, **kwargs) or {}

            region_name = result.pop("region_name", None)
            aws_partition = result.pop("aws_partition", None)

            if not region_name or not aws_partition:
                hook: AwsBaseHook = result.pop("hook", None)
                if not hook:
                    try:
                        hook = getattr(self, "hook")
                    except AttributeError as ex:
                        raise AirflowException(
                            f"region_name={region_name} or aws_partition={aws_partition} not provided."
                            f"You need provide them or specify 'hook' keyword argument in method:"
                            f"{method!r} of class {self!r}."
                        ) from ex

                region_name = region_name or hook.conn_region_name
                aws_partition = aws_partition or hook.conn_partition

            result["region_name"] = region_name
            result["aws_partition"] = aws_partition

            link_class.persist(**result)

            return result

        return wrapped

    return decorated
