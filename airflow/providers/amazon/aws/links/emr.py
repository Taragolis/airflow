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
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.links.base_aws import BaseAwsLink, aws_link

if TYPE_CHECKING:
    from airflow.utils.context import Context


class EmrClusterLink(BaseAwsLink):
    """Helper class for constructing AWS EMR Cluster Link"""

    name = "EMR Cluster"
    key = "emr_cluster"
    format_str = "{AWS_CONSOLE_LINK}/elasticmapreduce/home?region={region_name}#cluster-details:{job_flow_id}"


@aws_link(link_class=EmrClusterLink)
def persist_erm_cluster_link(self, context: 'Context', job_flow_id: str, **kwargs):
    return {"job_flow_id": job_flow_id, **kwargs}
