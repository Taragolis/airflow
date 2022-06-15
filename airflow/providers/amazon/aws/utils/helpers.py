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
import re
from typing import Optional

AWS_PARTITIONS = ("aws", "aws-cn", "aws-us-gov", "aws-iso", "aws-iso-b")


@functools.lru_cache(maxsize=None)
def resolve_aws_partition(region_name: Optional[str] = None, aws_partition: Optional[str] = None):
    """:placeholder:"""
    if not aws_partition and not region_name:
        raise ValueError("Either 'aws_partition' and 'region_name' should be provided.")

    if region_name:
        if re.match(r"^(us|eu|ap|sa|ca|me|af)-\w+-\d+$", region_name):
            # AWS Standard
            region_aws_partition = "aws"
        elif region_name.startswith("cn-"):
            # AWS China
            region_aws_partition = "aws-cn"
        elif region_name.startswith("us-gov-"):
            # AWS GovCloud (US)
            region_aws_partition = "aws-us-gov"
        # Partitions below classified as AWS (Top) Secret and unlikely will appear.
        elif region_name.startswith("us-iso-"):
            region_aws_partition = "aws-iso"
        elif region_name.startswith("us-iso-b-"):
            region_aws_partition = "aws-iso-b"
        else:
            raise ValueError(f"Can't find partition for region_name {region_name!r}.")

        if aws_partition and aws_partition != region_aws_partition:
            raise ValueError(
                f"Region {region_name!r} ({region_aws_partition}) "
                f"not included in partition {aws_partition!r}."
            )
        elif not aws_partition:
            aws_partition = region_aws_partition

    if aws_partition not in AWS_PARTITIONS:
        raise ValueError(f"partition expected one of {AWS_PARTITIONS}, got {aws_partition!r}.")

    return aws_partition
