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

import pytest

from airflow.providers.amazon.aws.utils.common import AWS_PARTITIONS, resolve_aws_partition


class TestResolvePartition:
    @pytest.mark.parametrize("region_name", ["", None])
    @pytest.mark.parametrize("aws_partition", ["", None])
    def test_args_not_provided(self, region_name, aws_partition):
        with pytest.raises(ValueError, match="Either 'aws_partition' and 'region_name' should be provided."):
            resolve_aws_partition(region_name=region_name, aws_partition=aws_partition)

    def test_wrong_partition(self):
        with pytest.raises(ValueError, match="^.*partition expected one of.*got.*$"):
            resolve_aws_partition(aws_partition="aws-moon")

    def test_wrong_region(self):
        pattern = "^.*Can't find partition for region_name.*$"
        with pytest.raises(ValueError, match=pattern):
            resolve_aws_partition(region_name="c6.48xlarge")

        with pytest.raises(ValueError, match=pattern):
            resolve_aws_partition(region_name="db.t4.medium")

    @pytest.mark.parametrize("aws_partition", AWS_PARTITIONS)
    def test_valid_partition(self, aws_partition):
        assert resolve_aws_partition(aws_partition=aws_partition) == aws_partition

    @pytest.mark.parametrize(
        "region_name,aws_partition",
        [
            ("us-west-42", "aws"),  # We do not validate that this region actually exists
            ("eu-west-1", "aws"),
            ("ca-central-1", "aws"),
            ("ap-southeast-2", "aws"),
            ("sa-east-1", "aws"),
            ("ca-central-1", "aws"),
            ("me-south-1", "aws"),
            ("af-south-1", "aws"),
            ("cn-north-pole-1", "aws-cn"),
            ("us-gov-wild-west-1", "aws-us-gov"),
            ("us-iso-unknown-1", "aws-iso"),
            ("us-isob-somewhere-1", "aws-iso-b"),
        ],
    )
    def test_resolve_by_region_name(self, region_name, aws_partition):
        # No AWS Partition
        assert resolve_aws_partition(region_name=region_name) == aws_partition

        # Validate that region_name within correct aws_partition
        assert resolve_aws_partition(region_name=region_name, aws_partition=aws_partition) == aws_partition

    @pytest.mark.parametrize(
        "region_name, aws_partition",
        [
            ("eu-west-1", "aws-iso-b"),
            ("us-west-2", "aws-iso"),
            ("cn-north-1", "aws-us-gov"),
            ("us-gov-east-1", "aws-cn"),
            ("us-iso-east-1", "aws-iso-b"),
            ("us-isob-east-1", "aws-cn"),
        ],
    )
    def test_region_name_wrong_partition(self, region_name, aws_partition):
        with pytest.raises(ValueError, match=r".*not included in partition.*"):
            resolve_aws_partition(region_name=region_name, aws_partition=aws_partition)
