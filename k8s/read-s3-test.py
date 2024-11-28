# Copyright (c) 2024, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Import SparkSession
from pyspark.sql import SparkSession
import time

# Create SparkSession 
spark = SparkSession.builder.appName("s3-read-test").getOrCreate()
# this is a public bucket containing some sample data for testing
data_path = "s3a://tpcds-share-emr/sf10-parquet/useDecimal=true,useDate=true,filterNull=false/call_center"
column_name = "cc_call_center_id"
spark.read.parquet(data_path).select(column_name).show()
# sleep 300 seconds to allow user to check the Spark UI
time.sleep(300)
spark.stop()
