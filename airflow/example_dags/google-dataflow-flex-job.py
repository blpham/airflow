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

from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)

DAG_ID = "google-dataflow-flex-job"

with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataflow"],
) as dag:
    start_template_job = DataflowStartFlexTemplateOperator(
        task_id="start_template_job",
        project_id="google.com:clouddfe",
        location="us-central1",
        body={
            "launchParameter": {
                "jobName": "wordcount-airflow-test",
                "containerSpecGcsPath": "gs://xianhualiu-bucket-1/templates/word-count.json",
                "parameters": {
                    "inputFile": "gs://xianhualiu-bucket-1/examples/kinglear.txt",
                    "output": "gs://xianhualiu-bucket-1/results/kinglear",
                },
            }
        },
        wait_until_finished=False,
    )

    start_template_job
