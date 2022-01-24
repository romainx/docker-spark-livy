from typing import Dict
import pytest
import requests
import logging
from collections import namedtuple
from requests import Response
from retrying import retry
import string
import random

"""
Tests Livy server.

Equivalent of simple curl commands

curl -d @app.json -H 'Content-Type: application/json' -X POST http://localhost:8998/batches
curl -X GET http://localhost:8998/batches


Ref

- <http://livy.incubator.apache.org/examples/>
- <https://gist.github.com/drmalex07/76df2860f427d17e61083520386b5531#readme---submit-to-livy-using-rest-api>
"""


@pytest.fixture(scope="session")
def livy_batches_url():
    return "http://localhost:8998/batches"


@pytest.fixture(scope="session")
def spark_pi() -> Dict:
    return {
        "file": "/opt/jars/spark-examples_2.11-2.4.7.jar",
        "className": "org.apache.spark.examples.SparkPi",
        "args": [1],
        "driverMemory": "512m",
        "executorMemory": "512m",
        "executorCores": 1,
        "numExecutors": 1,
    }


@pytest.fixture(scope="session", params=range(10))
def spark_pi_batch(livy_batches_url, spark_pi, request) -> Response:
    job = spark_pi
    job["name"] = f"job-{request.param}-{_id_generator()}"
    return requests.post(livy_batches_url, json=job)


@pytest.fixture(scope="session")
def spark_pi_batch_object(spark_pi_batch) -> namedtuple:
    return _to_batch_object(spark_pi_batch.json())


@pytest.fixture(scope="session")
def spark_pi_batch_status_code(spark_pi_batch) -> int:
    return spark_pi_batch.status_code


def _to_batch_object(data) -> namedtuple:
    return namedtuple("Batch", data.keys())(*data.values())


def _id_generator(size=5, chars=string.ascii_uppercase + string.digits):
    return "".join(random.choice(chars) for _ in range(size))


def test_livy_batches(livy_batches_url) -> None:
    r = requests.get(livy_batches_url)
    assert r.status_code == 200, "Batches end point not found"


def test_batch_created(spark_pi_batch_status_code, spark_pi_batch_object) -> None:
    # 201 -> Created
    assert spark_pi_batch_status_code == 201, "Request failed"
    assert spark_pi_batch_object.id >= 0, "Batch ID should be a positive int"
    assert spark_pi_batch_object.state in [
        "starting",
        "running",
    ], f"Batch {spark_pi_batch_object.id} should be running"


def test_batch_status(livy_batches_url, spark_pi_batch_object):
    # https://livy.incubator.apache.org/docs/latest/rest-api.html
    @retry(
        retry_on_result=lambda x: x in ["not_started", "starting", "running"],
        wait_random_min=3000,
        wait_random_max=10000,
    )
    def check_batch_status(check_url, check_batch):
        r = requests.get(f"{check_url}/{check_batch.id}")
        assert r.status_code == 200, f"Cannot get batch {check_batch.id} status"
        batch = _to_batch_object(r.json())
        logging.info(f"Batch {check_batch.id} -> {batch.state}")
        return batch.state

    assert check_batch_status(livy_batches_url, spark_pi_batch_object) == "success"
