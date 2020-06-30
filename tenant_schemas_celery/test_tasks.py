from __future__ import absolute_import

from celery import shared_task
from django.db import connection
from jobtastic import JobtasticTask

from test_app.tenant.models import DummyModel
from .test_app import app


class DoesNotExist(Exception):
    pass


@app.task
def update_task(model_id, name):
    try:
        dummy = DummyModel.objects.get(pk=model_id)

    except DummyModel.DoesNotExist:
        raise DoesNotExist()

    dummy.name = name
    dummy.save()


@app.task(bind=True)
def update_retry_task(self, model_id, name):
    if update_retry_task.request.retries:
        return update_task(model_id, name)

    # Don't throw the Retry exception.
    self.retry(countdown=0.1)


@shared_task
def get_schema_name():
    return connection.schema_name


class JobtasticSchemaTask(JobtasticTask):
    significant_kwargs = []
    herd_avoidance_timeout = -1
    cache_duration = -1

    def calculate_result(self):
        return connection.schema_name
