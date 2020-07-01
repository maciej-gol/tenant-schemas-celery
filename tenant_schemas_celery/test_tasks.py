from __future__ import absolute_import

from celery import shared_task, Task
from celery.task import Task as LegacyTask
from django.db import connection

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


class SchemaClassTask(Task):
    @property
    def connection_schema_name(self):
        return connection.schema_name


@shared_task(base=SchemaClassTask, bind=True)
def get_schema_from_class_task(self):
    '''
        NOTICE: decorator tasks using a custom base like this are not supported
    '''
    return self.connection_schema_name


class SchemaClassLegacyTask(LegacyTask):
    def run(self):
        return connection.schema_name
