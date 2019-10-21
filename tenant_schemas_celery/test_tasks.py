from __future__ import absolute_import

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
