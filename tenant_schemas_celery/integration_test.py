from __future__ import absolute_import

import pytest
import time

from django.db import connection
from django.db.models.fields import FieldDoesNotExist

from test_app.shared.models import Client
from test_app.tenant.models import DummyModel
from .compat import get_public_schema_name, schema_context, tenant_context
from .test_tasks import update_task, update_retry_task, DoesNotExist


@pytest.fixture
def setup_tenant_test(transactional_db):
    kwargs1 = {}
    kwargs2 = {}

    data = {}

    try:
        Client._meta.get_field('domain_url')
    except FieldDoesNotExist:
        pass
    else:
        kwargs1 = {'domain_url': 'test1.test.com'}
        kwargs2 = {'domain_url': 'test2.test.com'}

    tenant1 = data['tenant1'] = Client(name='test1', schema_name='test1', **kwargs1)
    tenant1.save()

    tenant2 = data['tenant2'] = Client(name='test2', schema_name='test2', **kwargs2)
    tenant2.save()

    connection.set_tenant(tenant1)
    DummyModel.objects.all().delete()
    data['dummy1'] = DummyModel.objects.create(name='test1')

    connection.set_tenant(tenant2)
    DummyModel.objects.all().delete()
    data['dummy2'] = DummyModel.objects.create(name='test2')

    connection.set_schema_to_public()

    try:
        yield data

    finally:
        connection.set_schema_to_public()


def test_should_update_model(setup_tenant_test):
    dummy1, dummy2 = setup_tenant_test['dummy1'], setup_tenant_test['dummy2']

    # We should be in public schema where dummies don't exist.
    for dummy in dummy1, dummy2:
        # Test both async and local versions.
        with pytest.raises(DoesNotExist):
            update_task.apply_async(args=(dummy.pk, 'updated-name')).get()

        with pytest.raises(DoesNotExist):
            update_task.apply(args=(dummy.pk, 'updated-name')).get()

    connection.set_tenant(setup_tenant_test['tenant1'])
    update_task.apply_async(args=(dummy1.pk, 'updated-name')).get()
    assert connection.schema_name == setup_tenant_test['tenant1'].schema_name

    # The task restores the schema from before running the task, so we are
    # using the `tenant1` tenant now.
    model_count = DummyModel.objects.filter(name='updated-name').count()
    assert model_count == 1

    connection.set_tenant(setup_tenant_test['tenant2'])
    model_count = DummyModel.objects.filter(name='updated-name').count()
    assert model_count == 0


def test_task_retry(setup_tenant_test):
    dummy1 = setup_tenant_test['dummy1']

    # Schema name should persist through retry attempts.
    connection.set_tenant(setup_tenant_test['tenant1'])
    update_retry_task.apply_async(args=(dummy1.pk, 'updated-name')).get()

    for _ in range(19):
        model_count = DummyModel.objects.filter(name='updated-name').count()
        try:
            assert model_count == 1

        except AssertionError:
            # Wait for the retried task to finish.
            time.sleep(0.1)

        else:
            break

    model_count = DummyModel.objects.filter(name='updated-name').count()
    assert model_count == 1


def test_restoring_schema_name(setup_tenant_test):
    dummy1 = setup_tenant_test['dummy1']
    dummy2 = setup_tenant_test['dummy2']

    with tenant_context(setup_tenant_test['tenant1']):
        update_task.apply_async(args=(dummy1.pk, 'updated-name')).get()

    assert connection.schema_name == get_public_schema_name()

    connection.set_tenant(setup_tenant_test['tenant1'])

    with tenant_context(setup_tenant_test['tenant2']):
        update_task.apply_async(args=(dummy2.pk, 'updated-name')).get()

    assert connection.schema_name == setup_tenant_test['tenant1'].schema_name

    connection.set_tenant(setup_tenant_test['tenant2'])

    # The model does not exist in the public schema.
    with pytest.raises(DoesNotExist):
        with schema_context(get_public_schema_name()):
            update_task.apply_async(args=(dummy2.pk, 'updated-name')).get()

    assert connection.schema_name == setup_tenant_test['tenant2'].schema_name
