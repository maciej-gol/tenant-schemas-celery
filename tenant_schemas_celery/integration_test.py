from __future__ import absolute_import

import time

import pytest
from django.db import connection, connections
from test_app.tenant.models import DummyModel

from tenant_schemas_celery.test_utils import create_client
from .compat import get_public_schema_name, schema_context, tenant_context
from .test_tasks import (
    update_task,
    update_retry_task,
    DoesNotExist,
    get_schema_name,
    get_schema_from_class_task,
    multiple_db_task,
)


@pytest.fixture
def setup_tenant_test(transactional_db):
    data = {}

    tenant1 = data["tenant1"] = create_client(
        name="test1", schema_name="test1", domain_url="test1.test.com"
    )
    tenant2 = data["tenant2"] = create_client(
        name="test2", schema_name="test2", domain_url="test2.test.com"
    )

    connection.set_tenant(tenant1)
    DummyModel.objects.all().delete()
    data["dummy1"] = DummyModel.objects.create(name="test1")

    connection.set_tenant(tenant2)
    DummyModel.objects.all().delete()
    data["dummy2"] = DummyModel.objects.create(name="test2")

    connection.set_schema_to_public()

    try:
        yield data

    finally:
        connection.set_schema_to_public()


def test_should_update_model(setup_tenant_test):
    dummy1, dummy2 = setup_tenant_test["dummy1"], setup_tenant_test["dummy2"]

    # We should be in public schema where dummies don't exist.
    for dummy in dummy1, dummy2:
        # Test both async and local versions.
        with pytest.raises(DoesNotExist):
            update_task.apply_async(args=(dummy.pk, "updated-name")).get()

        with pytest.raises(DoesNotExist):
            update_task.apply(args=(dummy.pk, "updated-name")).get()

    connection.set_tenant(setup_tenant_test["tenant1"])
    update_task.apply_async(args=(dummy1.pk, "updated-name")).get()
    assert connection.schema_name == setup_tenant_test["tenant1"].schema_name

    # The task restores the schema from before running the task, so we are
    # using the `tenant1` tenant now.
    model_count = DummyModel.objects.filter(name="updated-name").count()
    assert model_count == 1

    connection.set_tenant(setup_tenant_test["tenant2"])
    model_count = DummyModel.objects.filter(name="updated-name").count()
    assert model_count == 0


def test_task_retry(setup_tenant_test):
    dummy1 = setup_tenant_test["dummy1"]

    # Schema name should persist through retry attempts.
    connection.set_tenant(setup_tenant_test["tenant1"])
    update_retry_task.apply_async(args=(dummy1.pk, "updated-name")).get()

    for _ in range(19):
        model_count = DummyModel.objects.filter(name="updated-name").count()
        try:
            assert model_count == 1

        except AssertionError:
            # Wait for the retried task to finish.
            time.sleep(0.1)

        else:
            break

    model_count = DummyModel.objects.filter(name="updated-name").count()
    assert model_count == 1


def test_restoring_schema_name(setup_tenant_test):
    dummy1 = setup_tenant_test["dummy1"]
    dummy2 = setup_tenant_test["dummy2"]

    with tenant_context(setup_tenant_test["tenant1"]):
        update_task.apply_async(args=(dummy1.pk, "updated-name")).get()

    assert connection.schema_name == get_public_schema_name()

    connection.set_tenant(setup_tenant_test["tenant1"])

    with tenant_context(setup_tenant_test["tenant2"]):
        update_task.apply_async(args=(dummy2.pk, "updated-name")).get()

    assert connection.schema_name == setup_tenant_test["tenant1"].schema_name

    connection.set_tenant(setup_tenant_test["tenant2"])

    # The model does not exist in the public schema.
    with pytest.raises(DoesNotExist):
        with schema_context(get_public_schema_name()):
            update_task.apply_async(args=(dummy2.pk, "updated-name")).get()

    assert connection.schema_name == setup_tenant_test["tenant2"].schema_name


def test_shared_task_get_schema_name(setup_tenant_test):
    result = get_schema_name.delay().get(timeout=1)
    assert result == get_public_schema_name()

    with tenant_context(setup_tenant_test["tenant1"]):
        result = get_schema_name.delay().get(timeout=1)

    assert result == setup_tenant_test["tenant1"].schema_name

    with tenant_context(setup_tenant_test["tenant2"]):
        result = get_schema_name.delay().get(timeout=1)

    assert result == setup_tenant_test["tenant2"].schema_name


@pytest.mark.xfail(reason="decorator tasks with custom base class not implemented")
def test_custom_task_class_get_schema_name(setup_tenant_test):
    result = get_schema_from_class_task.delay().get(timeout=1)
    assert result == get_public_schema_name()

    with tenant_context(setup_tenant_test["tenant1"]):
        result = get_schema_from_class_task.delay().get(timeout=1)

    assert result == setup_tenant_test["tenant1"].schema_name

    with tenant_context(setup_tenant_test["tenant2"]):
        result = get_schema_from_class_task.delay().get(timeout=1)

    assert result == setup_tenant_test["tenant2"].schema_name


def test_use_multiple_databases(setup_tenant_test):
    """
    Test the case where a setting to have multiple database search path to be changed
    was put for a task
    """
    with tenant_context(setup_tenant_test["tenant1"]):
        result = multiple_db_task.delay().get(timeout=1)

        # Ensure restore_schema switched back to public
        assert connections["otherdb1"].schema_name == "public"
        assert connections["otherdb2"].schema_name == "public"

    # The task should have had the two configured DBs with tenant search path
    # the other db should be untouched
    assert result == {
        "default": "public",  # should be unchanged
        "otherdb1": setup_tenant_test["tenant1"].schema_name,
        "otherdb2": setup_tenant_test["tenant1"].schema_name,
    }
