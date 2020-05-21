from datetime import datetime, timedelta

from freezegun import freeze_time

from tenant_schemas_celery.task import TenantTask
from tenant_schemas_celery.app import CeleryApp
from tenant_schemas_celery.test_utils import create_client
from django.conf import settings

def test_task_get_tenant_for_schema_should_cache_results_local_setting(transactional_db):
    class DummyTask(TenantTask):
        tenant_cache_seconds = 1

        def run(self, *args, **kwargs):
            pass

    task = DummyTask()
    fresh_tenant = create_client(
        name="test_local", schema_name="test_local", domain_url="test_local.test.com"
    )

    cached_tenant = task.get_tenant_for_schema("test_local")

    # Check for equality, but the objects should be different. The one from cache was fetched separately.
    assert cached_tenant == fresh_tenant
    assert cached_tenant is not fresh_tenant

    cache_hit_tenant = task.get_tenant_for_schema("test_local")

    # A cache hit. The same instance should be returned.
    assert cache_hit_tenant == cached_tenant
    assert cache_hit_tenant is cached_tenant

    with freeze_time(
        datetime.utcnow() + 2 * timedelta(seconds=DummyTask.tenant_cache_seconds)
    ):
        cache_miss_tenant = task.get_tenant_for_schema("test_local")

        # A cache miss. Equality is required, but they are not they same objects.
        assert cache_miss_tenant == cached_tenant
        assert cache_miss_tenant is not cached_tenant


def test_task_get_tenant_for_schema_should_cache_results_global_setting(transactional_db):
    # Celery is not able to pick up settings without this
    app = CeleryApp('testapp')
    app.config_from_object('django.conf:settings', namespace="CELERY")

    # TASK_TENANT_CACHE_SECONDS global setting set to 10s
    class DummyTask(TenantTask):
        def run(self, *args, **kwargs):
            pass

    task = DummyTask()
    fresh_tenant = create_client(
        name="test_global", schema_name="test_global", domain_url="test_global.test.com"
    )

    cached_tenant = task.get_tenant_for_schema("test_global")

    # Check for equality, but the objects should be different. The one from cache was fetched separately.
    assert cached_tenant == fresh_tenant
    assert cached_tenant is not fresh_tenant

    cache_hit_tenant = task.get_tenant_for_schema("test_global")

    # A cache hit. The same instance should be returned.
    assert cache_hit_tenant == cached_tenant
    assert cache_hit_tenant is cached_tenant

    with freeze_time(
            datetime.utcnow() + 2 * timedelta(seconds=int(settings.CELERY_TASK_TENANT_CACHE_SECONDS))
    ):
        cache_miss_tenant = task.get_tenant_for_schema("test_global")

        # A cache miss. Equality is required, but they are not they same objects.
        assert cache_miss_tenant == cached_tenant
        assert cache_miss_tenant is not cached_tenant

def test_multiple_tasks_get_tenant_for_schema_should_cache_results_global_setting(transactional_db):
    # Celery is not able to pick up settings without this
    app = CeleryApp('testapp')
    app.config_from_object('django.conf:settings', namespace="CELERY")

    # TASK_TENANT_CACHE_SECONDS global setting set to 10s
    class DummyTask(TenantTask):
        def run(self, *args, **kwargs):
            pass

    class DummyTask2(TenantTask):
        def run(self, *args, **kwargs):
            pass

    task = DummyTask()
    task2 = DummyTask2()
    fresh_tenant = create_client(
        name="test_multiple", schema_name="test_multiple", domain_url="test_multiple.test.com"
    )

    cached_tenant = task.get_tenant_for_schema("test_multiple")

    # Check for equality, but the objects should be different. The one from cache was fetched separately.
    assert cached_tenant == fresh_tenant
    assert cached_tenant is not fresh_tenant

    cache_hit_tenant = task.get_tenant_for_schema("test_multiple")
    cache_hit_tenant2 = task2.get_tenant_for_schema("test_multiple")

    # A cache hit. The same instance should be returned.
    assert cache_hit_tenant == cached_tenant
    assert cache_hit_tenant is cached_tenant

    # A cache hit for same tenant, different task
    assert cache_hit_tenant2 == cached_tenant
    assert cache_hit_tenant2 is cached_tenant

    with freeze_time(
            datetime.utcnow() + 2 * timedelta(seconds=int(settings.CELERY_TASK_TENANT_CACHE_SECONDS))
    ):
        cache_miss_tenant = task.get_tenant_for_schema("test_multiple")
        cache_miss_tenant2 = task2.get_tenant_for_schema("test_multiple")

        # A cache miss. Equality is required, but they are not they same objects.
        assert cache_miss_tenant == cached_tenant
        assert cache_miss_tenant is not cached_tenant

        # Cache miss for same tenant, different task
        assert cache_miss_tenant2 == cached_tenant
        assert cache_miss_tenant2 is not cached_tenant
