from datetime import datetime, timedelta

from freezegun import freeze_time

from tenant_schemas_celery.task import TenantTask
from tenant_schemas_celery.app import CeleryApp
from tenant_schemas_celery.test_utils import create_client
from django.conf import settings

def test_task_get_tenant_for_schema_should_cache_results(transactional_db):
    # Celery is not able to pick up settings without this
    app = CeleryApp('testapp')
    app.config_from_object('django.conf:settings', namespace="CELERY")

    class DummyTask(TenantTask):
        tenant_cache_seconds = 1

        def run(self, *args, **kwargs):
            pass

    task = DummyTask()
    fresh_tenant = create_client(
        name="test1", schema_name="test1", domain_url="test1.test.com"
    )

    # TASK_TENANT_CACHE_SECONDS global setting set to 10s
    class DummyTask2(TenantTask):
        def run(self, *args, **kwargs):
            pass

    task2 = DummyTask2()
    fresh_tenant2 = create_client(
        name="test2", schema_name="test2", domain_url="test2.test.com"
    )

    cached_tenant = task.get_tenant_for_schema("test1")
    cached_tenant2 = task2.get_tenant_for_schema("test2")

    # Check for equality, but the objects should be different. The one from cache was fetched separately.
    assert cached_tenant == fresh_tenant
    assert cached_tenant is not fresh_tenant

    assert cached_tenant2 == fresh_tenant2
    assert cached_tenant2 is not fresh_tenant2

    cache_hit_tenant = task.get_tenant_for_schema("test1")
    cache_hit_tenant2 = task2.get_tenant_for_schema("test2")

    # A cache hit. The same instance should be returned.
    assert cache_hit_tenant == cached_tenant
    assert cache_hit_tenant is cached_tenant

    assert cache_hit_tenant2 == cached_tenant2
    assert cache_hit_tenant2 is cached_tenant2

    with freeze_time(
        datetime.utcnow() + 2 * timedelta(seconds=DummyTask.tenant_cache_seconds)
    ):
        cache_miss_tenant = task.get_tenant_for_schema("test1")

        # A cache miss. Equality is required, but they are not they same objects.
        assert cache_miss_tenant == cached_tenant
        assert cache_miss_tenant is not cached_tenant

        # cache hit for the second task since this uses global setting which holds
        # the entry for longer
        cache_hit_tenant2 = task2.get_tenant_for_schema("test2")
        assert cache_hit_tenant2 == cached_tenant2
        assert cache_hit_tenant2 is cached_tenant2

    with freeze_time(
            datetime.utcnow() + 2 * timedelta(seconds=int(settings.CELERY_TASK_TENANT_CACHE_SECONDS))
    ):
        cache_miss_tenant2 = task.get_tenant_for_schema("test2")

        # A cache miss. Equality is required, but they are not they same objects.
        assert cache_miss_tenant2 == cached_tenant2
        assert cache_miss_tenant2 is not cached_tenant2

