from datetime import datetime, timedelta

from freezegun import freeze_time

from tenant_schemas_celery.task import TenantTask
from tenant_schemas_celery.test_utils import create_client


def test_task_get_tenant_for_schema_should_cache_results(transactional_db):
    class DummyTask(TenantTask):
        tenant_cache_seconds = 1

        def run(self, *args, **kwargs):
            pass

    task = DummyTask()
    fresh_tenant = create_client(
        name="test1", schema_name="test1", domain_url="test1.test.com"
    )

    cached_tenant = task.get_tenant_for_schema("test1")

    # Check for equality, but the objects should be different. The one from cache was fetched separately.
    assert cached_tenant == fresh_tenant
    assert cached_tenant is not fresh_tenant

    cache_hit_tenant = task.get_tenant_for_schema("test1")

    # A cache hit. The same instance should be returned.
    assert cache_hit_tenant == cached_tenant
    assert cache_hit_tenant is cached_tenant

    with freeze_time(
        datetime.utcnow() + 2 * timedelta(seconds=DummyTask.tenant_cache_seconds)
    ):
        cache_miss_tenant = task.get_tenant_for_schema("test1")

        # A cache miss. Equality is required, but they are not they same objects.
        assert cache_miss_tenant == cached_tenant
        assert cache_miss_tenant is not cached_tenant
