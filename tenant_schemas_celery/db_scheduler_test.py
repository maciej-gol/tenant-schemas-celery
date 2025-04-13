import json
import pytest
from tenant_schemas_celery.db_scheduler import TenantAwareDatabaseScheduler
from django_celery_beat.models import PeriodicTask, IntervalSchedule
from tenant_schemas_celery.test_app import app
from tenant_schemas_celery.test_utils import ClientFactory
from tenant_schemas_celery.compat import tenant_context


@pytest.mark.usefixtures("transactional_db")
def test_schedule_should_read_entries_from_public_schema() -> None:
    scheduler = TenantAwareDatabaseScheduler(app=app)
    PeriodicTask.objects.create(
        name="test_task_name@public",
        task="test_task",
        interval=IntervalSchedule.objects.get_or_create(every=1, period="seconds")[0],
    )

    schedule = scheduler.schedule

    assert "test_task_name@public" in schedule


@pytest.mark.usefixtures("transactional_db")
def test_schedule_should_read_entries_from_tenant_schema(client_factory: ClientFactory) -> None:
    scheduler = TenantAwareDatabaseScheduler(app=app)
    tenant = client_factory.create_client(
        name="test_tenant", schema_name="test_tenant", domain_url="test_tenant.test.com"
    )

    with tenant_context(tenant):
        PeriodicTask.objects.create(
            name="test_tenant_task_name@test_tenant",
            task="test_tenant_task",
            interval=IntervalSchedule.objects.get_or_create(every=1, period="seconds")[0],
        )

    schedule = scheduler.schedule

    assert "test_tenant_task_name@test_tenant" in schedule


@pytest.mark.usefixtures("transactional_db")
def test_schedule_should_read_mixed_entries__public_and_tenant(client_factory: ClientFactory) -> None:
    scheduler = TenantAwareDatabaseScheduler(app=app)
    tenant = client_factory.create_client(
        name="test_tenant", schema_name="test_tenant", domain_url="test_tenant.test.com"
    )

    PeriodicTask.objects.create(
        name="test_task_name@public",
        task="test_task",
        interval=IntervalSchedule.objects.get_or_create(every=1, period="seconds")[0],
    )
    with tenant_context(tenant):
        PeriodicTask.objects.create(
            name="test_task_name@test_tenant",
            task="test_task",
            interval=IntervalSchedule.objects.get_or_create(every=1, period="seconds")[0],
        )

    schedule = scheduler.schedule

    assert "test_task_name@test_tenant" in schedule
    assert "test_task_name@public" in schedule


@pytest.mark.usefixtures("transactional_db")
def test_schedule_should_read_mixed_entries__two_tenants(client_factory: ClientFactory) -> None:
    scheduler = TenantAwareDatabaseScheduler(app=app)
    tenant_one = client_factory.create_client(
        name="test_tenant_one", schema_name="test_tenant_one", domain_url="test_tenant_one.test.com"
    )
    with tenant_context(tenant_one):
        PeriodicTask.objects.create(
            name="test_task_name@test_tenant_one",
            task="test_task",
            interval=IntervalSchedule.objects.get_or_create(every=1, period="seconds")[0],
        )

    tenant_two = client_factory.create_client(
        name="test_tenant_two", schema_name="test_tenant_two", domain_url="test_tenant_two.test.com"
    )
    with tenant_context(tenant_two):
        PeriodicTask.objects.create(
            name="test_task_name@test_tenant_two",
            task="test_task",
            interval=IntervalSchedule.objects.get_or_create(every=1, period="seconds")[0],
        )

    schedule = scheduler.schedule

    assert "test_task_name@test_tenant_one" in schedule
    assert "test_task_name@test_tenant_two" in schedule


@pytest.mark.usefixtures("transactional_db")
def test_schedule_should_reject_tasks_with_duplicate_names_in_two_schemas(client_factory: ClientFactory) -> None:
    scheduler = TenantAwareDatabaseScheduler(app=app)
    tenant_one = client_factory.create_client(
        name="test_tenant_one", schema_name="test_tenant_one", domain_url="test_tenant_one.test.com"
    )
    with tenant_context(tenant_one):
        PeriodicTask.objects.create(
            name="test_task_name@duplicate",
            task="test_task",
            interval=IntervalSchedule.objects.get_or_create(every=1, period="seconds")[0],
        )

    tenant_two = client_factory.create_client(
        name="test_tenant_two", schema_name="test_tenant_two", domain_url="test_tenant_two.test.com"
    )
    with tenant_context(tenant_two):
        PeriodicTask.objects.create(
            name="test_task_name@duplicate",
            task="test_task",
            interval=IntervalSchedule.objects.get_or_create(every=1, period="seconds")[0],
        )

    with pytest.raises(ValueError, match=r"duplicate periodic task name: 'test_task_name@duplicate'. Previously seen in schema: 'test_tenant_one'."):
        scheduler.schedule


@pytest.mark.usefixtures("transactional_db")
def test_schedule_should_read_updates_from_db__public() -> None:
    scheduler = TenantAwareDatabaseScheduler(app=app)
    task = PeriodicTask.objects.create(
        name="test_task_name@public",
        task="test_task",
        interval=IntervalSchedule.objects.get_or_create(every=1, period="seconds")[0],
    )

    schedule = scheduler.schedule

    task.headers = json.dumps({"foo": "bar"})
    task.save()

    schedule = scheduler.schedule

    assert schedule["test_task_name@public"].options["headers"]["foo"] == "bar"


@pytest.mark.usefixtures("transactional_db")
def test_schedule_should_read_updates_from_db__tenant(client_factory: ClientFactory) -> None:
    scheduler = TenantAwareDatabaseScheduler(app=app)
    tenant = client_factory.create_client(
        name="test_tenant", schema_name="test_tenant", domain_url="test_tenant.test.com"
    )
    with tenant_context(tenant):
        task = PeriodicTask.objects.create(
            name="test_task_name@test_tenant",
            task="test_task",
            interval=IntervalSchedule.objects.get_or_create(every=1, period="seconds")[0],
        )

    schedule = scheduler.schedule

    with tenant_context(tenant):
        task.headers = json.dumps({"foo": "bar"})
        task.save()

    schedule = scheduler.schedule

    assert schedule["test_task_name@test_tenant"].options["headers"]["foo"] == "bar"


@pytest.mark.usefixtures("transactional_db")
def test_schedule_should_update_tasks_in_proper_schema(client_factory: ClientFactory) -> None:
    scheduler = TenantAwareDatabaseScheduler(app=app)
    task = PeriodicTask.objects.create(
        name="test_task_name@public",
        task="test_task",
        interval=IntervalSchedule.objects.get_or_create(every=1, period="seconds")[0],
    )
    tenant_one = client_factory.create_client(
        name="test_tenant_one", schema_name="test_tenant_one", domain_url="test_tenant_one.test.com"
    )
    with tenant_context(tenant_one):
        tenant_task = PeriodicTask.objects.create(
            name="test_task_name@test_tenant_one",
            task="test_task",
            interval=IntervalSchedule.objects.get_or_create(every=1, period="seconds")[0],
        )

    schedule = scheduler.schedule
    scheduler.reserve(schedule[task.name])
    scheduler.reserve(schedule[tenant_task.name])
    scheduler.sync()

    task.refresh_from_db()
    assert task.total_run_count == 1

    with tenant_context(tenant_one):
        tenant_task.refresh_from_db()
    assert tenant_task.total_run_count == 1
