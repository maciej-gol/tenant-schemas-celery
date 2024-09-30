import pytest
from datetime import datetime, timedelta, timezone

from freezegun import freeze_time
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from tenant_schemas_celery.db_scheduler import TenantAwareDatabaseScheduler, TenantAwareModelEntry, TenantAwarePeriodicTaskWrapper, TenantAwarePeriodicTasks
from tenant_schemas_celery.test_app import app
from tenant_schemas_celery.test_utils import create_client
from tenant_schemas_celery.compat import get_public_schema_name, tenant_context

@pytest.mark.usefixtures("db")
class TestTenantAwarePeriodicTasks:
    def test_should_return_last_change_for_public_schema(self) -> None:
        expected_time = datetime(2020, 1, 1, tzinfo=timezone.utc)

        with freeze_time(expected_time):
            TenantAwarePeriodicTasks.update_changed()

        assert TenantAwarePeriodicTasks.last_change() == expected_time

    def test_should_return_last_change_for_tenants_and_public(self) -> None:
        expected_time = datetime(2020, 1, 1, tzinfo=timezone.utc)

        expected_tenant = create_client("expected-tenant", "expected-tenant", "some-domain")
        with freeze_time(expected_time), tenant_context(expected_tenant):
            TenantAwarePeriodicTasks.update_changed()

        other_tenant = create_client("other-tenant", "other-tenant", "some-domain")
        with freeze_time(expected_time - timedelta(seconds=1)), tenant_context(other_tenant):
            TenantAwarePeriodicTasks.update_changed()

        with freeze_time(expected_time - timedelta(seconds=2)):
            TenantAwarePeriodicTasks.update_changed()

        assert TenantAwarePeriodicTasks.last_change() == expected_time

    def test_should_return_last_change_for_only_tenants(self) -> None:
        expected_time = datetime(2020, 1, 1, tzinfo=timezone.utc)

        expected_tenant = create_client("expected-tenant", "expected-tenant", "some-domain")
        with freeze_time(expected_time), tenant_context(expected_tenant):
            TenantAwarePeriodicTasks.update_changed()

        other_tenant = create_client("other-tenant", "other-tenant", "some-domain")
        with freeze_time(expected_time - timedelta(seconds=1)), tenant_context(other_tenant):
            TenantAwarePeriodicTasks.update_changed()

        assert TenantAwarePeriodicTasks.last_change() == expected_time

@pytest.mark.usefixtures("db")
class TestTenantAwarePeriodicTaskWrapper:
    def __make_periodic_task(self, name: str, enabled: bool = True) -> PeriodicTask:
        return PeriodicTask.objects.create(
            name=name,
            task="tenant_schemas_celery.test_tasks.update_task",
            interval=IntervalSchedule.objects.create(every=1, period="days"),
            enabled=enabled,
        )

    def test_manager_should_return_all_enabled_tasks_for_public_schema(self) -> None:
        task = self.__make_periodic_task("some-name")
        self.__make_periodic_task("some-other-name", enabled=False)

        actual = TenantAwarePeriodicTaskWrapper.objects.enabled()

        assert len(actual) == 1
        assert actual[0].wrapped_model == task

    def test_manager_should_return_all_enabled_tasks_for_tenants(self) -> None:
        tenant_one = create_client("schema-one", "schema-one", "schema-one")
        with tenant_context(tenant_one):
            task_schema_one = self.__make_periodic_task("schema-one")
            self.__make_periodic_task("schema-one-other", enabled=False)

        tenant_two = create_client("schema-two", "schema-two", "schema-two")
        with tenant_context(tenant_two):
            task_schema_two = self.__make_periodic_task("schema-two")
            self.__make_periodic_task("schema-two-other", enabled=False)

        actual = TenantAwarePeriodicTaskWrapper.objects.enabled()
        actual.sort(key=lambda w: w.name)

        assert len(actual) == 2
        assert actual[0].wrapped_model == task_schema_one
        assert actual[1].wrapped_model == task_schema_two

    def test_manager_should_return_all_enabled_tasks_for_both_public_and_tenants(self) -> None:
        task_public = self.__make_periodic_task("schema-public")

        tenant_one = create_client("schema-one", "schema-one", "schema-one")
        with tenant_context(tenant_one):
            task_schema_one = self.__make_periodic_task("schema-one")

        tenant_two = create_client("schema-two", "schema-two", "schema-two")
        with tenant_context(tenant_two):
            task_schema_two = self.__make_periodic_task("schema-two")

        actual = TenantAwarePeriodicTaskWrapper.objects.enabled()
        actual.sort(key=lambda w: w.name)

        assert len(actual) == 3
        assert actual[0].wrapped_model == task_schema_one
        assert actual[1].wrapped_model == task_public
        assert actual[2].wrapped_model == task_schema_two

    def test_manager_should_return_all_enabled_duplicated_tasks_in_tenant_and_public(self) -> None:
        task = self.__make_periodic_task("some-name")
        tenant_one = create_client("schema-one", "schema-one", "schema-one")
        with tenant_context(tenant_one):
            tenant_task = self.__make_periodic_task("some-name")

        actual = TenantAwarePeriodicTaskWrapper.objects.enabled()
        actual.sort(key=lambda w: w.name)

        assert len(actual) == 2
        assert actual[0].wrapped_model == task
        assert actual[1].wrapped_model == tenant_task

    def test_manager_should_assign_correct_names_to_tasks(self) -> None:
        self.__make_periodic_task("task-schema-public")

        tenant_one = create_client("schema-one", "schema-one", "schema-one")
        with tenant_context(tenant_one):
            self.__make_periodic_task("task-schema-one")

        actual = TenantAwarePeriodicTaskWrapper.objects.enabled()
        actual.sort(key=lambda w: w.name)

        assert len(actual) == 2
        assert actual[0].name == f"task-schema-one@schema-one"
        assert actual[1].name == f"task-schema-public@{get_public_schema_name()}"

@pytest.mark.usefixtures("db")
class TestTenantAwareModelEntry:
    def __make_periodic_task(self, name: str) -> PeriodicTask:
        return PeriodicTask.objects.create(
            name=name,
            task="tenant_schemas_celery.test_tasks.update_task",
            interval=IntervalSchedule.objects.create(every=1, period="days"),
            enabled=True,
        )

    def test_should_add_schema_name_to_headers_for_public_task(self) -> None:
        self.__make_periodic_task("some-task")
        wrapped_model, = TenantAwarePeriodicTaskWrapper.objects.enabled()

        entry = TenantAwareModelEntry(wrapped_model)

        assert entry.options["headers"]["_schema_name"] == get_public_schema_name()

    def test_should_keep_original_name_for_public_task(self) -> None:
        self.__make_periodic_task("some-task")
        wrapped_model, = TenantAwarePeriodicTaskWrapper.objects.enabled()

        entry = TenantAwareModelEntry(wrapped_model)

        assert entry.name == "some-task"

    def test_should_add_schema_name_to_headers_for_tenants(self) -> None:
        some_tenant = create_client("some-tenant", "some-tenant", "some-tenant")
        with tenant_context(some_tenant):
            self.__make_periodic_task("some-task")
        wrapped_model, = TenantAwarePeriodicTaskWrapper.objects.enabled()

        entry = TenantAwareModelEntry(wrapped_model)

        assert entry.options["headers"]["_schema_name"] == some_tenant.schema_name

    def test_should_keep_original_name_for_tenants(self) -> None:
        some_tenant = create_client("some-tenant", "some-tenant", "some-tenant")
        with tenant_context(some_tenant):
            self.__make_periodic_task("some-task")
        wrapped_model, = TenantAwarePeriodicTaskWrapper.objects.enabled()

        entry = TenantAwareModelEntry(wrapped_model)

        assert entry.name == "some-task"

# transactional_db is needed by the DatabaseScheduler so it doesn't close the connection during schedule change.
@pytest.mark.usefixtures("transactional_db")
class TestTenantAwareDatabaseScheduler:
    def __make_periodic_task(self, name: str) -> PeriodicTask:
        return PeriodicTask.objects.create(
            name=name,
            task="tenant_schemas_celery.test_tasks.update_task",
            interval=IntervalSchedule.objects.create(every=1, period="days"),
            enabled=True,
        )

    def test_schedule_should_read_tasks_from_public_schema(self) -> None:
        task = self.__make_periodic_task("some-task")

        scheduler = TenantAwareDatabaseScheduler(app=app)
        schedule = scheduler.schedule

        assert schedule[f"{task.name}@{get_public_schema_name()}"].model == task

    def test_schedule_should_read_tasks_from_tenants(self) -> None:
        some_tenant = create_client("some-tenant", "some-tenant", "some-tenant")
        with tenant_context(some_tenant):
            task = self.__make_periodic_task("some-task")

        scheduler = TenantAwareDatabaseScheduler(app=app)
        schedule = scheduler.schedule

        assert schedule[f"{task.name}@some-tenant"].model == task

    def test_schedule_should_read_tasks_from_tenants_and_public_schema(self) -> None:
        public_task = self.__make_periodic_task("some-task")
        some_tenant = create_client("some-tenant", "some-tenant", "some-tenant")
        with tenant_context(some_tenant):
            tenant_task = self.__make_periodic_task("some-tenant-task")

        scheduler = TenantAwareDatabaseScheduler(app=app)
        schedule = scheduler.schedule

        assert schedule[f"{public_task.name}@{get_public_schema_name()}"].model == public_task
        assert schedule[f"{tenant_task.name}@some-tenant"].model == tenant_task

    def test_schedule_should_read_changed_tasks(self) -> None:
        public_task = self.__make_periodic_task("some-task")

        scheduler = TenantAwareDatabaseScheduler(app=app)
        schedule = scheduler.schedule
        assert schedule[f"{public_task.name}@{get_public_schema_name()}"].model == public_task

        some_tenant = create_client("some-tenant", "some-tenant", "some-tenant")
        with tenant_context(some_tenant):
            tenant_task = self.__make_periodic_task("some-tenant-task")

        schedule = scheduler.schedule

        assert schedule[f"{public_task.name}@{get_public_schema_name()}"].model == public_task
        assert schedule[f"{tenant_task.name}@some-tenant"].model == tenant_task
