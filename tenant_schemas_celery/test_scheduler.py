from collections.abc import Mapping
from tempfile import NamedTemporaryFile
from typing import Any, Optional, TypedDict

from celery import schedules, uuid
from django.db import connection
from django_tenants.utils import get_tenant_model, schema_context, get_public_schema_name
from pytest import fixture, mark
from tenant_schemas_celery.app import CeleryApp

from tenant_schemas_celery.scheduler import (
    TenantAwarePersistentScheduler,
    TenantAwareScheduler,
)

Tenant = get_tenant_model()


class FakeScheduler(TenantAwareScheduler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sent: list[tuple[str, TenantAwareScheduler.Entry]] = []

    def apply_async(self, entry, producer=None, advance=True, **kwargs):
        self._sent.append((connection.schema_name, entry))
        return self.app.AsyncResult(uuid())


class ScheduledEntryConfig(TypedDict, total=False):
    task: str
    schedule: int
    args: Optional[tuple[Any, ...]]
    kwargs: Optional[Mapping[str, Any]]
    options: Optional[Mapping[str, Any]]
    tenant_schemas: Optional[list[str]]


COMMON_PARAMETERS = mark.parametrize(
    "config",
    (
        {
            "test_task": {
                "task": "test_task",
                "schedule": schedules.crontab(minute="*"),
            }
        },
        {
            "test_tenant_specific_task": {
                "task": "tenant_specific_task",
                "schedule": schedules.crontab(minute="*"),
                "tenant_schemas": ["tenant1"],
            }
        },
        {
            "test_tenant_specific_task": {
                "task": "tenant_specific_task",
                "schedule": schedules.crontab(minute="*"),
                "tenant_schemas": ["tenant1", "tenant2"],
            },
            "test_generic_task": {
                "task": "generic_task",
                "schedule": schedules.crontab(minute="*"),
            },
        },
    ),
)


@fixture
def app(config: Mapping[str, ScheduledEntryConfig]) -> CeleryApp:
    app = CeleryApp("test_app", set_as_current=False)
    app.conf.beat_schedule = config
    return app


@COMMON_PARAMETERS
class TestTenantAwareSchedulerMixin:
    @fixture
    def scheduler(self, app: CeleryApp) -> FakeScheduler:
        return FakeScheduler(app)

    def test_schedule_setup_properly(
        self,
        scheduler: FakeScheduler,
        config: Mapping[str, ScheduledEntryConfig],
    ):
        for key, config in config.items():
            config_tenant_schemas = config.get("tenant_schemas", None)
            if config_tenant_schemas is None:
                expected_entry_names = [f"{key}@__all_tenants_only__"]
                expected_schema_names = [None]
            else:
                expected_entry_names = [f"{key}@{schema_name }" for schema_name in config_tenant_schemas]
                expected_schema_names = config_tenant_schemas

            for expected_entry_name, expected_schema_name in zip(expected_entry_names, expected_schema_names):
                assert expected_entry_name in scheduler.schedule
                entry = scheduler.schedule[expected_entry_name]

                assert entry.task == config["task"]
                assert entry.schedule == schedules.crontab(minute="*")
                assert entry.options["headers"].get("_schema_name") == expected_schema_name

    @fixture
    def tenants(self) -> None:
        with schema_context(get_public_schema_name()):
            Tenant.objects.create(
                name="Tenant1", schema_name="tenant1"
            )
            Tenant.objects.create(
                name="Tenant2", schema_name="tenant2"
            )

    @mark.django_db
    def test_apply_entry(self, scheduler: FakeScheduler, tenants: None):
        for task_name, entry in scheduler.schedule.items():
            scheduler.apply_entry(entry)

            if entry.options["headers"].get("_all_tenants_only"):
                schemas = Tenant.objects.values_list("schema_name", flat=True)
            else:
                schemas = [entry.options["headers"].get("_schema_name")]

            for schema_name in schemas:
                assert (schema_name, entry) in scheduler._sent

            scheduler._sent.clear()

    @mark.django_db
    class TestCustomQuerySet:
        @fixture
        def scheduler(self, app: CeleryApp) -> FakeScheduler:
            class WithCustomQuerySet(FakeScheduler):
                @classmethod
                def get_queryset(cls):
                    return super().get_queryset().filter(ready=True)

            return WithCustomQuerySet(app)

        def test_unready_tenants_are_not_sent(self, scheduler: FakeScheduler):
            with schema_context(get_public_schema_name()):
                Tenant.objects.create(
                    name="Tenant1",
                    schema_name="tenant1",
                    ready=False
                )

            for task_name, entry in scheduler.schedule.items():
                scheduler.apply_entry(entry)

            assert scheduler._sent == []


@COMMON_PARAMETERS
class TestTenantAwarePersistentScheduler:
    """This is mostly to test that the serialization of `TenantAwareSchedulerEntry`s works

    This is because `PersistentScheduler`'s `schedule` property does a dynamic lookup on the `shelve` db,
    which forces pickling/unpickling of the entries.
    """

    @fixture
    def scheduler(self, app: CeleryApp, tmp_path: str) -> TenantAwarePersistentScheduler:
        yield TenantAwarePersistentScheduler(
            app, schedule_filename=str(tmp_path / "schedule")
        )

    def test_schedule_setup_properly(
        self,
        scheduler: TenantAwarePersistentScheduler,
        config: Mapping[str, ScheduledEntryConfig],
    ):
        for key, config in config.items():
            config_tenant_schemas = config.get("tenant_schemas", None)
            if config_tenant_schemas is None:
                expected_entry_names = [f"{key}@__all_tenants_only__"]
                expected_schema_names = [None]
            else:
                expected_entry_names = [f"{key}@{schema_name }" for schema_name in config_tenant_schemas]
                expected_schema_names = config_tenant_schemas

            for expected_entry_name, expected_schema_name in zip(expected_entry_names, expected_schema_names):
                assert expected_entry_name in scheduler.schedule
                entry = scheduler.schedule[expected_entry_name]

                assert entry.task == config["task"]
                assert entry.schedule == schedules.crontab(minute="*")
                assert entry.options["headers"].get("_schema_name") == expected_schema_name
