import copy
import logging

from celery.beat import PersistentScheduler, ScheduleEntry, Scheduler
from django_tenants.utils import get_tenant_model, schema_context, get_public_schema_name
from django.db import models

logger = logging.getLogger(__name__)

Tenant = get_tenant_model()


class TenantAwareScheduleEntry(ScheduleEntry):
    def __init__(self, *args, **kwargs):
        if args and len(args) == 9:
            # Unpickled from database. Drop unused tenant_schemas field.
            args = args[:-1]
        else:
            # Initialized from code. Drop unused tenant_schemas field.
            kwargs.pop("tenant_schemas", None)

        super().__init__(*args, **kwargs)

    def __reduce__(self):
        """Needed for Pickle serialization"""
        return self.__class__, (
            self.name,
            self.task,
            self.last_run_at,
            self.total_run_count,
            self.schedule,
            self.args,
            self.kwargs,
            self.options,
        )


class TenantAwareSchedulerMixin:
    @classmethod
    def get_queryset(cls) -> models.QuerySet:
        return Tenant.objects.all()

    def _tenant_aware_beat_schedule_to_dict(self, beat_schedule: dict[str, object]) -> dict[str, dict[str, object]]:
        result = {}
        for name, entry in copy.deepcopy(beat_schedule).items():
            tenant_schemas = entry.pop("tenant_schemas", None)
            if tenant_schemas is None:
                schema_name = '__all_tenants_only__'
                entry.setdefault("options", {}).setdefault("headers", {})["_all_tenants_only"] = True
                result[f"{name}@{schema_name}"] = entry
            else:
                for schema_name in tenant_schemas:
                    entry.setdefault("options", {}).setdefault("headers", {})["_schema_name"] = schema_name
                    result[f"{name}@{schema_name}"] = copy.deepcopy(entry)

        return result

    def apply_entry(self, entry: ScheduleEntry, producer=None):
        """
        See https://github.com/celery/celery/blob/c571848023be732a1a11d46198cf831a522cfb54/celery/beat.py#L277
        """

        tenants = self.get_queryset()

        send_to_all_tenants = entry.options.setdefault("headers", {}).get("_all_tenants_only")
        if send_to_all_tenants:
            schemas = list(tenants.exclude(schema_name=get_public_schema_name()).values_list("schema_name", flat=True))
        else:
            schemas = list(tenants.filter(schema_name=entry.options["headers"]["_schema_name"]).values_list("schema_name", flat=True))

        logger.info(
            "TenantAwareScheduler: Sending due task %s (%s) to %s tenants",
            entry.name,
            entry.task,
            "all" if send_to_all_tenants else str(len(schemas)),
        )

        for schema in schemas:
            with schema_context(schema):
                logger.debug(
                    "Sending due task %s (%s) to tenant %s",
                    entry.name,
                    entry.task,
                    schema,
                )
                try:
                    result = self.apply_async(
                        entry, producer=producer, advance=False
                    )
                except Exception as exc:
                    logger.exception(exc)
                else:
                    logger.debug("%s sent. id->%s", entry.task, result.id)


# These classes need custom entry to provide backwards-compatibility to remove the now not-used tenant_schemas field.
class TenantAwareScheduler(TenantAwareSchedulerMixin, Scheduler):
    Entry = TenantAwareScheduleEntry

    def merge_inplace(self, b: dict[str, object]) -> None:
        return super().merge_inplace(self._tenant_aware_beat_schedule_to_dict(b))


class TenantAwarePersistentScheduler(
    TenantAwareSchedulerMixin, PersistentScheduler
):
    Entry = TenantAwareScheduleEntry

    def merge_inplace(self, b: dict[str, object]) -> None:
        return super().merge_inplace(self._tenant_aware_beat_schedule_to_dict(b))
