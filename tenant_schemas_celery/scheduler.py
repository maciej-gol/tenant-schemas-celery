import json
import logging
from typing import List, Optional

from celery.beat import PersistentScheduler, ScheduleEntry, Scheduler
from django_tenants.utils import get_tenant_model, tenant_context, get_public_schema_name
from django_celery_beat.schedulers import DatabaseScheduler, debug
from django.db import models
from django.conf import settings

logger = logging.getLogger(__name__)

Tenant = get_tenant_model()


class TenantAwareScheduleEntry(ScheduleEntry):
    tenant_schemas: Optional[List[str]] = None

    def __init__(self, *args, **kwargs):
        if args:
            # Unpickled from database
            self.tenant_schemas = args[-1]
        else:
            # Initialized from code
            self.tenant_schemas = kwargs.pop("tenant_schemas", None)

        super().__init__(*args, **kwargs)

    def update(self, other):
        """Update values from another entry.

        Will only update `tenant_schemas` and "editable" fields:
            ``task``, ``schedule``, ``args``, ``kwargs``, ``options``.
        """
        vars(self).update(
            {
                "task": other.task,
                "schedule": other.schedule,
                "args": other.args,
                "kwargs": other.kwargs,
                "options": other.options,
                "tenant_schemas": other.tenant_schemas,
            }
        )

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
            self.tenant_schemas,
        )

    def editable_fields_equal(self, other):
        for attr in (
            "task",
            "args",
            "kwargs",
            "options",
            "schedule",
            "tenant_schemas",
        ):
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True


class TenantAwareSchedulerMixin:
    Entry = TenantAwareScheduleEntry

    @classmethod
    def get_queryset(cls) -> models.QuerySet:
        return Tenant.objects.all()

    def apply_entry(self, entry: TenantAwareScheduleEntry, producer=None):
        """
        See https://github.com/celery/celery/blob/c571848023be732a1a11d46198cf831a522cfb54/celery/beat.py#L277
        """

        tenants = self.get_queryset()

        if entry.tenant_schemas is None:
            tenants = tenants.exclude(schema_name=get_public_schema_name())
        else:
            tenants = tenants.filter(schema_name__in=entry.tenant_schemas)

        logger.info(
            "TenantAwareScheduler: Sending due task %s (%s) to %s tenants",
            entry.name,
            entry.task,
            "all" if entry.tenant_schemas is None else str(len(tenants)),
        )

        for tenant in tenants:
            with tenant_context(tenant):
                logger.debug(
                    "Sending due task %s (%s) to tenant %s",
                    entry.name,
                    entry.task,
                    tenant.name,
                )
                try:
                    result = self.apply_async(
                        entry, producer=producer, advance=False
                    )
                except Exception as exc:
                    logger.exception(exc)
                else:
                    logger.debug("%s sent. id->%s", entry.task, result.id)


class TenantAwareScheduler(TenantAwareSchedulerMixin, Scheduler):
    pass


class TenantAwarePersistentScheduler(
    TenantAwareSchedulerMixin, PersistentScheduler
):
    pass


class TenantAwareDatabaseScheduler(DatabaseScheduler):
    def all_as_schedule(self):
        debug("DatabaseScheduler: Fetching database schedule")
        s = {}
        filter_kwargs = settings.TENANT_DEFAULT_FILTERS or {}

        for tenant in Tenant.objects.filter(**filter_kwargs):
            with tenant_context(tenant):
                for periodic_task_obj in self.Model.objects.enabled():
                    # update headers
                    headers = json.loads(periodic_task_obj.headers)
                    headers.update({"_schema_name": tenant.schema_name})
                    periodic_task_obj.headers = json.dumps(headers)
                    try:
                        task_name = (
                            f"{periodic_task_obj.name}-{tenant.schema_name}"
                            if periodic_task_obj.name in s
                            else periodic_task_obj.name
                        )
                        s[task_name] = self.Entry(periodic_task_obj, app=self.app)
                    except ValueError:
                        pass
        return s
