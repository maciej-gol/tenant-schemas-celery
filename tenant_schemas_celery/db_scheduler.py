import json
import logging
from typing import List
from django_celery_beat.models import PeriodicTask, PeriodicTasks
from django_celery_beat.schedulers import DatabaseScheduler

from tenant_schemas_celery.compat import get_tenant_model, schema_context, get_public_schema_name, tenant_context
from tenant_schemas_celery.scheduler import TenantAwareSchedulerMixin

logger = logging.getLogger(__name__)


class TenantAwareModelManager:
    def get_public_schema_name(self) -> List[str]:
        return [get_public_schema_name()]

    def get_tenant_schema_names(self) -> List[str]:
        return list(get_tenant_model().objects.values_list("schema_name", flat=True))

    def get_schema_names(self) -> List[str]:
        return [
            *self.get_public_schema_name(),
            *self.get_tenant_schema_names(),
        ]

    def enabled(self) -> List[PeriodicTask]:
        models = []
        for schema_name in self.get_schema_names():
            with schema_context(schema_name):
                for task in PeriodicTask.objects.enabled():
                    headers = json.loads(task.headers)
                    headers.setdefault("_schema_name", schema_name)
                    task.headers = json.dumps(headers)
                    models.append(task)

        return models

class TenantAwarePeriodicTaskWrapper:
    objects = TenantAwareModelManager()


class TenantAwarePeriodicTasks:
    @classmethod
    def last_change(cls) -> bool:
        with schema_context(get_public_schema_name()):
            all_tenants = list(get_tenant_model().objects.all())

            last_change = PeriodicTasks.last_change()

        for tenant in all_tenants:
            with tenant_context(tenant):
                tenant_last_change = PeriodicTasks.last_change()
                if last_change and tenant_last_change:
                    last_change = max(last_change, tenant_last_change)
                else:
                    last_change = tenant_last_change

        return last_change


class TenantAwareDatabaseScheduler(TenantAwareSchedulerMixin, DatabaseScheduler):
    Model = TenantAwarePeriodicTaskWrapper
    Changes = TenantAwarePeriodicTasks

    def setup_schedule(self):
        self.install_default_entries(self.schedule)
        self.update_from_dict(
            self._tenant_aware_beat_schedule_to_dict(self.app.conf.beat_schedule)
        )
