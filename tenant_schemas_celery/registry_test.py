import inspect

from tenant_schemas_celery.task import TenantTask
from tenant_schemas_celery.app import CeleryApp

from .test_app import app
from .test_tasks import get_schema_name, get_schema_from_class_task, SchemaClassTask


def test_get_schema_name_registration(transactional_db):
    name = 'tenant_schemas_celery.test_tasks.get_schema_name'
    assert name in app._tasks
    
    task = app._tasks[name]
    assert not inspect.isclass(task)
    assert task.__class__.__name__ == 'get_schema_name'
    assert isinstance(task, TenantTask)


def test_get_schema_from_class_task_registration(transactional_db):
    name = 'tenant_schemas_celery.test_tasks.get_schema_from_class_task'
    assert name in app._tasks
    
    task = app._tasks[name]
    assert not inspect.isclass(task)
    assert task.__class__.__name__ == 'get_schema_from_class_task'
    assert isinstance(task, TenantTask)
    assert isinstance(task, SchemaClassTask)
