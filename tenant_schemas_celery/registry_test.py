import inspect

from celery import Task
from celery.task import Task as LegacyTask

from tenant_schemas_celery.task import TenantTask
from tenant_schemas_celery.app import CeleryApp

from .test_app import app
from .test_tasks import get_schema_name, get_schema_from_class_task, SchemaClassTask, SchemaClassLegacyTask


def test_get_schema_name_registration(transactional_db):
    assert not issubclass(get_schema_name, LegacyTask)
    assert issubclass(get_schema_name, Task)
    
    name = 'tenant_schemas_celery.test_tasks.get_schema_name'
    assert name in app._tasks
    
    task = app._tasks[name]
    assert not inspect.isclass(task)
    assert task.__class__.__name__ == 'get_schema_name'
    assert isinstance(task, TenantTask)


def test_get_schema_from_class_task_registration(transactional_db):
    assert not issubclass(get_schema_from_class_task, LegacyTask)
    assert issubclass(get_schema_from_class_task, Task)
    
    name = 'tenant_schemas_celery.test_tasks.get_schema_from_class_task'
    assert name in app._tasks
    
    task = app._tasks[name]
    assert not inspect.isclass(task)
    assert task.__class__.__name__ == 'get_schema_from_class_task'
    assert isinstance(task, TenantTask)
    assert isinstance(task, SchemaClassTask)


def test_schema_class_legacy_task_registration(transactional_db):
    assert issubclass(SchemaClassLegacyTask, LegacyTask)
    assert not issubclass(SchemaClassLegacyTask, Task)
    
    name = 'tenant_schemas_celery.test_tasks.SchemaClassLegacyTask'
    assert name in app._tasks
    
    task = app._tasks[name]
    assert not inspect.isclass(task)
    assert task.__class__.__name__ == 'SchemaClassLegacyTask'
    assert isinstance(task, TenantTask)
    assert isinstance(task, SchemaClassLegacyTask)
