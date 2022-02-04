tenant-schemas-celery 
=====================

Celery application implementation that allows celery tasks to cooperate with
multi-tenancy provided by [django-tenant-schemas](https://github.com/bernardopires/django-tenant-schemas) and
[django-tenants](https://github.com/tomturner/django-tenants) packages.

This project might not seem frequently updated, but it just has all the functionality needed. Issues and questions are answered quickly.

Installation
------------

```bash
   $ pip install tenant-schemas-celery
   $ pip install django-tenants
```

Usage
-----

   * Define a celery app using given `CeleryApp` class.

```python
   import os
   os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'app.settings')

   from django.conf import settings

   from tenant_schemas_celery.app import CeleryApp as TenantAwareCeleryApp

   app = TenantAwareCeleryApp()
   app.config_from_object('django.conf:settings')
   app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
```

This assumes a fresh Celery 4.3.0 application. For previous versions, the key is to create a new `CeleryApp` instance that will be used to access task decorator from.

   * Replace your `@task` decorator with `@app.task`

```python
   from django.db import connection
   from myproject.celery import app

   @app.task
   def my_task():
      print(connection.schema_name)
```

   * Run celery worker (`myproject.celery` is where you've defined the `app` variable)

```bash
    $ celery worker -A myproject.celery
```

   * Post registered task. The schema name will get automatically added to the task's arguments.

```python
   from myproject.tasks import my_task
   my_task.delay()
```

The `TenantTask` class transparently inserts current connection's schema into
the task's kwargs. The schema name is then popped from the task's kwargs in
`task_prerun` signal handler, and the connection's schema is changed
accordingly.

### Multiple databases support

New in `2.0.0`.

Inside your celery tasks you might be working with multiple databases. You might want to change the schema for
all of the connections, or just a subset of them.

You can now use the `CELERY_TASK_TENANT_CACHE_SECONDS` django setting, or `TASK_TENANT_CACHE_SECONDS` celery setting, or
the `tenant_databases` attribute of the `TenantTask` to a list of database names (the key in the `settings.DATABASES` dictionary).

If not set, the settings defaults to `["default"]`.

### Tenant objects cache

New in `0.3.0`.

Every time a celery task is executed, the tenant object of the `connection` object is being refetched.
For some use cases, this can introduce significant performance hit.

In such scenarios, you can pass `tenant_cache_seconds` argument to the `@app.task()` decorator. This will
cause the tenant objects to be cached for given period of time. `0` turns this off. You can also enable cache globally
by setting celery's `TASK_TENANT_CACHE_SECONDS` (app-specific, usually it's `CELERY_TASK_TENANT_CACHE_SECONDS`).

```python
@app.task(tenant_cache_seconds=30)
def some_task():
    ...
```

Celery beat integration
-----------------------

This package does not provide support for scheduling periodic tasks inside given schema. Instead, you can use `{django_tenants,django_tenants_schemas}.utils.{get_tenant_model,tenant_context}` methods to launch given tasks within specific tenant.

Let's say that you would like to run a `reset_remaining_jobs` tasks periodically, for every tenant that you have. Instead of scheduling the task for each schema separately, you can schedule one dispatcher task that will iterate over all schemas and send specific task for each schema you want, instead:

```python
from django_tenants.utils import get_tenant_model, tenant_context
from django_tenant_schemas.utils import get_tenant_model, tenant_context

@app.task
def reset_remaining_jobs_in_all_schemas():
    for tenant in get_tenant_model().objects.exclude(schema_name='public'):
        with tenant_context(tenant):
            reset_remaining_jobs_in_schema.delay()

@app.task
def reset_remaining_jobs_in_schema():
    <do some logic>
```

The `reset_remaining_jobs_in_all_schemas` task (called the dispatch task) should be registered in your celery beat schedule. The `reset_remaining_jobs_in_schema` task should be called from the dispatch task.

That way you have full control over which schemas the task should be scheduled in.

Python compatibility
====================

The `2.x` series support Python>=3.7.
The `1.x` series support Python>=3.6. Python 3.6 reached EOL 2021-12.
The `0.x` series are the last one to support Python<3.6.
