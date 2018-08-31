tenant-schemas-celery 
=====================
 [![Build Status](https://travis-ci.org/maciej-gol/tenant-schemas-celery.svg?branch=master)](https://travis-ci.org/maciej-gol/tenant-schemas-celery)

Celery application implementation that allows celery tasks to cooperate with
multi-tenancy provided by [django-tenant-schemas](https://github.com/bernardopires/django-tenant-schemas) and
[django-tenants](https://github.com/tomturner/django-tenants) packages.

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

   from tenant_schemas_celery.app import CeleryApp

   app = CeleryApp()
   app.config_from_object('django.conf:settings')
   app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
```

This assumes a fresh Celery 3.1.13 application. For previous versions, the key is to create a new `CeleryApp` instance that will be used to access task decorator from.

   * Replace your `@task` decorator with `@app.task`

```python
   from django.db import connection
   from myproject.celery import app

   @app.task
   def my_task():
      print connection.schema_name
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
