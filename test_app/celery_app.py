import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'test_app.settings')

from django.conf import settings

from tenant_schemas_celery.app import CeleryApp

app = CeleryApp()
app.config_from_object('django.conf:settings')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
