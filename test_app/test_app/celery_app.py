from tenant_schemas_celery.app import CeleryApp

app = CeleryApp('testapp')
app.config_from_object("django.conf:settings")
app.autodiscover_tasks()
