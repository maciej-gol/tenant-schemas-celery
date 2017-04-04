from unittest import skipIf

from django.db import connection
from django.db.models.fields import FieldDoesNotExist
from tenant_schemas.utils import schema_context, tenant_context

from test_app.shared.models import Client
from test_app.tenant.models import DummyModel
from .compat import get_public_schema_name, TenantTestCase

try:
    from .app import CeleryApp
except ImportError:
    app = None
else:
    app = CeleryApp('testapp')

    class CeleryConfig:
        CELERY_ALWAYS_EAGER = True
        CELERY_EAGER_PROPAGATES_EXCEPTIONS = True

    app.config_from_object(CeleryConfig)

    @app.task
    def update_task(model_id, name):
        dummy = DummyModel.objects.get(pk=model_id)
        dummy.name = name
        dummy.save()

    @app.task
    def update_retry_task(model_id, name):
        if update_retry_task.request.retries:
            return update_task(model_id, name)

        # Don't throw the Retry exception.
        update_retry_task.retry(throw=False)


@skipIf(app is None, 'Celery is not available.')
class CeleryTasksTests(TenantTestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        kwargs1 = {}
        kwargs2 = {}

        try:
            Client._meta.get_field('domain_url')
        except FieldDoesNotExist:
            pass
        else:
            kwargs1 = {'domain_url': 'test1.test.com'}
            kwargs2 = {'domain_url': 'test2.test.com'}

        self.tenant1 = Client(name='test1', schema_name='test1', **kwargs1)
        self.tenant1.save()

        self.tenant2 = Client(name='test2', schema_name='test2', **kwargs2)
        self.tenant2.save()

        connection.set_tenant(self.tenant1)
        self.dummy1 = DummyModel.objects.create(name='test1')

        connection.set_tenant(self.tenant2)
        self.dummy2 = DummyModel.objects.create(name='test2')

        connection.set_schema_to_public()

    def tearDown(self):
        connection.set_schema_to_public()

    def test_basic_model_update(self):
        # We should be in public schema where dummies don't exist.
        for dummy in self.dummy1, self.dummy2:
            # Test both async and local versions.
            with self.assertRaises(DummyModel.DoesNotExist):
                update_task.apply_async(args=(dummy.pk, 'updated-name'))

            with self.assertRaises(DummyModel.DoesNotExist):
                update_task.apply(args=(dummy.pk, 'updated-name'))

        connection.set_tenant(self.tenant1)
        update_task.apply_async(args=(self.dummy1.pk, 'updated-name'))
        self.assertEqual(connection.schema_name, self.tenant1.schema_name)

        # The task restores the schema from before running the task, so we are
        # using the `tenant1` tenant now.
        model_count = DummyModel.objects.filter(name='updated-name').count()
        self.assertEqual(model_count, 1)

        connection.set_tenant(self.tenant2)
        model_count = DummyModel.objects.filter(name='updated-name').count()
        self.assertEqual(model_count, 0)

    def test_task_retry(self):
        # Schema name should persist through retry attempts.
        connection.set_tenant(self.tenant1)
        update_retry_task.apply_async(args=(self.dummy1.pk, 'updated-name'))

        model_count = DummyModel.objects.filter(name='updated-name').count()
        self.assertEqual(model_count, 1)

    def test_restoring_schema_name(self):
        with tenant_context(self.tenant1):
            update_task.apply_async(args=(self.dummy1.pk, 'updated-name'))
        self.assertEqual(connection.schema_name, get_public_schema_name())

        connection.set_tenant(self.tenant1)

        with tenant_context(self.tenant2):
            update_task.apply_async(args=(self.dummy2.pk, 'updated-name'))
        self.assertEqual(connection.schema_name, self.tenant1.schema_name)

        connection.set_tenant(self.tenant2)
        # The model does not exist in the public schema.
        with self.assertRaises(DummyModel.DoesNotExist):
            with schema_context(get_public_schema_name()):
                update_task.apply_async(args=(self.dummy2.pk, 'updated-name'))

        self.assertEqual(connection.schema_name, self.tenant2.schema_name)
