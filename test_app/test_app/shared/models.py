from __future__ import unicode_literals

from django.db import models

try:
    from tenant_schemas.models import TenantMixin

except ImportError:
    from django_tenants.models import TenantMixin


class Client(TenantMixin):
    name = models.CharField(max_length=16)
