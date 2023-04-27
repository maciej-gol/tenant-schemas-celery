from __future__ import unicode_literals

from django.db import models
from django_tenants.models import TenantMixin


class Client(TenantMixin):
    name = models.CharField(max_length=16)
    ready = models.BooleanField(default=False)


class Domain(TenantMixin):
    name = models.CharField(max_length=16)
