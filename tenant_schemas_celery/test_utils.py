from __future__ import absolute_import

from django.core.exceptions import FieldDoesNotExist

from test_app.shared.models import Client


def create_client(name, schema_name, domain_url):
    kwargs = {}
    try:
        Client._meta.get_field("domain_url")
    except FieldDoesNotExist:
        pass
    else:
        kwargs = {"domain_url": domain_url}
    tenant1 = Client(name=name, schema_name=schema_name, **kwargs)
    tenant1.save()
    return tenant1
