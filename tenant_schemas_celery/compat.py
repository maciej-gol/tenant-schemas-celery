try:
    from django_tenants.test.cases import TenantTestCase
    from django_tenants.models import TenantMixin
    from django_tenants.utils import (
        get_public_schema_name,
        get_tenant_model,
        schema_context,
        tenant_context,
    )

except ImportError:
    from tenant_schemas.test.cases import TenantTestCase
    from tenant_schemas.models import TenantMixin
    from tenant_schemas.utils import (
        get_public_schema_name,
        get_tenant_model,
        schema_context,
        tenant_context,
    )
