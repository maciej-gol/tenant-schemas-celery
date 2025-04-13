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


class ClientFactory:
    def __init__(self) -> None:
        self.__clients = []

    def create_client(self, name: str, schema_name: str, domain_url: str) -> Client:
        client = create_client(name, schema_name, domain_url)
        self.__clients.append(client)
        return client

    def __enter__(self) -> None:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.teardown()

    def teardown(self) -> None:
        for client in self.__clients:
            client.delete()
