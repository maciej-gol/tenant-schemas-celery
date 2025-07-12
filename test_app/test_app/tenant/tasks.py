from test_app.celery_app import app
from django.db import connection


@app.task
def print_schema():
    print(connection.schema_name)


@app.task
def periodic_print_schema():
    print_schema()
