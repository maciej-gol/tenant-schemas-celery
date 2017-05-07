from celery_app import app
from django.db import connection


@app.task
def print_schema():
    print connection.schema_name
