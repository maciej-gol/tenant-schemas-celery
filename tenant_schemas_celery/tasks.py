@app.task
def update_task(model_id, name):
    dummy = DummyModel.objects.get(pk=model_id)
    dummy.name = name
    dummy.save()

@app.task(bind=True)
def update_retry_task(self, model_id, name):
    connection.close()
    if update_retry_task.request.retries:
        return update_task(model_id, name)

    # Don't throw the Retry exception.
    self.retry(throw=False)
