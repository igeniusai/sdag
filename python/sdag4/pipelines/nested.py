from sdag4 import pipeline, task


@task("butta.sh")
def my_task(): ...


@pipeline
def inner():
    my_task()


@pipeline
def outer():
    inner()
