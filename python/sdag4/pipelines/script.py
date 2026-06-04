from sdag4 import pipeline, task


@task("script.sh")
def global_task(): ...


@pipeline
def target_pipeline3():
    global_task()
    local_task()


@target_pipeline3.task("script.sh")
def local_task(): ...
