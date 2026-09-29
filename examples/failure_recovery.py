from sdag import pipeline


@pipeline
def failure():
    """Pipeline that will always fail."""
    t = failed_task()
    will_never_run(t)


@failure.task("scripts/submit.sh", retries=2)
def failed_task():
    """Failed task."""
    msg = "Failing..."
    raise ValueError(msg)


@failure.task("scripts/submit.sh")
def will_never_run():
    """This will never run."""
    print("You will never see this message")
