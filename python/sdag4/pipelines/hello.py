from sdag4 import Script, pipeline


@pipeline
def hipipe():
    task = hello(name="sdag")


@hipipe.task(script=Script("echo Hi $NAME"), cmd="bash", mode="ext")
def hello(name: str): ...
