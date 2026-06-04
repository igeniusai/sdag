from sdag4 import Elif, Else, If, pipeline, task
from sdag4.pipelines.example import foopipe


@pipeline
def foopipe2():
    with If(hello(b="h")):
        t = foopipe()
        foopipe(t)
    with Elif(hello(b="c")):
        hello(b="a")
    with Else():
        hello(b="b")


@task("butta.sh")
def hello(b: str): ...
