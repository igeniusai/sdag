# Batch Processing

Batch processing is one of the most common patterns one faces when dealing with data. There is typically a big dataset to be processed made of several splits (subfolders). These splits can be often processed independently as separate tasks, which helps reducing size and wall time of the job. Pipelines can be set up in this way when processing data in batches with sdag:

```py
from pathlib import Path

from sdag import Artifact, pipeline


@pipeline
def process_in_batches(input_path: str, base_output: str):
    for split in ["split1", "split2", "split3"]:
        output_path = Path(base_output) / split
        t = process_split(
            input_path=input_path, split=split, output_path=output_path
        )
        t.name = f"process-{split}"


@process_in_batches.task("script.sh", cache=True)
def process_split(input_path: Path, split: str, output_path: Artifact[Path]):
    print(f"processing '{split}' and saving it to {output_path}")
```

In this way, splits are processed independently from each other. These tasks get custom names for easier traceability in the logs and separate caches. If some splits fail, you can use `sdag retry` to restart only the failed ones. If new splits are added to the dataset, the other ones got cached so they won't be processed again during subsequent runs.