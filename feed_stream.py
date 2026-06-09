import argparse
import shutil
import time
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
SOURCE_DIR = BASE_DIR / "data" / "source"
STREAM_INPUT_DIR = BASE_DIR / "data" / "stream_input"


def main():
    parser = argparse.ArgumentParser(
        description="Copies sample CSV batches into the watched streaming folder."
    )
    parser.add_argument("--delay", type=int, default=12, help="Seconds between files")
    args = parser.parse_args()

    STREAM_INPUT_DIR.mkdir(parents=True, exist_ok=True)

    batches = sorted(SOURCE_DIR.glob("batch_*.csv"))
    if not batches:
        raise FileNotFoundError(f"No sample batches found in {SOURCE_DIR}")

    for batch in batches:
        destination = STREAM_INPUT_DIR / batch.name
        shutil.copy2(batch, destination)
        print(f"Sent {batch.name} to stream input folder")
        time.sleep(args.delay)


if __name__ == "__main__":
    main()
