from pathlib import Path


class Settings:
    """Constants that are needed in several scripts."""

    PROJECT_ROOT = Path(__file__).resolve().parent
    DATA_DIR = rf"{PROJECT_ROOT}\data"
    RAW_DIR = rf"{DATA_DIR}\raw"
