from pathlib import Path

class Settings:
    PROJECT_ROOT = Path(__file__).resolve().parent
    DATA_DIR = f"{PROJECT_ROOT}\data"