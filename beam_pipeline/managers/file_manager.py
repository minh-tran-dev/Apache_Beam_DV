import json
from pathlib import Path


def get_json_from_path(json_path: Path) -> dict:
    with json_path.open("r") as source:
        return json.load(source)


def get_json_from_file(json: dict) -> dict:
    return json.load(json)


def get_verify_conf() -> dict:
    verify_conf_path = (
        Path(__file__).parent.parent.parent
        / "resources"
        / "config"
        / "pipeline_conf.json"
    )
    with verify_conf_path.open("r") as conf:
        return json.load(conf)
