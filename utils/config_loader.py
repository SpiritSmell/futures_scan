import json
from typing import Dict
from models.config import Config


def load_config(config_path: str = "config.json") -> Config:
    """Загружает и валидирует конфигурацию из JSON файла"""
    with open(config_path, 'r') as f:
        config_dict = json.load(f)
    return Config(**config_dict)


def load_api_keys(keys_file: str) -> Dict[str, Dict[str, str]]:
    """Загружает API ключи из .env.keys файла"""
    with open(keys_file, 'r') as f:
        return json.load(f)
