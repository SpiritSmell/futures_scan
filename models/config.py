from pydantic import BaseModel
from typing import List, Dict


class RabbitMQConfig(BaseModel):
    host: str
    port: int
    user: str
    password: str
    exchange: str


class CollectionConfig(BaseModel):
    interval_seconds: int
    retry_attempts: int
    retry_delays: List[int]


class LoggingConfig(BaseModel):
    level: str
    file: str


class Config(BaseModel):
    rabbitmq: RabbitMQConfig
    exchanges: List[str]
    symbols: List[str]
    collection: CollectionConfig
    logging: LoggingConfig
    api_keys_file: str
