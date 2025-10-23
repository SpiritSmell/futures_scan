import logging


def setup_logging(level: str, log_file: str) -> None:
    """Настраивает логирование согласно vision.md раздел 10"""
    logging.basicConfig(
        level=getattr(logging, level),
        format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
