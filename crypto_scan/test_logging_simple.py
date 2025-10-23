#!/usr/bin/env python3
"""
Простой тестовый скрипт для проверки настройки логирования без внешних зависимостей
"""

import logging
import sys
import os
from typing import Any, Dict, List, Optional

def setup_logging():
    """
    Настраивает логирование для вывода сообщений уровня INFO и выше.
    """
    # Настройка корневого логгера
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Вывод в консоль
            logging.FileHandler('test_logging.log', encoding='utf-8')  # Запись в файл
        ]
    )
    
    # Настройка конкретного логгера для этого модуля
    module_logger = logging.getLogger(__name__)
    module_logger.setLevel(logging.INFO)
    
    return module_logger

def safe_get_nested_data(data: Dict[str, Any], path: List[str], default: Any = None) -> Any:
    """
    Безопасно извлекает вложенные данные из словаря по указанному пути.
    """
    module_logger = logging.getLogger(__name__)
    try:
        current = data
        for key in path:
            if isinstance(current, dict):
                current = current.get(key)
            elif isinstance(current, list) and isinstance(key, int):
                if 0 <= key < len(current):
                    current = current[key]
                else:
                    return default
            else:
                return default
            
            if current is None:
                return default
        
        return current
    except (KeyError, IndexError, TypeError) as e:
        module_logger.warning(f"Ошибка при извлечении данных по пути {path}: {e}")
        return default

def extract_json_data_safely(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Безопасно извлекает данные из JSON структуры.
    """
    module_logger = logging.getLogger(__name__)
    
    # Возможные пути к данным (в порядке приоритета)
    possible_paths = [
        ['data', 'items', 0, 'data'],  # Оригинальный путь
        ['data', 'items', 0],          # Без последнего 'data'
        ['data', 'items'],             # Массив items целиком
        ['data'],                      # Только data
        [],                            # Корневой уровень
    ]
    
    for path in possible_paths:
        result = safe_get_nested_data(data, path)
        if result is not None:
            module_logger.info(f"Данные найдены по пути: {path}")
            return result
    
    module_logger.error("Не удалось найти данные ни по одному из известных путей")
    return None

def test_logging():
    """Тестирует настройку логирования"""
    print("=== Тестирование настройки логирования ===")
    
    # Настраиваем логирование
    module_logger = setup_logging()
    
    print("\n1. Тестирование прямых вызовов логгера:")
    module_logger.debug("Это DEBUG сообщение (не должно отображаться)")
    module_logger.info("✅ Это INFO сообщение (должно отображаться)")
    module_logger.warning("⚠️ Это WARNING сообщение (должно отображаться)")
    module_logger.error("❌ Это ERROR сообщение (должно отображаться)")
    
    print("\n2. Тестирование функции safe_get_nested_data:")
    test_data = {
        "data": {
            "items": [
                {"data": {"binance": {"BTC/USD": {"price": 50000}}}}
            ]
        }
    }
    
    # Успешное извлечение данных
    result = safe_get_nested_data(test_data, ['data', 'items', 0, 'data'])
    print(f"Результат извлечения: {result}")
    
    # Ошибка извлечения данных
    result_error = safe_get_nested_data(test_data, ['nonexistent', 'path'])
    print(f"Результат с ошибкой: {result_error}")
    
    print("\n3. Тестирование функции extract_json_data_safely:")
    extracted_data = extract_json_data_safely(test_data)
    print(f"Извлеченные данные: {extracted_data}")
    
    # Тест с неправильной структурой данных
    wrong_data = {"wrong_structure": "no_data"}
    extracted_wrong = extract_json_data_safely(wrong_data)
    print(f"Данные с неправильной структурой: {extracted_wrong}")
    
    print("\n=== Тестирование завершено ===")
    print("Проверьте файл test_logging.log для записанных логов")

if __name__ == "__main__":
    test_logging()
