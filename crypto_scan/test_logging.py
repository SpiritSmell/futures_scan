#!/usr/bin/env python3
"""
Тестовый скрипт для проверки настройки логирования в модуле save_to_clickhouse_json.py
"""

import sys
import os

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, os.path.dirname(__file__))

def test_logging():
    """Тестирует настройку логирования"""
    print("=== Тестирование настройки логирования ===")
    
    # Импортируем модуль с настроенным логированием
    from save_to_clickhouse_json import (
        module_logger, 
        safe_get_nested_data, 
        extract_json_data_safely,
        transform_futures_data
    )
    
    print("\n1. Тестирование прямых вызовов логгера:")
    module_logger.debug("Это DEBUG сообщение (не должно отображаться)")
    module_logger.info("Это INFO сообщение (должно отображаться)")
    module_logger.warning("Это WARNING сообщение (должно отображаться)")
    module_logger.error("Это ERROR сообщение (должно отображаться)")
    
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
    
    print("\n4. Тестирование функции transform_futures_data:")
    if extracted_data:
        transformed = transform_futures_data(extracted_data)
        print(f"Трансформированные данные: {transformed}")
    
    print("\n=== Тестирование завершено ===")

if __name__ == "__main__":
    test_logging()
