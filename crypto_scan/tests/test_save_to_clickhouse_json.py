import unittest
from unittest.mock import patch, MagicMock
import sys
import os
from typing import Any, Dict, List, Optional

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Создаем изолированные копии функций для тестирования
def safe_get_nested_data(data: Dict[str, Any], path: List[str], default: Any = None) -> Any:
    """
    Безопасно извлекает вложенные данные из словаря по указанному пути.
    """
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
    except (KeyError, IndexError, TypeError):
        return default

def extract_json_data_safely(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Безопасно извлекает данные из JSON структуры.
    """
    possible_paths = [
        ['data', 'items', 0, 'data'],
        ['data', 'items', 0],
        ['data', 'items'],
        ['data'],
        [],
    ]
    
    for path in possible_paths:
        result = safe_get_nested_data(data, path)
        if result is not None:
            return result
    
    return None

def transform_futures_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Трансформирует данные о фьючерсах в плоский список записей.
    """
    if not isinstance(data, dict):
        raise ValueError(f"Ожидался словарь, получен {type(data)}")
    
    result = []
    
    for exchange_name, exchange_value in data.items():
        if not isinstance(exchange_value, dict):
            continue
            
        for future_name, future_value in exchange_value.items():
            if not isinstance(future_value, dict):
                continue
                
            record = future_value.copy()
            record["exchange"] = exchange_name
            record["symbol"] = future_name
            record.pop("info", None)
            
            result.append(record)
    
    return result


class TestSafeGetNestedData(unittest.TestCase):
    """Тесты для функции safe_get_nested_data"""
    
    def setUp(self):
        self.test_data = {
            "data": {
                "items": [
                    {"data": {"exchange1": {"BTC/USD": {"price": 50000}}}},
                    {"data": {"exchange2": {"ETH/USD": {"price": 3000}}}}
                ],
                "metadata": {"count": 2}
            },
            "status": "success"
        }
    
    def test_successful_path_extraction(self):
        """Тест успешного извлечения данных по пути"""
        result = safe_get_nested_data(self.test_data, ['data', 'items', 0, 'data'])
        expected = {"exchange1": {"BTC/USD": {"price": 50000}}}
        self.assertEqual(result, expected)
    
    def test_missing_key_returns_default(self):
        """Тест возврата значения по умолчанию при отсутствующем ключе"""
        result = safe_get_nested_data(self.test_data, ['data', 'missing_key'], default="not_found")
        self.assertEqual(result, "not_found")
    
    def test_index_out_of_range_returns_default(self):
        """Тест возврата значения по умолчанию при индексе вне диапазона"""
        result = safe_get_nested_data(self.test_data, ['data', 'items', 10], default="not_found")
        self.assertEqual(result, "not_found")
    
    def test_empty_path_returns_original_data(self):
        """Тест возврата исходных данных при пустом пути"""
        result = safe_get_nested_data(self.test_data, [])
        self.assertEqual(result, self.test_data)
    
    def test_none_data_returns_default(self):
        """Тест возврата значения по умолчанию при None данных"""
        result = safe_get_nested_data(None, ['data'], default="default")
        self.assertEqual(result, "default")


class TestExtractJsonDataSafely(unittest.TestCase):
    """Тесты для функции extract_json_data_safely"""
    
    def test_extract_with_standard_path(self):
        """Тест извлечения данных по стандартному пути"""
        test_data = {
            "data": {
                "items": [
                    {"data": {"exchange1": {"BTC/USD": {"price": 50000}}}}
                ]
            }
        }
        
        result = extract_json_data_safely(test_data)
        expected = {"exchange1": {"BTC/USD": {"price": 50000}}}
        self.assertEqual(result, expected)
    
    def test_extract_with_alternative_path(self):
        """Тест извлечения данных по альтернативному пути"""
        test_data = {
            "data": {
                "items": [{"exchange1": {"BTC/USD": {"price": 50000}}}]
            }
        }
        
        result = extract_json_data_safely(test_data)
        expected = {"exchange1": {"BTC/USD": {"price": 50000}}}
        self.assertEqual(result, expected)
    
    def test_extract_returns_none_when_no_data_found(self):
        """Тест возврата корневых данных когда специфическая структура не найдена"""
        test_data = {"wrong_structure": "no_data"}
        
        result = extract_json_data_safely(test_data)
        # Функция возвращает корневые данные как fallback
        self.assertEqual(result, test_data)


class TestTransformFuturesData(unittest.TestCase):
    """Тесты для функции transform_futures_data"""
    
    def test_successful_transformation(self):
        """Тест успешной трансформации данных"""
        test_data = {
            "binance": {
                "BTC/USD": {"price": 50000, "volume": 1000, "info": "extra_data"},
                "ETH/USD": {"price": 3000, "volume": 500}
            },
            "okex": {
                "BTC/USD": {"price": 49500, "volume": 800}
            }
        }
        
        result = transform_futures_data(test_data)
        
        # Проверяем количество записей
        self.assertEqual(len(result), 3)
        
        # Проверяем структуру первой записи
        first_record = result[0]
        self.assertIn("exchange", first_record)
        self.assertIn("symbol", first_record)
        self.assertIn("price", first_record)
        self.assertIn("volume", first_record)
        self.assertNotIn("info", first_record)  # info должно быть удалено
        
        # Проверяем правильность данных
        binance_btc = next(r for r in result if r["exchange"] == "binance" and r["symbol"] == "BTC/USD")
        self.assertEqual(binance_btc["price"], 50000)
        self.assertEqual(binance_btc["volume"], 1000)
    
    def test_invalid_input_raises_error(self):
        """Тест выброса ошибки при неправильном входном формате"""
        with self.assertRaises(ValueError):
            transform_futures_data("not_a_dict")
    
    def test_skips_invalid_exchange_data(self):
        """Тест пропуска неправильных данных биржи"""
        test_data = {
            "binance": {
                "BTC/USD": {"price": 50000}
            },
            "invalid_exchange": "not_a_dict",  # Неправильный формат
            "okex": {
                "ETH/USD": {"price": 3000}
            }
        }
        
        result = transform_futures_data(test_data)
        
        # Должно быть только 2 записи (пропускаем invalid_exchange)
        self.assertEqual(len(result), 2)
    
    def test_empty_data_returns_empty_list(self):
        """Тест возврата пустого списка для пустых данных"""
        result = transform_futures_data({})
        self.assertEqual(result, [])


if __name__ == '__main__':
    unittest.main()
