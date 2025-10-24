# Futures Data Collector

Сборщик real-time данных о фьючерсах с криптовалютных бирж с отправкой в RabbitMQ.

## Описание

Приложение собирает данные о фьючерсах (цены, orderbook, funding rate, mark price) с нескольких бирж одновременно и отправляет их в RabbitMQ для дальнейшей обработки.

**Возможности:**
- ✅ Параллельный сбор данных с 5 бирж
- ✅ Retry логика с экспоненциальной задержкой (3 попытки: 1s, 2s, 4s)
- ✅ Изоляция ошибок между биржами
- ✅ Периодическая статистика каждые 60 секунд
- ✅ Graceful shutdown
- ✅ Публикация в RabbitMQ с topic routing

## Технологии

- Python 3.11+
- ccxt - работа с API бирж
- aio-pika - асинхронная работа с RabbitMQ
- pydantic - валидация данных

## Поддерживаемые биржи

- Binance
- Bybit
- Bitget
- HTX (Huobi)
- Gate.io

## Установка

### 1. Клонировать репозиторий
```bash
git clone <repo_url>
cd futures_scan
```

### 2. Создать виртуальное окружение
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# или
.venv\Scripts\activate  # Windows
```

### 3. Установить зависимости
```bash
pip install -r requirements.txt
```

### 4. Настроить конфигурацию
```bash
cp config.example.json config.json
cp .env.keys.example .env.keys
```

### 5. Отредактировать `config.json`
```json
{
  "rabbitmq": {
    "host": "localhost",
    "port": 5672,
    "user": "guest",
    "password": "guest",
    "exchange": "futures_data"
  },
  "exchanges": ["binance", "bybit", "bitget"],
  "symbols": ["BTC/USDT:USDT", "ETH/USDT:USDT"],
  "collection": {
    "interval_seconds": 5,
    "retry_attempts": 3,
    "retry_delays": [1, 2, 4]
  },
  "logging": {
    "level": "INFO",
    "file": "app.log"
  }
}
```

### 6. Отредактировать `.env.keys` (опционально)
```json
{
  "binance": {
    "apiKey": "your_api_key",
    "secret": "your_secret"
  }
}
```

**Примечание:** API ключи не обязательны для публичных данных (ticker, orderbook, funding rate).

## Запуск

```bash
python main.py
```

**Пример вывода:**
```
2025-10-24 13:28:04,533 [INFO] [main] Futures Data Collector started
2025-10-24 13:28:04,533 [INFO] [main] Exchanges: ['binance', 'bybit', 'bitget']
2025-10-24 13:28:04,533 [INFO] [main] Symbols: ['BTC/USDT:USDT', 'ETH/USDT:USDT']
2025-10-24 13:28:04,533 [INFO] [rabbitmq] Connected to RabbitMQ at localhost:5672
2025-10-24 13:28:04,533 [INFO] [binance] Starting collector for binance
2025-10-24 13:28:04,533 [INFO] [binance] Successfully collected BTC/USDT:USDT
2025-10-24 13:28:04,533 [INFO] [rabbitmq] Published to futures.binance.BTCUSDTUSDT

=== Statistics (last 60s) ===
Binance: 14 success, 0 errors
Bybit: 14 success, 0 errors
Bitget: 13 success, 0 errors
RabbitMQ: 41 published, 0 failed
=============================
```

## Конфигурация

### config.json

| Параметр | Описание | Значение по умолчанию |
|----------|----------|----------------------|
| `rabbitmq.host` | Адрес RabbitMQ сервера | `localhost` |
| `rabbitmq.port` | Порт RabbitMQ | `5672` |
| `rabbitmq.exchange` | Имя exchange | `futures_data` |
| `exchanges` | Список бирж | `["binance", "bybit", "bitget"]` |
| `symbols` | Список символов фьючерсов | `["BTC/USDT:USDT", "ETH/USDT:USDT"]` |
| `collection.interval_seconds` | Интервал сбора данных | `5` |
| `collection.retry_attempts` | Количество попыток при ошибках | `3` |
| `collection.retry_delays` | Задержки между попытками (сек) | `[1, 2, 4]` |
| `logging.level` | Уровень логирования | `INFO` |
| `logging.file` | Файл для логов | `app.log` |

### .env.keys

JSON файл с API ключами для каждой биржи (опционально):
```json
{
  "binance": {
    "apiKey": "your_api_key",
    "secret": "your_secret"
  },
  "bybit": {
    "apiKey": "your_api_key",
    "secret": "your_secret"
  }
}
```

## Формат данных

Данные отправляются в RabbitMQ в формате JSON с routing key: `futures.{exchange}.{symbol}`

**Примеры routing keys:**
- `futures.binance.BTCUSDTUSDT`
- `futures.bybit.ETHUSDTUSDT`

**Структура сообщения:**
```json
{
  "exchange": "binance",
  "symbol": "BTC/USDT:USDT",
  "timestamp": 1729765084533,
  "ticker": {
    "bid": 110332.2,
    "ask": 110332.9,
    "last": 110332.5,
    "volume_24h": 1234567890.0
  },
  "orderbook": {
    "bids": [[110332.2, 1.5], [110332.1, 2.3]],
    "asks": [[110332.9, 1.2], [110333.0, 3.1]],
    "timestamp": 1729765084500
  },
  "funding_rate": 0.00002755,
  "next_funding_time": 1729766400000,
  "mark_price": 110332.5
}
```

## Мониторинг

### Логи
- Выводятся в консоль и файл `app.log`
- Формат: `[LEVEL] [component] message`
- Уровни: DEBUG, INFO, WARNING, ERROR

### Статистика
Выводится каждые 60 секунд:
```
=== Statistics (last 60s) ===
Binance: 14 success, 0 errors
Bybit: 14 success, 0 errors
RabbitMQ: 28 published, 0 failed
=============================
```

### RabbitMQ Management UI
`http://localhost:15672` (логин: guest, пароль: guest)

## Остановка

Нажмите `Ctrl+C` для graceful shutdown. Все задачи будут корректно завершены.

## Структура проекта

```
futures_scan/
├── main.py                    # Точка входа
├── config.json                # Конфигурация
├── .env.keys                  # API ключи
├── collectors/                # Сборщики данных с бирж
├── publishers/                # Отправка в RabbitMQ
├── models/                    # Модели данных
├── utils/                     # Утилиты (конфиг, логирование)
└── tests/                     # Тесты
```

## Документация

- [vision.md](vision.md) - техническое видение проекта
- [doc/tasklist.md](doc/tasklist.md) - план разработки

## Принципы

Проект следует принципу KISS - максимальная простота без оверинжиниринга.
