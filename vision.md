# Техническое видение проекта: Сборщик данных о фьючерсах

> **Принцип разработки: KISS (Keep It Simple, Stupid)**  
> Создаем максимально простое решение для проверки идеи. Никакого оверинжиниринга.

---

## 1. Технологии

### Основной стек
- **Python 3.11+** - основной язык разработки
- **ccxt** - универсальная библиотека для работы с API бирж
- **aio-pika** - асинхронная работа с RabbitMQ
- **asyncio** - асинхронный сбор данных

### Целевые биржи (MVP)
- Bitget
- Bybit  
- Binance
- HTX
- Gate.io

### Инфраструктура
- **RabbitMQ** - брокер сообщений для передачи данных
- **Docker** - для запуска RabbitMQ локально

### Библиотеки
- **pydantic** - валидация данных и конфигурации
- **python-dotenv** - управление переменными окружения

### Что НЕ используем в MVP
- ❌ Базы данных
- ❌ Kubernetes
- ❌ Микросервисы
- ❌ Сложные системы мониторинга

---

## 2. Принципы разработки

### Основные принципы
- **KISS** - максимальная простота, минимум абстракций
- **Fail Fast** - быстрое обнаружение ошибок, явные исключения
- **Single Responsibility** - один модуль = одна задача
- **Конфигурация через файлы** - простые JSON/YAML конфиги

### Подход к разработке
- **Монолитное приложение** - один Python процесс
- **Асинхронность** - asyncio для параллельного сбора с бирж
- **Без тестов в MVP** - сначала проверяем идею
- **Минимум зависимостей** - только необходимое

### Обработка ошибок
- **Retry при сбоях API** - автоматические повторы с экспоненциальной задержкой
- **Graceful degradation** - если одна биржа недоступна, остальные продолжают работать
- **Логирование всех ошибок** - приложение не падает при ошибках отдельных бирж
- **Изоляция ошибок** - проблема на одной бирже не влияет на другие

### Что НЕ делаем в MVP
- ❌ Unit/Integration тесты
- ❌ CI/CD pipeline
- ❌ Code coverage
- ❌ Сложные паттерны проектирования

---

## 3. Структура проекта

```
futures_scan/
├── main.py                    # Точка входа, запуск приложения
├── config.json                # Конфигурация (биржи, RabbitMQ, параметры)
├── .env                       # Секретные данные (API ключи)
├── requirements.txt           # Зависимости Python
├── README.md                  # Документация по запуску
├── collectors/
│   ├── __init__.py
│   ├── base_collector.py      # Базовый класс для сборщика
│   └── exchange_collector.py  # Сборщик данных с бирж через ccxt
├── publishers/
│   ├── __init__.py
│   └── rabbitmq_publisher.py  # Отправка данных в RabbitMQ
├── models/
│   ├── __init__.py
│   ├── config.py              # Pydantic модели конфигурации
│   └── futures_data.py        # Pydantic модели данных фьючерсов
├── utils/
│   ├── __init__.py
│   ├── config_loader.py       # Загрузка и валидация конфигурации
│   └── logger.py              # Настройка логирования
└── tests/
    ├── __init__.py
    └── (тесты добавим позже)
```

### Принципы организации
- **Плоская структура** - максимум 2 уровня вложенности
- **Один файл = одна ответственность**
- **Явные имена** - понятно что делает каждый модуль
- **Минимум файлов** - только необходимое

---

## 4. Архитектура проекта

### Компоненты и поток данных

```
┌─────────────────────────────────────────────────────────┐
│                      main.py                            │
│  - Загрузка конфигурации                                │
│  - Инициализация логгера                                │
│  - Запуск сборщиков для каждой биржи                    │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│              ExchangeCollector (для каждой биржи)       │
│  - Подключение к API биржи через ccxt                   │
│  - Получение данных о фьючерсах                         │
│  - Retry при ошибках с экспоненциальной задержкой       │
│  - Работает в отдельной async задаче                    │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│              RabbitMQPublisher (один экземпляр)         │
│  - Подключение к RabbitMQ                               │
│  - Отправка данных в exchange                           │
│  - При недоступности RabbitMQ - логируем ошибку         │
└─────────────────────────────────────────────────────────┘
```

### Принципы архитектуры
- **Асинхронность** - каждая биржа работает независимо в своей async задаче
- **Один publisher** - все сборщики используют один экземпляр RabbitMQPublisher
- **Изоляция ошибок** - падение одного сборщика не влияет на другие
- **Простой retry** - при ошибке API ждем и повторяем с экспоненциальной задержкой
- **Без буферизации** - если RabbitMQ недоступен, данные теряются (логируем ошибку)

### Жизненный цикл
1. **Старт**: загрузка конфигурации, подключение к RabbitMQ
2. **Запуск**: N async задач (по одной на биржу)
3. **Цикл**: получить данные → отправить в RabbitMQ → подождать 5 секунд
4. **Shutdown**: graceful shutdown всех задач при Ctrl+C

### Параметры сбора
- **Интервал**: минимум 5 секунд между запросами
- **Без буферизации**: данные отправляются сразу или теряются

### Что НЕ делаем
- ❌ Очереди между компонентами
- ❌ Сложные state machines
- ❌ Event-driven архитектура
- ❌ Буферизация данных

---

## 5. Модель данных

### Структура данных фьючерса (Pydantic модели)

```python
class FuturesData(BaseModel):
    # Метаданные
    exchange: str              # Название биржи (binance, bybit, etc.)
    symbol: str                # Символ фьючерса (BTC/USDT:USDT)
    timestamp: int             # Unix timestamp в миллисекундах
    
    # Основные данные (обязательные)
    ticker: TickerData         # Цены bid/ask/last
    orderbook: OrderbookData   # Стакан заявок (максимальная глубина)
    
    # Данные фандинга
    funding_rate: Optional[float]           # Текущая ставка фандинга
    next_funding_time: Optional[int]        # Время следующего фандинга (unix timestamp)
    mark_price: Optional[float]             # Марк цена
    
    # Расширяемость (для будущих параметров)
    extra: Optional[Dict[str, Any]] = None  # Любые дополнительные поля

class TickerData(BaseModel):
    bid: float                 # Лучшая цена покупки
    ask: float                 # Лучшая цена продажи
    last: float                # Последняя цена сделки
    volume_24h: Optional[float] = None  # Объем за 24 часа

class OrderbookData(BaseModel):
    bids: List[List[float]]    # [[price, amount], ...] - максимальная глубина
    asks: List[List[float]]    # [[price, amount], ...] - максимальная глубина
    timestamp: int             # Timestamp стакана
```

### Формат отправки в RabbitMQ
- **Формат**: JSON (сериализация через Pydantic)
- **Routing key**: `futures.{exchange}.{symbol}` 
  - Пример: `futures.binance.BTCUSDT`
- **Exchange type**: topic (для гибкой маршрутизации подписчиков)
- **Кодировка**: UTF-8

### Принципы модели данных
- **Расширяемость** - поле `extra` для будущих параметров без изменения схемы
- **Optional поля** - не все биржи предоставляют все данные
- **Типизация** - Pydantic для автоматической валидации
- **Простота** - плоская структура, минимум вложенности
- **Конфигурируемость** - список символов задается в config.json

### Что собираем
- ✅ Ticker (цены)
- ✅ Orderbook (максимальная глубина, которую дает биржа)
- ✅ Funding rate
- ✅ Next funding time
- ✅ Mark price
- ❌ Исторические trades (не нужны)

---

## 6. Мониторинг

### Что мониторим
- **Работоспособность сборщиков** - каждый сборщик логирует успешные запросы
- **Ошибки API** - все ошибки от бирж в лог с деталями
- **Отправка в RabbitMQ** - успех/неудача публикации
- **Счетчики** - количество успешных/неудачных запросов

### Инструменты мониторинга
- **Логи** - основной инструмент мониторинга (уровень DEBUG)
- **Консоль** - вывод статуса в stdout при запуске
- **Периодическая статистика** - каждые 60 секунд вывод сводки в консоль
- **RabbitMQ Management UI** - встроенный веб-интерфейс для мониторинга очередей

### Формат логов
```
[DEBUG] [binance] Fetching data for BTC/USDT:USDT
[INFO] [binance] Successfully collected data for BTC/USDT:USDT
[ERROR] [bybit] Failed to fetch orderbook: Connection timeout (retry 1/3)
[INFO] [rabbitmq] Published message to futures.binance.BTCUSDT
[WARNING] [htx] Funding rate not available for ETH/USDT:USDT
```

### Периодическая статистика (каждые 60 секунд)
```
=== Statistics (last 60s) ===
Binance: 12 success, 0 errors
Bybit: 11 success, 1 errors
HTX: 12 success, 0 errors
RabbitMQ: 35 published, 0 failed
=============================
```

### Что НЕ делаем в MVP
- ❌ Prometheus + Grafana
- ❌ Health check endpoints
- ❌ Alerting системы
- ❌ Метрики в отдельной БД
- ❌ Distributed tracing

### Простой мониторинг
- Смотрим логи: `tail -f app.log`
- Проверяем RabbitMQ UI: `http://localhost:15672`
- Статистика в консоли каждую минуту

---

## 7. Сценарии работы

### Сценарий 1: Запуск приложения
1. Пользователь запускает: `python main.py`
2. Приложение загружает `config.json`
3. Подключается к RabbitMQ
4. Запускает сборщики для каждой биржи из конфига
5. Начинает сбор данных каждые 5 секунд
6. Выводит статистику каждые 60 секунд

### Сценарий 2: Нормальная работа
1. Каждые 5 секунд сборщик запрашивает данные с биржи
2. Получает ticker, orderbook, funding rate, mark price
3. Валидирует данные через Pydantic
4. Отправляет в RabbitMQ с routing key `futures.{exchange}.{symbol}`
5. Логирует результат

### Сценарий 3: Ошибка API биржи
1. Запрос к бирже падает с ошибкой
2. Логируем ошибку с деталями
3. Retry с экспоненциальной задержкой: 1s → 2s → 4s
4. Максимум 3 попытки
5. После 3 неудачных попыток - пропускаем итерацию, переходим к следующему циклу

### Сценарий 4: RabbitMQ недоступен
1. Попытка отправить данные в RabbitMQ
2. Ошибка подключения
3. Логируем ошибку
4. Данные теряются (без буферизации)
5. Продолжаем работу

### Сценарий 5: Остановка приложения
1. Пользователь нажимает Ctrl+C
2. Приложение получает сигнал SIGINT
3. Graceful shutdown: завершаем все async задачи
4. Закрываем соединение с RabbitMQ
5. Выводим финальную статистику
6. Выход

### Сценарий 6: Добавление нового символа (через Control API)
1. Пользователь отправляет команду в RabbitMQ control queue
2. Control listener получает команду и обновляет список символов
3. Отправляет ответ с результатом выполнения
4. Все collectors начинают собирать данные для нового символа без перезапуска

**Альтернатива:** Редактирование `config.json` и перезапуск приложения

---

## 6.5. Control API через RabbitMQ

### Архитектура управления
- **Control Queue** - очередь для входящих команд управления
- **Response Exchange** - topic exchange для ответов на команды
- **Shared State** - разделяемое состояние между всеми collectors
- **Control Listener** - асинхронная задача для обработки команд

### Control Queue
- **Имя**: `futures_collector_control`
- **Тип**: durable queue
- **Формат сообщений**: JSON

### Response Exchange
- **Имя**: `futures_collector_responses`
- **Тип**: topic exchange
- **Routing key**: `control.response.{command}`

### Поддерживаемые команды

#### 1. add_symbol - Добавить символ
```json
{
  "command": "add_symbol",
  "symbol": "SOL/USDT:USDT",
  "correlation_id": "uuid-12345",
  "timestamp": 1730107603
}
```

**Ответ (успех):**
```json
{
  "correlation_id": "uuid-12345",
  "success": true,
  "command": "add_symbol",
  "message": "Symbol SOL/USDT:USDT added successfully",
  "data": {
    "symbol": "SOL/USDT:USDT",
    "current_symbols": ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]
  },
  "timestamp": 1730107604
}
```

**Ответ (ошибка - дубликат):**
```json
{
  "correlation_id": "uuid-12345",
  "success": false,
  "command": "add_symbol",
  "message": "Symbol SOL/USDT:USDT already exists",
  "error": "duplicate_symbol",
  "timestamp": 1730107604
}
```

#### 2. remove_symbol - Удалить символ
```json
{
  "command": "remove_symbol",
  "symbol": "ETH/USDT:USDT",
  "correlation_id": "uuid-12346",
  "timestamp": 1730107605
}
```

**Ответ (успех):**
```json
{
  "correlation_id": "uuid-12346",
  "success": true,
  "command": "remove_symbol",
  "message": "Symbol ETH/USDT:USDT removed successfully",
  "data": {
    "symbol": "ETH/USDT:USDT",
    "current_symbols": ["BTC/USDT:USDT", "SOL/USDT:USDT"]
  },
  "timestamp": 1730107606
}
```

#### 3. set_symbols - Заменить весь список
```json
{
  "command": "set_symbols",
  "symbols": ["BTC/USDT:USDT", "SOL/USDT:USDT", "DOGE/USDT:USDT"],
  "correlation_id": "uuid-12347",
  "timestamp": 1730107607
}
```

#### 4. get_symbols - Получить текущий список
```json
{
  "command": "get_symbols",
  "correlation_id": "uuid-12348",
  "timestamp": 1730107608
}
```

**Ответ:**
```json
{
  "correlation_id": "uuid-12348",
  "success": true,
  "command": "get_symbols",
  "message": "Symbols retrieved successfully",
  "data": {
    "symbols": ["BTC/USDT:USDT", "ETH/USDT:USDT"],
    "count": 2
  },
  "timestamp": 1730107609
}
```

#### 5. get_statistics - Получить статистику
```json
{
  "command": "get_statistics",
  "correlation_id": "uuid-12349",
  "timestamp": 1730107610
}
```

**Ответ:**
```json
{
  "correlation_id": "uuid-12349",
  "success": true,
  "command": "get_statistics",
  "message": "Statistics retrieved successfully",
  "data": {
    "exchange_success": {"binance": 120, "bybit": 118},
    "exchange_errors": {"htx": 5},
    "rabbitmq_published": 238,
    "rabbitmq_failed": 0
  },
  "timestamp": 1730107611
}
```

### Коды ошибок
- `invalid_command` - отсутствует обязательное поле или неверный формат
- `invalid_json` - невалидный JSON в сообщении
- `duplicate_symbol` - символ уже существует (add_symbol)
- `symbol_not_found` - символ не найден (remove_symbol)
- `unknown_command` - неизвестная команда
- `internal_error` - внутренняя ошибка сервиса
- `timeout` - нет ответа в течение заданного времени

### Использование через скрипт
```bash
# Добавить символ
python scripts/control_client.py add_symbol "SOL/USDT:USDT"

# Удалить символ
python scripts/control_client.py remove_symbol "ETH/USDT:USDT"

# Заменить список
python scripts/control_client.py set_symbols "BTC/USDT:USDT,SOL/USDT:USDT"

# Получить список
python scripts/control_client.py get_symbols

# Получить статистику
python scripts/control_client.py get_statistics
```

### Принципы Control API
- **Асинхронность** - команды обрабатываются независимо от collectors
- **Обратная связь** - каждая команда получает ответ с результатом
- **Correlation ID** - для связи команды и ответа
- **Валидация** - все команды валидируются перед выполнением
- **Логирование** - все команды и ответы логируются
- **Thread-safe** - использование asyncio.Lock для безопасности

### Что НЕ делаем в MVP
- ❌ Аутентификация команд
- ❌ Rate limiting
- ❌ История команд
- ❌ Rollback изменений
- ❌ Batch операции

---

## 8. Деплой

### Локальный запуск (MVP)
```bash
# 1. Клонируем репозиторий
git clone <repo_url>
cd futures_scan

# 2. Устанавливаем зависимости
pip install -r requirements.txt

# 3. Настраиваем конфигурацию
cp config.example.json config.json
# Редактируем config.json (указываем адрес RabbitMQ сервера)

# 4. Запускаем приложение
python main.py
```

### Конфигурация RabbitMQ
- RabbitMQ работает на отдельном сервере
- В `config.json` указываем host, port, credentials для подключения
- Приложение подключается к удаленному RabbitMQ

### Запуск в фоне (опционально)
```bash
# Через screen
screen -S futures_scan
python main.py
# Ctrl+A, D для отключения

# Через nohup
nohup python main.py > app.log 2>&1 &
```

### Что НЕ делаем в MVP
- ❌ Docker контейнер для приложения
- ❌ Kubernetes
- ❌ CI/CD pipeline
- ❌ Автоматические обновления
- ❌ Systemd service (пока не нужен)
- ❌ Удаленный деплой (только локально)

### Простой деплой для MVP
- Запуск на локальной машине
- RabbitMQ на отдельном сервере (уже настроен)
- Обновления вручную (git pull + restart)
- Логи в файл или консоль

---

## 9. Подход к конфигурированию

### Структура конфигурации

**config.json** - основная конфигурация:
```json
{
  "rabbitmq": {
    "host": "192.168.192.42",
    "port": 5672,
    "user": "rmuser",
    "password": "rmpassword",
    "exchange": "futures_data"
  },
  "exchanges": ["bitget", "bybit", "binance", "htx", "gateio"],
  "symbols": [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "SOL/USDT:USDT"
  ],
  "collection": {
    "interval_seconds": 5,
    "retry_attempts": 3,
    "retry_delays": [1, 2, 4]
  },
  "api_keys_file": ".env.keys"
}
```

**.env.keys** - API ключи бирж (JSON формат):
```json
{
  "binance": {
    "apiKey": "your_binance_key",
    "secret": "your_binance_secret"
  },
  "bybit": {
    "apiKey": "your_bybit_key",
    "secret": "your_bybit_secret"
  },
  "bitget": {
    "apiKey": "your_bitget_key",
    "secret": "your_bitget_secret"
  },
  "htx": {
    "apiKey": "your_htx_key",
    "secret": "your_htx_secret"
  },
  "gateio": {
    "apiKey": "your_gateio_key",
    "secret": "your_gateio_secret"
  }
}
```

### Логика сбора данных
- Для каждого символа из `symbols` проверяем его наличие на всех биржах из `exchanges`
- Если символ доступен на бирже - собираем данные
- Если символ недоступен - пропускаем с предупреждением (только в DEBUG режиме)
- Каждая биржа обрабатывает все доступные символы независимо

### Принципы конфигурирования
- **Разделение**: настройки в `config.json`, секреты в `.env.keys`
- **JSON формат** - простой и читаемый
- **Валидация через Pydantic** - проверка корректности при загрузке
- **Шаблоны**: `config.example.json` и `.env.keys.example` в репозитории
- **Гибкость**: легко добавить новую биржу или символ

### Файлы в репозитории
- ✅ `config.example.json` - шаблон конфигурации
- ✅ `.env.keys.example` - шаблон API ключей
- ✅ `.gitignore` - исключает `config.json` и `.env.keys`

### .gitignore
```
.env.keys
config.json
*.log
__pycache__/
*.pyc
```

---

## 10. Подход к логгированию

### Конфигурация логирования

**Уровни логов:**
- **DEBUG** - детальная информация (по умолчанию для MVP)
- **INFO** - успешные операции
- **WARNING** - предупреждения (недоступные символы, missing data)
- **ERROR** - ошибки API, RabbitMQ

**Куда пишем:**
- **Консоль (stdout)** - все логи + периодическая статистика
- **Файл app.log** - все логи для последующего анализа
- **Без ограничений размера** - для MVP не ограничиваем размер файла

### Формат логов
```
2025-10-23 12:45:30 [DEBUG] [binance] Fetching data for BTC/USDT:USDT
2025-10-23 12:45:31 [INFO] [binance] Successfully collected BTC/USDT:USDT
2025-10-23 12:45:31 [INFO] [rabbitmq] Published to futures.binance.BTCUSDT
2025-10-23 12:45:32 [ERROR] [bybit] API error: Connection timeout (retry 1/3)
2025-10-23 12:45:33 [DEBUG] [htx] Symbol SOL/USDT:USDT not available on exchange
```

### Структура логов
- **Timestamp** - точное время события
- **Level** - уровень важности
- **Component** - биржа или компонент (binance, rabbitmq, main)
- **Message** - описание события

### Настройка уровня логирования

Добавляем в **config.json**:
```json
{
  "logging": {
    "level": "DEBUG",
    "file": "app.log"
  }
}
```

Возможные значения: `DEBUG`, `INFO`, `WARNING`, `ERROR`

### Реализация в коде
```python
import logging

# Уровень из конфига
log_level = config.logging.level

logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    handlers=[
        logging.FileHandler(config.logging.file),
        logging.StreamHandler()
    ]
)
```

### Что НЕ делаем в MVP
- ❌ Structured logging (JSON logs)
- ❌ Централизованное хранилище логов
- ❌ Автоматическая ротация логов
- ❌ Отправка логов в внешние системы (ELK, Splunk)
- ❌ Ограничение размера файла

### Простое логирование
- Python `logging` модуль
- Один файл `app.log`
- Уровень настраивается через `config.json`
- Ручная очистка при необходимости

---

