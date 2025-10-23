# Futures Data Collector

Сборщик real-time данных о фьючерсах с криптовалютных бирж с отправкой в RabbitMQ.

## Описание

Приложение собирает данные о фьючерсах (цены, orderbook, funding rate, mark price) с нескольких бирж одновременно и отправляет их в RabbitMQ для дальнейшей обработки.

## Технологии

- Python 3.11+
- ccxt - работа с API бирж
- aio-pika - асинхронная работа с RabbitMQ
- pydantic - валидация данных

## Поддерживаемые биржи

- Binance
- Bybit
- Bitget
- HTX
- Gate.io

## Установка

1. Клонировать репозиторий:
```bash
git clone <repo_url>
cd futures_scan
```

2. Установить зависимости:
```bash
pip install -r requirements.txt
```

3. Настроить конфигурацию:
```bash
cp config.example.json config.json
cp .env.keys.example .env.keys
```

4. Отредактировать `config.json`:
   - Указать адрес RabbitMQ сервера
   - Выбрать биржи и символы для сбора

5. Отредактировать `.env.keys`:
   - Добавить API ключи для бирж

## Запуск

```bash
python main.py
```

## Конфигурация

### config.json

- `rabbitmq` - настройки подключения к RabbitMQ
- `exchanges` - список бирж для сбора данных
- `symbols` - список символов фьючерсов
- `collection.interval_seconds` - интервал сбора данных (по умолчанию 5 секунд)
- `collection.retry_attempts` - количество попыток при ошибках API
- `logging.level` - уровень логирования (DEBUG, INFO, WARNING, ERROR)

### .env.keys

JSON файл с API ключами для каждой биржи.

## Формат данных

Данные отправляются в RabbitMQ в формате JSON с routing key: `futures.{exchange}.{symbol}`

Пример: `futures.binance.BTCUSDT`

## Мониторинг

- Логи выводятся в консоль и файл `app.log`
- Статистика работы выводится каждые 60 секунд
- RabbitMQ Management UI: `http://localhost:15672` (если RabbitMQ локальный)

## Остановка

Нажмите `Ctrl+C` для корректного завершения работы.

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
