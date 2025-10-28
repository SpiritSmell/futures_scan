#!/usr/bin/env python3
"""
Control client для управления futures_scan через RabbitMQ
"""
import asyncio
import json
import sys
import uuid
import time
import aio_pika
from aio_pika import Message


class ControlClient:
    """Клиент для отправки команд управления"""
    
    def __init__(self, host="localhost", port=5672, user="guest", password="guest"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.control_queue = "futures_collector_control"
        self.response_exchange = "futures_collector_responses"
    
    async def send_command(self, command_data: dict, timeout: float = 5.0):
        """Отправить команду и дождаться ответа"""
        
        # Генерируем correlation_id
        correlation_id = str(uuid.uuid4())
        command_data["correlation_id"] = correlation_id
        command_data["timestamp"] = int(time.time())
        
        # Подключаемся к RabbitMQ
        connection = await aio_pika.connect_robust(
            host=self.host,
            port=self.port,
            login=self.user,
            password=self.password
        )
        
        channel = await connection.channel()
        
        # Создаем временную очередь для ответов
        response_queue = await channel.declare_queue(
            f"response_{correlation_id}",
            exclusive=True,
            auto_delete=True
        )
        
        # Привязываем к response exchange
        await response_queue.bind(
            self.response_exchange,
            routing_key=f"control.response.{command_data['command']}"
        )
        
        # Future для ответа
        response_future = asyncio.Future()
        
        async def on_response(message: aio_pika.IncomingMessage):
            async with message.process():
                response = json.loads(message.body.decode())
                if response.get("correlation_id") == correlation_id:
                    response_future.set_result(response)
        
        await response_queue.consume(on_response)
        
        # Отправляем команду
        await channel.default_exchange.publish(
            Message(
                body=json.dumps(command_data).encode('utf-8'),
                content_type='application/json',
                correlation_id=correlation_id
            ),
            routing_key=self.control_queue
        )
        
        print(f"⏳ Sending command: {command_data['command']}...")
        
        try:
            # Ждем ответ
            response = await asyncio.wait_for(response_future, timeout=timeout)
            await connection.close()
            return response
        except asyncio.TimeoutError:
            await connection.close()
            return {
                "success": False,
                "error": "timeout",
                "message": f"No response received within {timeout} seconds"
            }


def print_response(response: dict):
    """Красиво вывести ответ"""
    if response.get("success"):
        print(f"✓ Success: {response.get('message')}")
        if response.get("data"):
            data = response['data']
            # Форматируем вывод данных
            if 'symbols' in data:
                print(f"  Symbols ({data.get('count', len(data['symbols']))}):")
                for symbol in data['symbols']:
                    print(f"    • {symbol}")
            elif 'current_symbols' in data:
                print(f"  Current symbols:")
                for symbol in data['current_symbols']:
                    print(f"    • {symbol}")
            elif 'exchange_success' in data:
                print(f"  Statistics:")
                print(f"    Success: {data.get('exchange_success', {})}")
                print(f"    Errors: {data.get('exchange_errors', {})}")
                print(f"    RabbitMQ published: {data.get('rabbitmq_published', 0)}")
                print(f"    RabbitMQ failed: {data.get('rabbitmq_failed', 0)}")
    else:
        print(f"✗ Error: {response.get('message')}")
        if response.get("error"):
            print(f"  Error code: {response['error']}")


async def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  control_client.py add_symbol <SYMBOL>")
        print("  control_client.py remove_symbol <SYMBOL>")
        print("  control_client.py set_symbols <SYMBOL1,SYMBOL2,...>")
        print("  control_client.py get_symbols")
        print("  control_client.py get_statistics")
        print()
        print("Examples:")
        print("  control_client.py add_symbol 'SOL/USDT:USDT'")
        print("  control_client.py remove_symbol 'ETH/USDT:USDT'")
        print("  control_client.py set_symbols 'BTC/USDT:USDT,SOL/USDT:USDT'")
        sys.exit(1)
    
    client = ControlClient()
    command = sys.argv[1]
    
    if command == "add_symbol":
        if len(sys.argv) < 3:
            print("Error: symbol required")
            sys.exit(1)
        response = await client.send_command({
            "command": "add_symbol",
            "symbol": sys.argv[2]
        })
        print_response(response)
    
    elif command == "remove_symbol":
        if len(sys.argv) < 3:
            print("Error: symbol required")
            sys.exit(1)
        response = await client.send_command({
            "command": "remove_symbol",
            "symbol": sys.argv[2]
        })
        print_response(response)
    
    elif command == "set_symbols":
        if len(sys.argv) < 3:
            print("Error: symbols required (comma-separated)")
            sys.exit(1)
        symbols = sys.argv[2].split(",")
        response = await client.send_command({
            "command": "set_symbols",
            "symbols": symbols
        })
        print_response(response)
    
    elif command == "get_symbols":
        response = await client.send_command({"command": "get_symbols"})
        print_response(response)
    
    elif command == "get_statistics":
        response = await client.send_command({"command": "get_statistics"})
        print_response(response)
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
