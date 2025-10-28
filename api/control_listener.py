import asyncio
import json
import logging
import time
from typing import Optional
import aio_pika
from aio_pika import Connection, Channel, Queue, Message, Exchange
from utils.shared_state import SharedState
from utils.statistics import Statistics


class ControlListener:
    """Слушает control queue и отправляет ответы в response exchange"""
    
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        control_queue: str,
        response_exchange: str,
        shared_state: SharedState,
        statistics: Statistics
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.control_queue_name = control_queue
        self.response_exchange_name = response_exchange
        self.shared_state = shared_state
        self.statistics = statistics
        self.connection: Optional[Connection] = None
        self.channel: Optional[Channel] = None
        self.queue: Optional[Queue] = None
        self.response_exchange: Optional[Exchange] = None
        self.logger = logging.getLogger("control_listener")
    
    async def connect(self):
        """Подключиться к RabbitMQ"""
        try:
            self.connection = await aio_pika.connect_robust(
                host=self.host,
                port=self.port,
                login=self.user,
                password=self.password
            )
            self.channel = await self.connection.channel()
            
            # Control queue
            self.queue = await self.channel.declare_queue(
                self.control_queue_name,
                durable=True
            )
            
            # Response exchange (topic)
            self.response_exchange = await self.channel.declare_exchange(
                self.response_exchange_name,
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            
            self.logger.info(f"Control listener connected")
            self.logger.info(f"  Control queue: {self.control_queue_name}")
            self.logger.info(f"  Response exchange: {self.response_exchange_name}")
        except Exception as e:
            self.logger.error(f"Failed to connect control listener: {e}")
            raise
    
    async def start(self):
        """Начать прослушивание команд"""
        await self.queue.consume(self._process_message)
        self.logger.info("Control listener started")
    
    async def _process_message(self, message: aio_pika.IncomingMessage):
        """Обработать входящую команду"""
        async with message.process():
            correlation_id = None
            command = None
            
            try:
                command_data = json.loads(message.body.decode())
                correlation_id = command_data.get("correlation_id")
                command = command_data.get("command")
                
                self.logger.info(f"Received command: {command} (id: {correlation_id})")
                
                # Валидация
                if not command:
                    await self._send_error_response(
                        correlation_id,
                        None,
                        "Missing required field: command",
                        "invalid_command"
                    )
                    return
                
                # Выполнение команды
                response = await self._execute_command(command_data)
                
                # Отправка ответа
                await self._send_response(response)
                
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON: {e}")
                await self._send_error_response(
                    correlation_id,
                    command,
                    f"Invalid JSON: {str(e)}",
                    "invalid_json"
                )
            except Exception as e:
                self.logger.error(f"Error processing command: {e}")
                await self._send_error_response(
                    correlation_id,
                    command,
                    f"Internal error: {str(e)}",
                    "internal_error"
                )
    
    async def _execute_command(self, command_data: dict) -> dict:
        """Выполнить команду и вернуть результат"""
        command = command_data.get("command")
        correlation_id = command_data.get("correlation_id")
        
        # add_symbol
        if command == "add_symbol":
            symbol = command_data.get("symbol")
            if not symbol:
                return {
                    "correlation_id": correlation_id,
                    "success": False,
                    "command": command,
                    "message": "Missing required field: symbol",
                    "error": "invalid_command",
                    "timestamp": int(time.time())
                }
            
            success = await self.shared_state.add_symbol(symbol)
            current_symbols = await self.shared_state.get_symbols()
            
            return {
                "correlation_id": correlation_id,
                "success": success,
                "command": command,
                "message": f"Symbol {symbol} {'added successfully' if success else 'already exists'}",
                "error": None if success else "duplicate_symbol",
                "data": {
                    "symbol": symbol,
                    "current_symbols": current_symbols
                },
                "timestamp": int(time.time())
            }
        
        # remove_symbol
        elif command == "remove_symbol":
            symbol = command_data.get("symbol")
            if not symbol:
                return {
                    "correlation_id": correlation_id,
                    "success": False,
                    "command": command,
                    "message": "Missing required field: symbol",
                    "error": "invalid_command",
                    "timestamp": int(time.time())
                }
            
            success = await self.shared_state.remove_symbol(symbol)
            current_symbols = await self.shared_state.get_symbols()
            
            return {
                "correlation_id": correlation_id,
                "success": success,
                "command": command,
                "message": f"Symbol {symbol} {'removed successfully' if success else 'not found'}",
                "error": None if success else "symbol_not_found",
                "data": {
                    "symbol": symbol,
                    "current_symbols": current_symbols
                },
                "timestamp": int(time.time())
            }
        
        # set_symbols
        elif command == "set_symbols":
            symbols = command_data.get("symbols")
            if not symbols or not isinstance(symbols, list):
                return {
                    "correlation_id": correlation_id,
                    "success": False,
                    "command": command,
                    "message": "Missing or invalid field: symbols (must be array)",
                    "error": "invalid_command",
                    "timestamp": int(time.time())
                }
            
            await self.shared_state.set_symbols(symbols)
            current_symbols = await self.shared_state.get_symbols()
            
            return {
                "correlation_id": correlation_id,
                "success": True,
                "command": command,
                "message": f"Symbols updated successfully",
                "error": None,
                "data": {
                    "symbols": current_symbols,
                    "count": len(current_symbols)
                },
                "timestamp": int(time.time())
            }
        
        # get_symbols
        elif command == "get_symbols":
            symbols = await self.shared_state.get_symbols()
            
            return {
                "correlation_id": correlation_id,
                "success": True,
                "command": command,
                "message": "Symbols retrieved successfully",
                "error": None,
                "data": {
                    "symbols": symbols,
                    "count": len(symbols)
                },
                "timestamp": int(time.time())
            }
        
        # get_statistics
        elif command == "get_statistics":
            return {
                "correlation_id": correlation_id,
                "success": True,
                "command": command,
                "message": "Statistics retrieved successfully",
                "error": None,
                "data": {
                    "exchange_success": dict(self.statistics.exchange_success),
                    "exchange_errors": dict(self.statistics.exchange_errors),
                    "rabbitmq_published": self.statistics.rabbitmq_published,
                    "rabbitmq_failed": self.statistics.rabbitmq_failed
                },
                "timestamp": int(time.time())
            }
        
        # unknown command
        else:
            return {
                "correlation_id": correlation_id,
                "success": False,
                "command": command,
                "message": f"Unknown command: {command}",
                "error": "unknown_command",
                "timestamp": int(time.time())
            }
    
    async def _send_response(self, response: dict):
        """Отправить ответ в response exchange"""
        try:
            routing_key = f"control.response.{response['command']}"
            
            await self.response_exchange.publish(
                Message(
                    body=json.dumps(response).encode('utf-8'),
                    content_type='application/json',
                    correlation_id=response.get('correlation_id')
                ),
                routing_key=routing_key
            )
            
            status = "✓" if response['success'] else "✗"
            self.logger.info(f"{status} Response sent: {response['command']} (id: {response['correlation_id']})")
            
        except Exception as e:
            self.logger.error(f"Failed to send response: {e}")
    
    async def _send_error_response(self, correlation_id, command, message, error):
        """Отправить ответ об ошибке"""
        response = {
            "correlation_id": correlation_id,
            "success": False,
            "command": command,
            "message": message,
            "error": error,
            "timestamp": int(time.time())
        }
        await self._send_response(response)
    
    async def close(self):
        """Закрыть соединение"""
        if self.connection:
            await self.connection.close()
            self.logger.info("Control listener closed")
