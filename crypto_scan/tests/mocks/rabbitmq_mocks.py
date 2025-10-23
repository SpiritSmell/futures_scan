"""
Mock implementations for RabbitMQ clients.
Provides realistic mock behavior for testing message queue operations.
"""

import asyncio
import json
import time
from typing import Dict, List, Any, Optional, Callable
from unittest.mock import AsyncMock, Mock
from dataclasses import dataclass, field


@dataclass
class MockMessage:
    """Mock message structure for RabbitMQ."""
    body: bytes
    routing_key: str
    exchange: str
    timestamp: float = field(default_factory=time.time)
    headers: Dict[str, Any] = field(default_factory=dict)
    properties: Dict[str, Any] = field(default_factory=dict)
    
    def json(self) -> Dict[str, Any]:
        """Decode message body as JSON."""
        return json.loads(self.body.decode('utf-8'))


class MockAsyncRabbitMQClient:
    """Mock async RabbitMQ client for testing."""
    
    def __init__(self, host: str = "localhost", port: int = 5672, 
                 user: str = "guest", password: str = "guest", **kwargs):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connection_params = kwargs
        
        # State tracking
        self.is_connected = False
        self.is_started = False
        self.channels = {}
        self.exchanges = {}
        self.queues = {}
        self.bindings = {}
        self.published_messages = []
        self.consumed_messages = []
        
        # Statistics
        self.connection_attempts = 0
        self.publish_count = 0
        self.consume_count = 0
        self.error_count = 0
        
        # Configuration
        self.connection_delay = 0.1  # Simulate connection time
        self.publish_delay = 0.01    # Simulate publish time
        self.failure_rate = 0.0      # Configurable failure rate
        
        # Callbacks
        self.message_handlers = {}
    
    async def connect(self) -> bool:
        """Mock connection to RabbitMQ."""
        self.connection_attempts += 1
        await asyncio.sleep(self.connection_delay)
        
        # Simulate connection failures
        if self._should_fail():
            self.error_count += 1
            raise ConnectionError(f"Failed to connect to RabbitMQ at {self.host}:{self.port}")
        
        self.is_connected = True
        return True
    
    async def disconnect(self):
        """Mock disconnection from RabbitMQ."""
        await asyncio.sleep(0.01)
        self.is_connected = False
        self.is_started = False
        self.channels.clear()
    
    async def start(self):
        """Start the RabbitMQ client."""
        if not self.is_connected:
            await self.connect()
        
        self.is_started = True
    
    async def stop(self):
        """Stop the RabbitMQ client."""
        self.is_started = False
        await self.disconnect()
    
    async def declare_exchange(self, exchange_name: str, exchange_type: str = "direct", 
                             durable: bool = True, **kwargs):
        """Mock exchange declaration."""
        if not self.is_connected:
            raise ConnectionError("Not connected to RabbitMQ")
        
        self.exchanges[exchange_name] = {
            'name': exchange_name,
            'type': exchange_type,
            'durable': durable,
            'arguments': kwargs
        }
    
    async def declare_queue(self, queue_name: str, durable: bool = True, **kwargs):
        """Mock queue declaration."""
        if not self.is_connected:
            raise ConnectionError("Not connected to RabbitMQ")
        
        self.queues[queue_name] = {
            'name': queue_name,
            'durable': durable,
            'arguments': kwargs,
            'messages': []
        }
    
    async def bind_queue(self, queue_name: str, exchange_name: str, routing_key: str = ""):
        """Mock queue binding."""
        if not self.is_connected:
            raise ConnectionError("Not connected to RabbitMQ")
        
        binding_key = f"{exchange_name}:{queue_name}:{routing_key}"
        self.bindings[binding_key] = {
            'queue': queue_name,
            'exchange': exchange_name,
            'routing_key': routing_key
        }
    
    async def publish_message(self, exchange_name: str, routing_key: str, 
                            message: Dict[str, Any], **kwargs) -> bool:
        """Mock message publishing."""
        if not self.is_connected:
            raise ConnectionError("Not connected to RabbitMQ")
        
        await asyncio.sleep(self.publish_delay)
        
        # Simulate publish failures
        if self._should_fail():
            self.error_count += 1
            raise Exception(f"Failed to publish message to {exchange_name}")
        
        # Create mock message
        mock_message = MockMessage(
            body=json.dumps(message).encode('utf-8'),
            routing_key=routing_key,
            exchange=exchange_name,
            headers=kwargs.get('headers', {}),
            properties=kwargs.get('properties', {})
        )
        
        # Store published message
        self.published_messages.append(mock_message)
        self.publish_count += 1
        
        # Route message to bound queues
        await self._route_message(mock_message)
        
        return True
    
    async def publish_batch(self, messages: List[Dict[str, Any]], 
                          exchange_name: str, routing_key: str = "") -> int:
        """Mock batch message publishing."""
        if not self.is_connected:
            raise ConnectionError("Not connected to RabbitMQ")
        
        successful_publishes = 0
        
        for message in messages:
            try:
                await self.publish_message(exchange_name, routing_key, message)
                successful_publishes += 1
            except Exception:
                # Continue with other messages even if one fails
                continue
        
        return successful_publishes
    
    async def consume_messages(self, queue_name: str, callback: Callable, 
                             auto_ack: bool = True, **kwargs):
        """Mock message consumption."""
        if not self.is_connected:
            raise ConnectionError("Not connected to RabbitMQ")
        
        if queue_name not in self.queues:
            raise ValueError(f"Queue {queue_name} not declared")
        
        self.message_handlers[queue_name] = callback
        
        # Process existing messages in queue
        queue_messages = self.queues[queue_name]['messages']
        for message in queue_messages.copy():
            try:
                await callback(message)
                if auto_ack:
                    queue_messages.remove(message)
                self.consume_count += 1
            except Exception as e:
                self.error_count += 1
                # In real RabbitMQ, message would be requeued or sent to DLQ
    
    async def get_queue_info(self, queue_name: str) -> Dict[str, Any]:
        """Mock queue information retrieval."""
        if queue_name not in self.queues:
            raise ValueError(f"Queue {queue_name} not found")
        
        queue = self.queues[queue_name]
        return {
            'name': queue_name,
            'messages': len(queue['messages']),
            'consumers': len([h for h in self.message_handlers if h == queue_name]),
            'durable': queue['durable']
        }
    
    async def purge_queue(self, queue_name: str) -> int:
        """Mock queue purging."""
        if queue_name not in self.queues:
            raise ValueError(f"Queue {queue_name} not found")
        
        message_count = len(self.queues[queue_name]['messages'])
        self.queues[queue_name]['messages'].clear()
        return message_count
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get client statistics for testing."""
        return {
            'connection_attempts': self.connection_attempts,
            'publish_count': self.publish_count,
            'consume_count': self.consume_count,
            'error_count': self.error_count,
            'is_connected': self.is_connected,
            'is_started': self.is_started,
            'exchanges_count': len(self.exchanges),
            'queues_count': len(self.queues),
            'bindings_count': len(self.bindings),
            'published_messages_count': len(self.published_messages),
            'failure_rate': self.failure_rate
        }
    
    def get_published_messages(self, exchange_name: Optional[str] = None, 
                             routing_key: Optional[str] = None) -> List[MockMessage]:
        """Get published messages for testing verification."""
        messages = self.published_messages
        
        if exchange_name:
            messages = [m for m in messages if m.exchange == exchange_name]
        
        if routing_key:
            messages = [m for m in messages if m.routing_key == routing_key]
        
        return messages
    
    def clear_published_messages(self):
        """Clear published messages history."""
        self.published_messages.clear()
    
    def set_failure_rate(self, rate: float):
        """Set failure rate for testing error scenarios."""
        self.failure_rate = max(0.0, min(1.0, rate))
    
    def set_delays(self, connection_delay: float = 0.1, publish_delay: float = 0.01):
        """Set operation delays for testing."""
        self.connection_delay = connection_delay
        self.publish_delay = publish_delay
    
    async def _route_message(self, message: MockMessage):
        """Route message to bound queues."""
        for binding_key, binding in self.bindings.items():
            if (binding['exchange'] == message.exchange and 
                (binding['routing_key'] == message.routing_key or binding['routing_key'] == "")):
                
                queue_name = binding['queue']
                if queue_name in self.queues:
                    self.queues[queue_name]['messages'].append(message)
    
    def _should_fail(self) -> bool:
        """Determine if operation should fail based on failure rate."""
        import random
        return random.random() < self.failure_rate


class MockRabbitMQClient(MockAsyncRabbitMQClient):
    """Synchronous mock RabbitMQ client."""
    
    def connect(self) -> bool:
        """Synchronous connect."""
        import asyncio
        return asyncio.run(super().connect())
    
    def disconnect(self):
        """Synchronous disconnect."""
        import asyncio
        asyncio.run(super().disconnect())
    
    def start(self):
        """Synchronous start."""
        import asyncio
        asyncio.run(super().start())
    
    def stop(self):
        """Synchronous stop."""
        import asyncio
        asyncio.run(super().stop())
    
    def publish_message(self, exchange_name: str, routing_key: str, 
                       message: Dict[str, Any], **kwargs) -> bool:
        """Synchronous publish."""
        import asyncio
        return asyncio.run(super().publish_message(exchange_name, routing_key, message, **kwargs))
    
    def publish_batch(self, messages: List[Dict[str, Any]], 
                     exchange_name: str, routing_key: str = "") -> int:
        """Synchronous batch publish."""
        import asyncio
        return asyncio.run(super().publish_batch(messages, exchange_name, routing_key))


class MockRabbitMQConnectionPool:
    """Mock connection pool for RabbitMQ clients."""
    
    def __init__(self, max_connections: int = 10, **connection_params):
        self.max_connections = max_connections
        self.connection_params = connection_params
        self.pool = []
        self.active_connections = 0
        self.total_created = 0
        self.total_borrowed = 0
        self.total_returned = 0
    
    async def get_connection(self) -> MockAsyncRabbitMQClient:
        """Get connection from pool."""
        if self.pool:
            connection = self.pool.pop()
            self.total_borrowed += 1
            return connection
        
        if self.active_connections < self.max_connections:
            connection = MockAsyncRabbitMQClient(**self.connection_params)
            await connection.connect()
            self.active_connections += 1
            self.total_created += 1
            self.total_borrowed += 1
            return connection
        
        raise Exception("Connection pool exhausted")
    
    async def return_connection(self, connection: MockAsyncRabbitMQClient):
        """Return connection to pool."""
        if connection.is_connected and len(self.pool) < self.max_connections:
            self.pool.append(connection)
            self.total_returned += 1
        else:
            await connection.disconnect()
            self.active_connections -= 1
    
    async def close_all(self):
        """Close all connections in pool."""
        for connection in self.pool:
            await connection.disconnect()
        self.pool.clear()
        self.active_connections = 0
    
    def get_pool_statistics(self) -> Dict[str, Any]:
        """Get pool statistics."""
        return {
            'max_connections': self.max_connections,
            'active_connections': self.active_connections,
            'available_connections': len(self.pool),
            'total_created': self.total_created,
            'total_borrowed': self.total_borrowed,
            'total_returned': self.total_returned,
            'utilization': self.active_connections / self.max_connections
        }


# Utility functions for testing
def create_mock_crypto_message(exchange: str, symbol: str, data_type: str = "ticker") -> Dict[str, Any]:
    """Create a realistic crypto data message for testing."""
    import random
    
    base_message = {
        'timestamp': int(time.time() * 1000),
        'exchange': exchange,
        'symbol': symbol,
        'data_type': data_type
    }
    
    if data_type == "ticker":
        base_message.update({
            'price': random.uniform(0.1, 50000),
            'volume': random.uniform(1000, 100000),
            'change_24h': random.uniform(-10, 10),
            'high_24h': random.uniform(100, 60000),
            'low_24h': random.uniform(50, 40000)
        })
    elif data_type == "funding_rate":
        base_message.update({
            'funding_rate': random.uniform(-0.001, 0.001),
            'next_funding_time': int(time.time() * 1000) + 8 * 3600 * 1000,
            'predicted_rate': random.uniform(-0.001, 0.001)
        })
    elif data_type == "order_book":
        base_message.update({
            'bids': [[random.uniform(100, 200), random.uniform(1, 10)] for _ in range(10)],
            'asks': [[random.uniform(200, 300), random.uniform(1, 10)] for _ in range(10)],
            'spread': random.uniform(0.01, 1.0)
        })
    
    return base_message


def create_batch_crypto_messages(exchange: str, symbols: List[str], 
                               data_type: str = "ticker", count: int = 100) -> List[Dict[str, Any]]:
    """Create a batch of crypto messages for testing."""
    import random
    
    messages = []
    for _ in range(count):
        symbol = random.choice(symbols)
        message = create_mock_crypto_message(exchange, symbol, data_type)
        messages.append(message)
    
    return messages
