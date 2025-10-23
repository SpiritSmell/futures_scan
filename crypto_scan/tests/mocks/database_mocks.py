"""
Mock implementations for database clients (ClickHouse, PostgreSQL).
Provides realistic mock behavior for testing database operations.
"""

import asyncio
import json
import time
from typing import Dict, List, Any, Optional, Union
from unittest.mock import AsyncMock, Mock
from dataclasses import dataclass, field


@dataclass
class MockDatabaseRecord:
    """Mock database record structure."""
    table: str
    data: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    id: Optional[str] = None


class MockDatabaseClient:
    """Mock database client for testing."""
    
    def __init__(self, host: str = "localhost", port: int = 8123, 
                 database: str = "crypto_data", user: str = "default", 
                 password: str = "", **kwargs):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection_params = kwargs
        
        # State tracking
        self.is_connected = False
        self.tables = {}
        self.records = []
        self.queries_executed = []
        
        # Statistics
        self.connection_attempts = 0
        self.insert_count = 0
        self.select_count = 0
        self.error_count = 0
        
        # Configuration
        self.connection_delay = 0.05
        self.query_delay = 0.02
        self.failure_rate = 0.0
        
        # Initialize default tables
        self._create_default_tables()
    
    def _create_default_tables(self):
        """Create default crypto data tables."""
        self.tables = {
            'tickers': {
                'columns': [
                    'timestamp', 'exchange', 'symbol', 'price', 'volume', 
                    'change_24h', 'high_24h', 'low_24h', 'bid', 'ask'
                ],
                'records': []
            },
            'funding_rates': {
                'columns': [
                    'timestamp', 'exchange', 'symbol', 'funding_rate', 
                    'next_funding_time', 'predicted_rate'
                ],
                'records': []
            },
            'order_books': {
                'columns': [
                    'timestamp', 'exchange', 'symbol', 'bids', 'asks', 'spread'
                ],
                'records': []
            },
            'system_metrics': {
                'columns': [
                    'timestamp', 'metric_name', 'metric_value', 'tags'
                ],
                'records': []
            }
        }
    
    async def connect(self) -> bool:
        """Mock database connection."""
        self.connection_attempts += 1
        await asyncio.sleep(self.connection_delay)
        
        if self._should_fail():
            self.error_count += 1
            raise ConnectionError(f"Failed to connect to database at {self.host}:{self.port}")
        
        self.is_connected = True
        return True
    
    async def disconnect(self):
        """Mock database disconnection."""
        await asyncio.sleep(0.01)
        self.is_connected = False
    
    async def execute_query(self, query: str, parameters: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Mock query execution."""
        if not self.is_connected:
            raise ConnectionError("Not connected to database")
        
        await asyncio.sleep(self.query_delay)
        
        if self._should_fail():
            self.error_count += 1
            raise Exception(f"Query execution failed: {query[:50]}...")
        
        # Store query for testing verification
        self.queries_executed.append({
            'query': query,
            'parameters': parameters,
            'timestamp': time.time()
        })
        
        # Parse and execute mock query
        query_lower = query.lower().strip()
        
        if query_lower.startswith('select'):
            self.select_count += 1
            return await self._execute_select(query, parameters)
        elif query_lower.startswith('insert'):
            self.insert_count += 1
            return await self._execute_insert(query, parameters)
        elif query_lower.startswith('create table'):
            return await self._execute_create_table(query)
        else:
            return []
    
    async def insert_batch(self, table: str, records: List[Dict[str, Any]]) -> int:
        """Mock batch insert operation."""
        if not self.is_connected:
            raise ConnectionError("Not connected to database")
        
        await asyncio.sleep(self.query_delay * len(records) / 100)  # Simulate batch efficiency
        
        if self._should_fail():
            self.error_count += 1
            raise Exception(f"Batch insert failed for table {table}")
        
        if table not in self.tables:
            raise ValueError(f"Table {table} does not exist")
        
        # Add records to mock table
        for record in records:
            mock_record = MockDatabaseRecord(
                table=table,
                data=record,
                id=f"{table}_{len(self.tables[table]['records'])}"
            )
            self.tables[table]['records'].append(mock_record)
            self.records.append(mock_record)
        
        self.insert_count += len(records)
        return len(records)
    
    async def select_data(self, table: str, conditions: Optional[Dict] = None, 
                         limit: Optional[int] = None, 
                         order_by: Optional[str] = None) -> List[Dict[str, Any]]:
        """Mock data selection."""
        if not self.is_connected:
            raise ConnectionError("Not connected to database")
        
        await asyncio.sleep(self.query_delay)
        
        if table not in self.tables:
            raise ValueError(f"Table {table} does not exist")
        
        records = self.tables[table]['records']
        results = []
        
        for record in records:
            # Apply conditions filter
            if conditions:
                match = True
                for key, value in conditions.items():
                    if key not in record.data or record.data[key] != value:
                        match = False
                        break
                if not match:
                    continue
            
            results.append(record.data)
        
        # Apply ordering
        if order_by:
            reverse = order_by.startswith('-')
            field = order_by.lstrip('-')
            if field in results[0] if results else {}:
                results.sort(key=lambda x: x.get(field, 0), reverse=reverse)
        
        # Apply limit
        if limit:
            results = results[:limit]
        
        self.select_count += 1
        return results
    
    async def get_table_info(self, table: str) -> Dict[str, Any]:
        """Mock table information retrieval."""
        if table not in self.tables:
            raise ValueError(f"Table {table} does not exist")
        
        table_info = self.tables[table]
        return {
            'name': table,
            'columns': table_info['columns'],
            'record_count': len(table_info['records']),
            'size_bytes': len(table_info['records']) * 1024  # Mock size calculation
        }
    
    async def create_table(self, table: str, columns: List[str], **kwargs) -> bool:
        """Mock table creation."""
        if not self.is_connected:
            raise ConnectionError("Not connected to database")
        
        await asyncio.sleep(self.query_delay)
        
        self.tables[table] = {
            'columns': columns,
            'records': [],
            'created_at': time.time(),
            'options': kwargs
        }
        
        return True
    
    async def drop_table(self, table: str) -> bool:
        """Mock table deletion."""
        if table in self.tables:
            del self.tables[table]
            return True
        return False
    
    async def truncate_table(self, table: str) -> int:
        """Mock table truncation."""
        if table not in self.tables:
            raise ValueError(f"Table {table} does not exist")
        
        record_count = len(self.tables[table]['records'])
        self.tables[table]['records'].clear()
        return record_count
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get database client statistics."""
        return {
            'connection_attempts': self.connection_attempts,
            'insert_count': self.insert_count,
            'select_count': self.select_count,
            'error_count': self.error_count,
            'is_connected': self.is_connected,
            'tables_count': len(self.tables),
            'total_records': len(self.records),
            'queries_executed': len(self.queries_executed),
            'failure_rate': self.failure_rate
        }
    
    def get_executed_queries(self, query_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get executed queries for testing verification."""
        queries = self.queries_executed
        
        if query_type:
            queries = [q for q in queries if q['query'].lower().startswith(query_type.lower())]
        
        return queries
    
    def clear_query_history(self):
        """Clear query execution history."""
        self.queries_executed.clear()
    
    def set_failure_rate(self, rate: float):
        """Set failure rate for testing error scenarios."""
        self.failure_rate = max(0.0, min(1.0, rate))
    
    def set_delays(self, connection_delay: float = 0.05, query_delay: float = 0.02):
        """Set operation delays for testing."""
        self.connection_delay = connection_delay
        self.query_delay = query_delay
    
    async def _execute_select(self, query: str, parameters: Optional[Dict]) -> List[Dict[str, Any]]:
        """Execute mock SELECT query."""
        # Simple query parsing for testing
        if 'from' in query.lower():
            parts = query.lower().split('from')
            if len(parts) > 1:
                table_part = parts[1].strip().split()[0]
                if table_part in self.tables:
                    return [r.data for r in self.tables[table_part]['records'][:10]]  # Limit for testing
        return []
    
    async def _execute_insert(self, query: str, parameters: Optional[Dict]) -> List[Dict[str, Any]]:
        """Execute mock INSERT query."""
        # Mock insert - would need real parsing for production
        return []
    
    async def _execute_create_table(self, query: str) -> List[Dict[str, Any]]:
        """Execute mock CREATE TABLE query."""
        # Mock table creation - would need real parsing for production
        return []
    
    def _should_fail(self) -> bool:
        """Determine if operation should fail based on failure rate."""
        import random
        return random.random() < self.failure_rate


class MockClickHouseClient(MockDatabaseClient):
    """Mock ClickHouse client with specific ClickHouse features."""
    
    def __init__(self, **kwargs):
        kwargs.setdefault('port', 8123)
        super().__init__(**kwargs)
        
        # ClickHouse specific features
        self.compression_enabled = kwargs.get('compression', True)
        self.batch_size = kwargs.get('batch_size', 10000)
    
    async def insert_batch_compressed(self, table: str, records: List[Dict[str, Any]]) -> int:
        """Mock compressed batch insert (ClickHouse feature)."""
        # Simulate compression benefits
        compression_ratio = 0.3 if self.compression_enabled else 1.0
        adjusted_delay = self.query_delay * len(records) / 100 * compression_ratio
        
        await asyncio.sleep(adjusted_delay)
        return await self.insert_batch(table, records)
    
    async def optimize_table(self, table: str) -> bool:
        """Mock table optimization (ClickHouse feature)."""
        if table not in self.tables:
            raise ValueError(f"Table {table} does not exist")
        
        await asyncio.sleep(0.1)  # Simulate optimization time
        return True


class MockPostgreSQLClient(MockDatabaseClient):
    """Mock PostgreSQL client with specific PostgreSQL features."""
    
    def __init__(self, **kwargs):
        kwargs.setdefault('port', 5432)
        super().__init__(**kwargs)
        
        # PostgreSQL specific features
        self.transaction_active = False
        self.transaction_queries = []
    
    async def begin_transaction(self):
        """Begin database transaction."""
        if self.transaction_active:
            raise Exception("Transaction already active")
        
        self.transaction_active = True
        self.transaction_queries = []
    
    async def commit_transaction(self):
        """Commit database transaction."""
        if not self.transaction_active:
            raise Exception("No active transaction")
        
        # Execute all transaction queries
        for query_info in self.transaction_queries:
            await self.execute_query(query_info['query'], query_info['parameters'])
        
        self.transaction_active = False
        self.transaction_queries = []
    
    async def rollback_transaction(self):
        """Rollback database transaction."""
        if not self.transaction_active:
            raise Exception("No active transaction")
        
        self.transaction_active = False
        self.transaction_queries = []
    
    async def execute_query(self, query: str, parameters: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Execute query with transaction support."""
        if self.transaction_active:
            # Store query for later execution
            self.transaction_queries.append({
                'query': query,
                'parameters': parameters
            })
            return []
        
        return await super().execute_query(query, parameters)


# Utility functions for testing
def create_mock_ticker_record(exchange: str, symbol: str) -> Dict[str, Any]:
    """Create a realistic ticker record for testing."""
    import random
    
    return {
        'timestamp': int(time.time() * 1000),
        'exchange': exchange,
        'symbol': symbol,
        'price': random.uniform(0.1, 50000),
        'volume': random.uniform(1000, 100000),
        'change_24h': random.uniform(-10, 10),
        'high_24h': random.uniform(100, 60000),
        'low_24h': random.uniform(50, 40000),
        'bid': random.uniform(100, 50000),
        'ask': random.uniform(100, 50000)
    }


def create_mock_funding_rate_record(exchange: str, symbol: str) -> Dict[str, Any]:
    """Create a realistic funding rate record for testing."""
    import random
    
    return {
        'timestamp': int(time.time() * 1000),
        'exchange': exchange,
        'symbol': symbol,
        'funding_rate': random.uniform(-0.001, 0.001),
        'next_funding_time': int(time.time() * 1000) + 8 * 3600 * 1000,
        'predicted_rate': random.uniform(-0.001, 0.001)
    }


def create_batch_ticker_records(exchanges: List[str], symbols: List[str], 
                               count: int = 100) -> List[Dict[str, Any]]:
    """Create a batch of ticker records for testing."""
    import random
    
    records = []
    for _ in range(count):
        exchange = random.choice(exchanges)
        symbol = random.choice(symbols)
        record = create_mock_ticker_record(exchange, symbol)
        records.append(record)
    
    return records


def create_batch_funding_rate_records(exchanges: List[str], symbols: List[str], 
                                    count: int = 50) -> List[Dict[str, Any]]:
    """Create a batch of funding rate records for testing."""
    import random
    
    records = []
    for _ in range(count):
        exchange = random.choice(exchanges)
        symbol = random.choice(symbols)
        record = create_mock_funding_rate_record(exchange, symbol)
        records.append(record)
    
    return records
