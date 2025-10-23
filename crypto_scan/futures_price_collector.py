import asyncio
import json
import hashlib
import logging
import os
import signal
import ssl
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field

import ccxt.pro as ccxt_async
import ccxt as ccxt_sync
from ccxt.base.exchange import Exchange as CCXTExchange

from packages import processor_template
from packages.app_template import AppTemplate

# Get the logger for the current module
logger = logging.getLogger(__name__)
from packages.clickhouse import ClickhouseConnector
from packages.json_utils import save_data_to_json
# Define the path to the system's CA bundle
SYSTEM_CA_BUNDLE = '/etc/ssl/certs/ca-certificates.crt'

#["bitget",  "bybit", "binance", "htx","gateio"],


# Constants
DEFAULT_POLL_DELAY = 10  # seconds
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30  # seconds

# Type aliases
ExchangeName = str
Symbol = str
ExchangeData = Dict[str, Any]
TickerData = Dict[ExchangeName, Dict[Symbol, Any]]
FundingRates = Dict[ExchangeName, Dict[Symbol, Dict[str, Any]]]

@dataclass
class ExchangeConfig:
    """Configuration for a cryptocurrency exchange."""
    name: str
    api_key: str = ""
    secret: str = ""
    options: Dict[str, Any] = field(default_factory=dict)
    enable_rate_limit: bool = True
    verify_ssl: bool = False

def calculate_hash(data: Dict[str, Any]) -> str:
    """Вычисляет хэш данных тикеров."""
    tickers_data = [item for item in data.items()]
    return hashlib.md5(json.dumps(tickers_data, sort_keys=True).encode()).hexdigest()


class CryptoDataStreamer:
    def __init__(self, symbol_exchange_map: Dict[str, List[str]] | None = None) -> None:
        """Initialize the CryptoDataStreamer with optional symbol to exchange mapping.
        
        Args:
            symbol_exchange_map: Optional mapping of exchange names to their supported symbols
        """
        self.api_keys: Dict[str, Dict[str, str]] = {}
        self.tasks: List[asyncio.Task] = []
        self.symbol_exchange_map: Dict[str, List[str]] = symbol_exchange_map or {}
        self.async_exchange_instances: Dict[str, CCXTExchange] = {}
        self.sync_exchange_instances: Dict[str, CCXTExchange] = {}
        self.exchanges_names_list: List[str] = []
        self.ticker_data: TickerData = {}
        self.futures_data: FundingRates = {}
        self.latest_timestamps: Dict[str, int] = {}
        self.dd: Optional[processor_template.DataDispatcher] = None
        self.polls_delay: int = DEFAULT_POLL_DELAY
        self._shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        """Close all exchange connections and clean up resources.
        
        This method ensures all connections are properly closed and tasks are cancelled.
        It's safe to call this method multiple times.
        """
        if not hasattr(self, '_shutdown_event') or self._shutdown_event.is_set():
            return
            
        self._shutdown_event.set()
        
        # Cancel all running tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.warning(f"Error cancelling task: {e}")
        
        # Close all exchange connections
        close_tasks = []
        for exchange_id, exchange in list(self.async_exchange_instances.items()):
            try:
                close_task = asyncio.create_task(exchange.close())
                close_tasks.append(close_task)
            except Exception as e:
                logger.error(f"Error scheduling close for {exchange_id}: {e}")
        
        # Wait for all close operations to complete with timeout
        if close_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*close_tasks, return_exceptions=True),
                    timeout=5.0
                )
                logger.info("All exchanges closed successfully")
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for some exchanges to close")
            except Exception as e:
                logger.error(f"Error during exchange close: {e}")
        
        self.async_exchange_instances.clear()
        self.sync_exchange_instances.clear()

    def __del__(self) -> None:
        """Ensure resources are cleaned up when the object is garbage collected."""
        if hasattr(self, '_shutdown_event') and not self._shutdown_event.is_set():
            try:
                if self.async_exchange_instances:
                    # Try to close synchronously if we're in an event loop
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            # If loop is running, schedule async close
                            asyncio.create_task(self.close())
                        else:
                            # Otherwise run until complete
                            loop.run_until_complete(self.close())
                    except RuntimeError:
                        # No event loop, create a new one
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            loop.run_until_complete(self.close())
                        finally:
                            loop.close()
            except Exception as e:
                logger.error(f"Error during cleanup in __del__: {e}", exc_info=True)

    async def initialize_exchanges(self, exchanges_names_list: Optional[List[str]] = None) -> None:
        """Initialize multiple exchanges in parallel.
        
        Args:
            exchanges_names_list: List of exchange names to initialize. If None, uses the list from settings.
        """
        if not exchanges_names_list:
            exchanges_names_list = self.exchanges_names_list
            
        if not exchanges_names_list:
            logger.warning("No exchanges specified for initialization")
            return
            
        self.exchanges_names_list = list(set(exchanges_names_list))  # Remove duplicates
        
        # Initialize exchanges with retries
        tasks = []
        for exchange_id in self.exchanges_names_list:
            task = asyncio.create_task(self._initialize_exchange_with_retry(exchange_id, 'swap'))
            tasks.append(task)
            
        # Wait for all initializations to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Log results
        for exchange_id, result in zip(self.exchanges_names_list, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to initialize {exchange_id}: {result}")
            elif result:
                logger.info(f"Successfully initialized {exchange_id}")

    async def _initialize_exchange_with_retry(self, exchange_id: str, market_type: str, max_retries: int = 3) -> bool:
        """Initialize an exchange with retry logic.
        
        Args:
            exchange_id: The exchange ID (e.g., 'binance', 'bybit')
            market_type: Market type ('spot', 'swap', 'future', etc.)
            max_retries: Maximum number of retry attempts
            
        Returns:
            bool: True if initialization was successful, False otherwise
        """
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            try:
                await self.initialize_exchange(exchange_id, market_type)
                return True
            except Exception as e:
                last_error = e
                wait_time = min(2 ** attempt, 10)  # Exponential backoff, max 10 seconds
                logger.warning(
                    f"Attempt {attempt}/{max_retries} failed for {exchange_id}. "
                    f"Retrying in {wait_time} seconds... Error: {e}"
                )
                await asyncio.sleep(wait_time)
        
        logger.error(f"Failed to initialize {exchange_id} after {max_retries} attempts: {last_error}")
        return False

    async def initialize_exchange(self, exchange_id: str, market_type: str = 'spot') -> None:
        """Initialize a single exchange with the specified market type.
        
        Args:
            exchange_id: The exchange ID (e.g., 'binance', 'bybit')
            market_type: Market type ('spot', 'swap', 'future', etc.)
            
        Raises:
            ValueError: If the exchange doesn't support required methods
            ConnectionError: If there's a connection issue
            Exception: For any other errors during initialization
        """
        exchange_id = exchange_id.lower()
        
        if exchange_id in self.async_exchange_instances:
            logger.debug(f"Exchange {exchange_id} is already initialized")
            return
            
        logger.info(f"Initializing {exchange_id} ({market_type})...")
        
        try:
            # Get API keys if available
            api_key = self.api_keys.get(exchange_id, {}).get('apiKey', '')
            secret = self.api_keys.get(exchange_id, {}).get('secret', '')
            
            # Common exchange configuration
            exchange_config = {
                'enableRateLimit': True,
                'rateLimit': 1000,  # Default rate limit in ms
                'timeout': REQUEST_TIMEOUT * 1000,  # Convert to ms
                'options': {
                    'defaultType': market_type,
                    'adjustForTimeDifference': True,
                    'recvWindow': 10 * 1000,  # 10 seconds
                },
                'apiKey': api_key,
                'secret': secret,
                'verbose': False,  # Disable verbose logging
                'verify': False if exchange_id == 'htx' else (SYSTEM_CA_BUNDLE if os.path.exists(SYSTEM_CA_BUNDLE) else None),
            }
            
            # Initialize async exchange instance
            exchange_async: CCXTExchange = getattr(ccxt_async, exchange_id)(exchange_config)
            
            # Initialize sync exchange instance for operations that don't support async
            exchange_sync = getattr(ccxt_sync, exchange_id)(exchange_config)
            
            # Verify required methods are supported
            required_methods = ['fetchTicker', 'fetchMarkets', 'fetchFundingRates']
            missing_methods = [m for m in required_methods if not exchange_async.has.get(m, False)]
            
            if missing_methods:
                raise ValueError(
                    f"Exchange {exchange_id} is missing required methods: {', '.join(missing_methods)}"
                )
            
            # Load markets to verify connection and get available symbols
            try:
                # Use sync version to avoid potential async issues during initialization
                all_markets = exchange_sync.load_markets()
                
                # Filter for swap markets and store symbols
                swap_markets = [
                    market['symbol'] 
                    for market in all_markets.values() 
                    if market.get('swap', False) and market.get('active', True)
                ]
                
                if not swap_markets:
                    logger.warning(f"No active swap markets found for {exchange_id}")
                
                self.symbol_exchange_map[exchange_id] = swap_markets
                
                # Store exchange instances
                self.async_exchange_instances[exchange_id] = exchange_async
                self.sync_exchange_instances[exchange_id] = exchange_sync
                
                logger.info(
                    f"Successfully initialized {exchange_id} with {len(swap_markets)} "
                    f"active {market_type} markets"
                )
                
            except Exception as e:
                await exchange_async.close()
                raise ConnectionError(f"Failed to load markets for {exchange_id}: {e}")
                
        except ImportError:
            raise ImportError(f"Exchange {exchange_id} is not supported by CCXT")
        except Exception as e:
            logger.error(f"Error initializing {exchange_id}: {e}", exc_info=True)
            raise

    async def fetch_tickers(self, exchange_id: str) -> Dict[str, Any]:
        """Fetch ticker data from a specific exchange.
        
        Args:
            exchange_id: The exchange ID to fetch tickers from
            
        Returns:
            Dict containing ticker data or error information
        """
        exchange = self.async_exchange_instances.get(exchange_id)
        if not exchange:
            logger.error(f"Exchange {exchange_id} not initialized")
            return {}
            
        try:
            # Use rate limiting to avoid being banned
            if hasattr(exchange, 'rate_limit'):
                await asyncio.sleep(exchange.rate_limit / 1000)  # Convert ms to seconds
                
            async with self._lock:
                tickers = await exchange.fetch_tickers()
                self.ticker_data[exchange_id] = tickers
                
            logger.debug(f"Fetched {len(tickers)} tickers from {exchange_id}")
            return tickers
            
        except asyncio.CancelledError:
            logger.debug(f"Ticker fetch for {exchange_id} was cancelled")
            raise
        except Exception as e:
            logger.error(f"Error fetching tickers from {exchange_id}: {e}", exc_info=True)
            # Return empty dict to avoid breaking the data structure
            return {}

    async def fetch_tickers_from_exchanges(self) -> None:
        """Continuously fetch ticker data from all initialized exchanges."""
        logger.info("Starting ticker data collection")
        
        while not self._shutdown_event.is_set():
            start_time = asyncio.get_event_loop().time()
            
            # Process exchanges in batches to avoid rate limiting
            for exchange_id in list(self.async_exchange_instances.keys()):
                if self._shutdown_event.is_set():
                    break
                    
                try:
                    await self.fetch_tickers(exchange_id)
                except Exception as e:
                    logger.error(f"Error in ticker fetch for {exchange_id}: {e}")
            
            # Calculate sleep time to maintain polling interval
            elapsed = asyncio.get_event_loop().time() - start_time
            sleep_time = max(0, self.polls_delay - elapsed)
            
            if sleep_time > 0 and not self._shutdown_event.is_set():
                await asyncio.sleep(sleep_time)
                
        logger.info("Ticker data collection stopped")

    async def fetch_funding_rates(self, exchange_id: str) -> Dict[str, Any]:
        """Fetch funding rates from a specific exchange.
        
        Args:
            exchange_id: The exchange ID to fetch funding rates from
            
        Returns:
            Dict containing funding rates or error information
        """
        exchange = self.async_exchange_instances.get(exchange_id)
        if not exchange:
            logger.error(f"Exchange {exchange_id} not initialized")
            return {}
            
        symbols = self.symbol_exchange_map.get(exchange_id, [])
        if not symbols:
            logger.warning(f"No symbols configured for {exchange_id}")
            return {}
            
        try:
            # Use rate limiting to avoid being banned
            if hasattr(exchange, 'rate_limit'):
                await asyncio.sleep(exchange.rate_limit / 1000)  # Convert ms to seconds
                
            funding_rates = {}
            
            # Process symbols in chunks to avoid rate limits
            chunk_size = 10  # Adjust based on exchange rate limits
            for i in range(0, len(symbols), chunk_size):
                chunk = symbols[i:i + chunk_size]
                try:
                    rates = await exchange.fetch_funding_rates(chunk)
                    funding_rates.update(rates)
                except Exception as e:
                    logger.error(f"Error fetching funding rates for chunk {i//chunk_size + 1}: {e}")
            
            async with self._lock:
                self.futures_data[exchange_id] = funding_rates
                
            logger.debug(f"Fetched funding rates for {len(funding_rates)} symbols from {exchange_id}")
            return funding_rates
            
        except asyncio.CancelledError:
            logger.debug(f"Funding rate fetch for {exchange_id} was cancelled")
            raise
        except Exception as e:
            logger.error(f"Error fetching funding rates from {exchange_id}: {e}", exc_info=True)
            return {}

    async def fetch_funding_rates_from_exchanges(self) -> None:
        """Continuously fetch funding rates from all initialized exchanges."""
        logger.info("Starting funding rate collection")
        
        while not self._shutdown_event.is_set():
            start_time = asyncio.get_event_loop().time()
            
            # Process exchanges in batches to avoid rate limiting
            for exchange_id in list(self.async_exchange_instances.keys()):
                if self._shutdown_event.is_set():
                    break
                    
                try:
                    await self.fetch_funding_rates(exchange_id)
                except Exception as e:
                    logger.error(f"Error in funding rate fetch for {exchange_id}: {e}")
            
            # Calculate sleep time to maintain polling interval
            elapsed = asyncio.get_event_loop().time() - start_time
            sleep_time = max(0, self.polls_delay * 2 - elapsed)  # Funding rates update less frequently
            
            if sleep_time > 0 and not self._shutdown_event.is_set():
                await asyncio.sleep(sleep_time)
                
        logger.info("Funding rate collection stopped")



    async def process_exchanges(self) -> None:
        """Start all data collection and processing tasks.
        
        This method initializes and runs all the necessary coroutines in parallel,
        handling their lifecycle and ensuring proper cleanup.
        """
        if not self.async_exchange_instances:
            logger.warning("No exchanges initialized. Call initialize_exchanges() first.")
            return
            
        logger.info(f"Starting data processing for {len(self.async_exchange_instances)} exchanges")
        
        # Create tasks for each data collection process
        tasks = [
            asyncio.create_task(self.fetch_tickers_from_exchanges()),
            asyncio.create_task(self.fetch_funding_rates_from_exchanges()),
            asyncio.create_task(self.send_data())
        ]
        
        # Store tasks for proper cleanup
        self.tasks.extend(tasks)
        
        try:
            # Wait for all tasks to complete (they won't unless there's an error)
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Data processing was cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in data processing: {e}", exc_info=True)
            raise

    def transpose_data(self, data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Transpose data from exchange-first to symbol-first format.
        
        Args:
            data: Dictionary with exchange names as keys and their data as values
            
        Returns:
            Dictionary with symbols as keys and exchange data as values
        """
        result: Dict[str, Dict[str, Any]] = {}
        
        for exchange_name, exchange_data in data.items():
            if not isinstance(exchange_data, dict):
                continue
                
            for symbol, value in exchange_data.items():
                if symbol not in result:
                    result[symbol] = {}
                result[symbol][exchange_name] = value
                
        return result

    def prepare_data_for_sending(self) -> Dict[str, Any]:
        """Prepare the data structure to be sent to the message queue.
        
        Returns:
            Dictionary containing ticker and funding rate data ready for serialization
        """
        try:
            # Create a deep copy to avoid modifying the original data
            data = {
                "tickers": self.ticker_data.copy(),
                "futures": self.futures_data.copy(),
                "metadata": {
                    "timestamp": int(datetime.now(timezone.utc).timestamp()),
                    "exchange_count": len(self.async_exchange_instances),
                    "ticker_count": sum(len(tickers) for tickers in self.ticker_data.values()),
                    "funding_rate_count": sum(len(rates) for rates in self.futures_data.values())
                }
            }
            
            # Validate data before sending
            if not data["tickers"] and not data["futures"]:
                logger.warning("No ticker or funding rate data available to send")
                return {}
                
            return data
            
        except Exception as e:
            logger.error(f"Error preparing data for sending: {e}", exc_info=True)
            return {}

    async def send_data(self) -> None:
        """Continuously send data to the message queue when it changes.
        
        This method compares hashes of the current and previous data to avoid
        sending duplicate information and reduce network traffic.
        """
        logger.info("Starting data sender")
        
        # Initialize hashes to detect changes
        previous_tickers_hash = ""
        previous_futures_hash = ""
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while not self._shutdown_event.is_set():
            try:
                # Prepare data and calculate hashes
                data = self.prepare_data_for_sending()
                if not data:
                    await asyncio.sleep(1)
                    continue
                
                # Calculate hashes for change detection
                current_tickers_hash = calculate_hash(data.get("tickers", {}))
                current_futures_hash = calculate_hash(data.get("futures", {}))
                
                # Only send if data has changed
                if (current_tickers_hash != previous_tickers_hash or 
                    current_futures_hash != previous_futures_hash):
                    
                    if self.dd:
                        try:
                            await self.dd.set_data(data)
                            logger.debug(
                                f"Sent data: {len(data.get('tickers', {}))} tickers, "
                                f"{len(data.get('futures', {}))} funding rates"
                            )
                            consecutive_errors = 0  # Reset error counter on success
                            
                            # Update hashes only after successful send
                            previous_tickers_hash = current_tickers_hash
                            previous_futures_hash = current_futures_hash
                            
                        except Exception as e:
                            consecutive_errors += 1
                            logger.error(f"Error sending data (attempt {consecutive_errors}): {e}")
                            
                            if consecutive_errors >= max_consecutive_errors:
                                logger.error("Max consecutive errors reached, giving up")
                                break
                    
                # Small delay to prevent busy waiting
                await asyncio.sleep(0.1)
                
            except asyncio.CancelledError:
                logger.info("Data sender was cancelled")
                break
                
            except Exception as e:
                logger.error(f"Unexpected error in send_data: {e}", exc_info=True)
                await asyncio.sleep(1)  # Prevent tight error loop
        
        logger.info("Data sender stopped")

    def calculate_futures_hash(self, data: Dict[str, Any]) -> str:
        """Calculate a hash of the futures data for change detection.
        
        Args:
            data: Dictionary containing futures data
            
        Returns:
            MD5 hash string of the sorted data
        """
        try:
            # Create a stable representation of the data
            sorted_data = sorted(
                (str(k), str(v)) 
                for k, v in data.items()
            )
            return hashlib.md5(json.dumps(sorted_data, sort_keys=True).encode()).hexdigest()
        except Exception as e:
            logger.error(f"Error calculating futures hash: {e}")
            return ""

    @staticmethod
    def reformat_exchanges_data_futures(
        exchanges_data_futures: Dict[str, Dict[str, Dict[str, Any]]]
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Reformat funding rate data into a symbol-first structure.
        
        Args:
            exchanges_data_futures: Nested dictionary of exchange -> symbol -> data
            
        Returns:
            Reformatted dictionary of symbol -> exchange -> funding rate data
        """
        result: Dict[str, Dict[str, Dict[str, Any]]] = {}
        
        for exchange_id, exchange_data in exchanges_data_futures.items():
            if not isinstance(exchange_data, dict):
                continue
                
            for futures_id, futures in exchange_data.items():
                if not isinstance(futures, dict):
                    continue
                    
                if futures_id not in result:
                    result[futures_id] = {}
                    
                # Only include relevant fields to reduce data size
                result[futures_id][exchange_id] = {
                    'fundingRate': futures.get('fundingRate'),
                    'timestamp': futures.get('timestamp'),
                    'datetime': futures.get('datetime')
                }
                
        return result

    async def run(self) -> None:
        """Run the main data collection and processing loop.
        
        This is the main entry point that sets up the application,
        loads configuration, and starts all the necessary components.
        """
        logger.info("Starting CryptoDataStreamer")
        
        try:
            # Initialize configuration
            at = AppTemplate([('config=', "<config filename>")])
            at.get_arguments_from_env()
            at.get_arguments()
            
            if not at.parameters.get('config'):
                raise ValueError("No configuration file specified. Use --config=path/to/config.cfg")
                
            at.load_settings_from_file(at.parameters['config'])
            
            # Initialize data dispatcher if configured
            if all(key in at.settings for key in ["user", "password", "host", "out_exchange"]):
                self.dd = processor_template.DataDispatcher(
                    user=at.settings["user"],
                    password=at.settings["password"],
                    host=at.settings["host"],
                    exchange=at.settings["out_exchange"]
                )
            else:
                logger.warning("Incomplete configuration for data dispatcher. Data will not be sent.")
            
            # Apply settings
            self.api_keys = at.settings.get("api_keys", {})
            self.polls_delay = int(at.settings.get("polls_delay", DEFAULT_POLL_DELAY))
            
            # Get list of exchanges to monitor
            exchanges = at.settings.get("exchanges", [])
            if not exchanges:
                raise ValueError("No exchanges specified in configuration")

                
            logger.info(f"Initializing {len(exchanges)} exchanges: {', '.join(exchanges)}")
            
            # Initialize exchanges
            await self.initialize_exchanges(exchanges)
            
            if not self.async_exchange_instances:
                raise RuntimeError("Failed to initialize any exchanges. Check logs for details.")
                
            logger.info(f"Successfully initialized {len(self.async_exchange_instances)} exchanges")
            
            # Start data processing
            await self.process_exchanges()
            
        except asyncio.CancelledError:
            logger.info("Shutdown requested")
        except Exception as e:
            logger.critical(f"Fatal error: {e}", exc_info=True)
            raise
        finally:
            logger.info("Shutting down...")
            await self.close()
            logger.info("Shutdown complete")


async def signal_handler(signal_name: str) -> None:
    """Handle OS signals for graceful shutdown."""
    logger.info(f"Received {signal_name} signal. Shutting down...")
    
    # Find and cancel all running tasks
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    # Wait for tasks to be cancelled
    await asyncio.gather(*tasks, return_exceptions=True)


async def main() -> None:
    """Main entry point with proper signal handling."""
    # Set up signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(
                sig, lambda s=sig: asyncio.create_task(
                    signal_handler(signal.Signals(sig).name)
                )
            )
        except (NotImplementedError, RuntimeError):
            # Windows or not in main thread
            signal.signal(sig, lambda s, f: asyncio.create_task(signal_handler(signal.Signals(s).name)))
    
    # Initialize and run the data streamer
    data_streamer = CryptoDataStreamer()
    
    try:
        await data_streamer.run()
    except asyncio.CancelledError:
        logger.info("Main task was cancelled")
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}", exc_info=True)
        raise
    finally:
        await data_streamer.close()


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('crypto_streamer.log', mode='w') # Overwrite log on start
        ]
    )
    # Suppress noisy library logs
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("pika").setLevel(logging.WARNING)
    
    # Run the application
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
