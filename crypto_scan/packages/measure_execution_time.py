import time
import asyncio
import logging
from typing import Callable, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def measure_execution_time(func: Callable, *args: Any, **kwargs: Any) -> Any:
    """
    Measures the execution time of a given function.

    :param func: The function to execute.
    :param args: Positional arguments to pass to the function.
    :param kwargs: Keyword arguments to pass to the function.
    :return: The result of the function execution.
    """
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    end_time = time.perf_counter()
    execution_time = end_time - start_time

    logger.info(f"Function '{func.__name__}' executed in {execution_time:.6f} seconds.")
    return result

async def measure_async_execution_time(func: Callable, *args: Any, **kwargs: Any) -> Any:
    """
    Measures the execution time of a given asynchronous function.

    :param func: The asynchronous function to execute.
    :param args: Positional arguments to pass to the function.
    :param kwargs: Keyword arguments to pass to the function.
    :return: The result of the function execution.
    """
    start_time = time.perf_counter()
    result = await func(*args, **kwargs)
    end_time = time.perf_counter()
    execution_time = end_time - start_time

    logger.info(f"Async function '{func.__name__}' executed in {execution_time:.6f} seconds.")
    return result

# Example usage:
def example_function(x, y):
    time.sleep(1)  # Simulate a time-consuming operation
    return x + y

async def example_async_function(x, y):
    await asyncio.sleep(1)  # Simulate a time-consuming operation
    return x + y

# Measure execution time
result = measure_execution_time(example_function, 3, 5)
logger.info(f"Result: {result}")

# Measure async execution time
async def main():
    async_result = await measure_async_execution_time(example_async_function, 3, 5)
    logger.info(f"Async result: {async_result}")

if __name__ == "__main__":
    asyncio.run(main())
