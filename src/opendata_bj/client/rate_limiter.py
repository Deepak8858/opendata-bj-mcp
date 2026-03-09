"""Rate limiting and retry utilities for the Benin OpenData API client."""

import asyncio
import logging
import time
from typing import Optional, Callable, Any
from functools import wraps

import httpx

logger = logging.getLogger(__name__)


class RateLimiter:
    """Token bucket rate limiter for controlling request rate.
    
    Implements a token bucket algorithm to limit the number of requests
    per time window. Useful for respecting API rate limits.
    
    Args:
        max_requests_per_minute: Maximum number of requests allowed per minute
        
    Example:
        limiter = RateLimiter(max_requests_per_minute=100)
        await limiter.acquire()
        # Make API request
    """
    
    def __init__(self, max_requests_per_minute: int = 100):
        self.max_rate = max_requests_per_minute
        self.tokens = float(max_requests_per_minute)
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()
    
    async def acquire(self) -> None:
        """Acquire a token, waiting if necessary to respect rate limit."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            
            # Replenish tokens based on elapsed time
            self.tokens = min(
                self.max_rate,
                self.tokens + elapsed * self.max_rate / 60.0
            )
            
            if self.tokens < 1.0:
                # Calculate wait time needed for one token
                sleep_time = (1.0 - self.tokens) * 60.0 / self.max_rate
                logger.debug(f"Rate limit: waiting {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
                self.tokens = 0.0
            else:
                self.tokens -= 1.0
            
            self.last_update = now


class RetryConfig:
    """Configuration for retry behavior.
    
    Args:
        max_attempts: Maximum number of retry attempts (default: 3)
        base_delay: Initial delay between retries in seconds (default: 1.0)
        max_delay: Maximum delay between retries in seconds (default: 60.0)
        backoff_factor: Multiplicative factor for exponential backoff (default: 2.0)
        retry_on_status: Set of HTTP status codes that trigger a retry (default: {429, 500, 502, 503, 504})
        respect_retry_after: Whether to respect the Retry-After header (default: True)
    """
    
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
        retry_on_status: Optional[set] = None,
        respect_retry_after: bool = True
    ):
        self.max_attempts = max(max_attempts, 1)
        self.base_delay = max(base_delay, 0.0)
        self.max_delay = max(max_delay, 0.0)
        self.backoff_factor = max(backoff_factor, 1.0)
        self.retry_on_status = retry_on_status or {429, 500, 502, 503, 504}
        self.respect_retry_after = respect_retry_after
    
    def calculate_delay(self, attempt: int, retry_after: Optional[int] = None) -> float:
        """Calculate the delay before the next retry attempt.
        
        Args:
            attempt: Current attempt number (0-indexed)
            retry_after: Optional Retry-After header value in seconds
            
        Returns:
            Delay in seconds
        """
        if retry_after is not None and self.respect_retry_after:
            return min(float(retry_after), self.max_delay)
        
        delay = self.base_delay * (self.backoff_factor ** attempt)
        return min(delay, self.max_delay)


async def execute_with_retry(
    operation: Callable[[], Any],
    config: RetryConfig,
    operation_name: str = "operation"
) -> Any:
    """Execute an async operation with retry logic.
    
    Args:
        operation: Async callable to execute
        config: Retry configuration
        operation_name: Name of the operation for logging
        
    Returns:
        Result of the operation
        
    Raises:
        httpx.HTTPStatusError: If all retry attempts fail
        Exception: If the operation raises an unexpected exception
    """
    last_error: Optional[Exception] = None
    
    for attempt in range(config.max_attempts):
        try:
            response = await operation()
            
            # Check for retryable status codes
            if isinstance(response, httpx.Response):
                if response.status_code in config.retry_on_status:
                    # Check if this is the last attempt
                    if attempt == config.max_attempts - 1:
                        logger.warning(
                            f"{operation_name} failed after {config.max_attempts} attempts: "
                            f"HTTP {response.status_code}"
                        )
                        # Return the response even with error status on last attempt
                        return response
                    
                    # Get Retry-After header if present
                    retry_after = None
                    if config.respect_retry_after:
                        retry_after_header = response.headers.get("Retry-After")
                        if retry_after_header:
                            try:
                                retry_after = int(retry_after_header)
                            except ValueError:
                                pass
                    
                    # Calculate delay
                    delay = config.calculate_delay(attempt, retry_after)
                    
                    logger.info(
                        f"{operation_name} got HTTP {response.status_code}, "
                        f"retrying in {delay:.2f}s (attempt {attempt + 1}/{config.max_attempts})"
                    )
                    
                    await asyncio.sleep(delay)
                    continue
            
            return response
            
        except httpx.HTTPStatusError as e:
            last_error = e
            status_code = e.response.status_code
            
            # Check if we should retry this status code
            if status_code not in config.retry_on_status:
                raise
            
            # Check if this is the last attempt
            if attempt == config.max_attempts - 1:
                logger.warning(
                    f"{operation_name} failed after {config.max_attempts} attempts: "
                    f"HTTP {status_code}"
                )
                raise
            
            # Get Retry-After header if present
            retry_after = None
            if config.respect_retry_after:
                retry_after_header = e.response.headers.get("Retry-After")
                if retry_after_header:
                    try:
                        retry_after = int(retry_after_header)
                    except ValueError:
                        pass
            
            # Calculate delay
            delay = config.calculate_delay(attempt, retry_after)
            
            logger.info(
                f"{operation_name} got HTTP {status_code}, "
                f"retrying in {delay:.2f}s (attempt {attempt + 1}/{config.max_attempts})"
            )
            
            await asyncio.sleep(delay)
            
        except Exception:
            # Don't retry non-HTTP errors
            raise
    
    # This should never be reached, but just in case
    if last_error:
        raise last_error
    raise RuntimeError("Unexpected end of retry loop")
