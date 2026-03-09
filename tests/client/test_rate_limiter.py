"""Tests for rate limiting and retry functionality."""

import pytest
import respx
import httpx
from httpx import Response

from opendata_bj.client.rate_limiter import RateLimiter, RetryConfig, execute_with_retry
from opendata_bj.client.portal import BeninPortalClient


@pytest.mark.asyncio
async def test_rate_limiter_basic():
    """Test that rate limiter controls request rate."""
    limiter = RateLimiter(max_requests_per_minute=60)  # 1 per second
    
    import time
    start_time = time.monotonic()
    
    # First acquire should be immediate
    await limiter.acquire()
    
    # Consume all tokens
    for _ in range(59):
        await limiter.acquire()
    
    # Second acquire should wait (no more tokens)
    await limiter.acquire()
    
    elapsed = time.monotonic() - start_time
    # Should have waited at least ~1 second
    assert elapsed >= 0.5


@pytest.mark.asyncio
async def test_rate_limiter_high_limit():
    """Test rate limiter with high limit (no waiting)."""
    limiter = RateLimiter(max_requests_per_minute=6000)  # 100 per second
    
    # Multiple acquires should not wait with such a high limit
    for _ in range(5):
        await limiter.acquire()


@pytest.mark.asyncio
async def test_retry_config_calculate_delay():
    """Test retry delay calculation."""
    config = RetryConfig(
        base_delay=1.0,
        backoff_factor=2.0,
        max_delay=10.0
    )
    
    # Test exponential backoff
    assert config.calculate_delay(0) == 1.0  # 1 * 2^0
    assert config.calculate_delay(1) == 2.0  # 1 * 2^1
    assert config.calculate_delay(2) == 4.0  # 1 * 2^2
    assert config.calculate_delay(3) == 8.0  # 1 * 2^3
    assert config.calculate_delay(4) == 10.0  # Capped at max_delay
    
    # Test with Retry-After header
    assert config.calculate_delay(0, retry_after=5) == 5.0
    assert config.calculate_delay(0, retry_after=15) == 10.0  # Capped at max_delay


@pytest.mark.asyncio
async def test_execute_with_retry_success():
    """Test that successful operation doesn't retry."""
    config = RetryConfig(max_attempts=3)
    call_count = 0
    
    async def operation():
        nonlocal call_count
        call_count += 1
        return "success"
    
    result = await execute_with_retry(operation, config, "test_op")
    assert result == "success"
    assert call_count == 1


def create_mock_response(status_code: int, headers=None):
    """Create a mock response with headers."""
    if headers is None:
        headers = {}
    return type('Response', (), {
        'status_code': status_code,
        'headers': headers
    })()


@pytest.mark.asyncio
async def test_execute_with_retry_eventual_success():
    """Test retry until success with proper httpx Response objects."""
    config = RetryConfig(max_attempts=3, base_delay=0.01, retry_on_status={503})
    call_count = 0
    
    async def operation():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            # Return a response with retryable status
            return Response(503)
        return Response(200)
    
    result = await execute_with_retry(operation, config, "test_op")
    assert result.status_code == 200
    assert call_count == 3


@pytest.mark.asyncio
async def test_execute_with_retry_exhausted():
    """Test that all retries are exhausted before failing."""
    config = RetryConfig(max_attempts=3, base_delay=0.01)
    call_count = 0
    
    async def operation():
        nonlocal call_count
        call_count += 1
        raise httpx.HTTPStatusError(
            "Server error",
            request=None,
            response=create_mock_response(503)
        )
    
    with pytest.raises(httpx.HTTPStatusError):
        await execute_with_retry(operation, config, "test_op")
    
    assert call_count == 3


@pytest.mark.asyncio
async def test_execute_with_retry_no_retry_status():
    """Test that non-retryable status codes fail immediately."""
    config = RetryConfig(max_attempts=3, retry_on_status={429, 503})
    call_count = 0
    
    async def operation():
        nonlocal call_count
        call_count += 1
        raise httpx.HTTPStatusError(
            "Not found",
            request=None,
            response=create_mock_response(404)
        )
    
    with pytest.raises(httpx.HTTPStatusError):
        await execute_with_retry(operation, config, "test_op")
    
    # Should not retry on 404
    assert call_count == 1


@pytest.mark.asyncio
async def test_execute_with_retry_respects_retry_after():
    """Test that Retry-After header is respected."""
    config = RetryConfig(max_attempts=2, base_delay=10.0, respect_retry_after=True)
    
    async def operation():
        raise httpx.HTTPStatusError(
            "Rate limited",
            request=None,
            response=create_mock_response(429, {"Retry-After": "1"})
        )
    
    with pytest.raises(httpx.HTTPStatusError):
        await execute_with_retry(operation, config, "test_op")


@pytest.mark.asyncio
async def test_client_with_rate_limiting():
    """Test that client respects rate limit configuration."""
    client = BeninPortalClient(rate_limit=60, retry_attempts=5)
    
    assert client._rate_limiter is not None
    assert client._rate_limiter.max_rate == 60
    assert client._retry_config.max_attempts == 5
    
    await client.close()


@pytest.mark.asyncio
async def test_client_disabled_rate_limiting():
    """Test that rate limiting can be disabled."""
    client = BeninPortalClient(rate_limit=0)
    
    assert client._rate_limiter is None
    
    await client.close()


@pytest.mark.asyncio
async def test_client_retry_on_429():
    """Test that client retries on 429 Too Many Requests."""
    client = BeninPortalClient(rate_limit=0, retry_attempts=3, retry_backoff=1.0)
    
    mock_data = {
        "datasets": [
            {"id": "ds1", "name": "ds1", "title": "Dataset 1", "organization": "Org1", "resources": []}
        ]
    }
    
    request_count = 0
    
    def mock_handler(request):
        nonlocal request_count
        request_count += 1
        if request_count < 3:
            return Response(429, headers={"Retry-After": "0.01"})
        return Response(200, json=mock_data)
    
    with respx.mock:
        respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all"
        ).mock(side_effect=mock_handler)
        
        datasets = await client.get_all_datasets()
        
        assert len(datasets) == 1
        assert datasets[0].id == "ds1"
        assert request_count == 3  # 2 failures + 1 success
    
    await client.close()


@pytest.mark.asyncio
async def test_client_retry_on_503():
    """Test that client retries on 503 Service Unavailable."""
    client = BeninPortalClient(rate_limit=0, retry_attempts=2, retry_backoff=1.0)
    
    mock_data = {"datasets": []}
    
    with respx.mock:
        route = respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all"
        )
        route.side_effect = [
            Response(503),
            Response(200, json=mock_data)
        ]
        
        datasets = await client.get_all_datasets()
        assert datasets == []
        assert route.call_count == 2
    
    await client.close()


@pytest.mark.asyncio
async def test_client_no_retry_on_404():
    """Test that client does not retry on 404 Not Found."""
    client = BeninPortalClient(rate_limit=0, retry_attempts=3)
    
    with respx.mock:
        route = respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all"
        ).mock(return_value=Response(404))
        
        # get_all_datasets should handle 404 gracefully
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_all_datasets()
        
        assert route.call_count == 1  # No retry
    
    await client.close()
