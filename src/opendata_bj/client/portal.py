import httpx
import base64
import logging
from typing import List, Optional, Dict, Any, Tuple, AsyncIterator
from urllib.parse import urlparse

from opendata_bj.models.dataset import Dataset
from opendata_bj.config import (
    DEFAULT_BASE_URL,
    API_TIMEOUT,
    RESOURCE_TIMEOUT,
    DEFAULT_HEADERS,
    get_resource_request_headers,
    MAX_PREVIEW_BYTES,
    ENDPOINT_DATASETS_ALL,
    ENDPOINT_ORGANIZATIONS,
    ENDPOINT_BULK_UPLOAD,
)
from opendata_bj.client.rate_limiter import RateLimiter, RetryConfig, execute_with_retry

logger = logging.getLogger(__name__)


class BeninPortalClient:
    """Client for the Benin OpenData Portal API.
    
    Provides methods to query datasets, organizations, and resources with
    built-in support for rate limiting and automatic retry on failures.
    
    Args:
        base_url: Base URL for the API (default: https://donneespubliques.gouv.bj)
        api_key: Optional API key for authenticated requests
        rate_limit: Maximum requests per minute (default: 100, set to 0 to disable)
        retry_attempts: Number of retry attempts for failed requests (default: 3)
        retry_backoff: Backoff factor for exponential delay (default: 2.0)
    
    Example:
        client = BeninPortalClient(rate_limit=60, retry_attempts=5)
        datasets = await client.get_all_datasets(limit=10)
    """
    
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        api_key: Optional[str] = None,
        rate_limit: int = 100,
        retry_attempts: int = 3,
        retry_backoff: float = 2.0
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        
        # Configure rate limiter
        self._rate_limiter = RateLimiter(rate_limit) if rate_limit > 0 else None
        
        # Configure retry settings
        self._retry_config = RetryConfig(
            max_attempts=retry_attempts,
            backoff_factor=retry_backoff
        )
        
        self.client = httpx.AsyncClient(
            timeout=API_TIMEOUT,
            headers=DEFAULT_HEADERS,
            follow_redirects=True,
        )
        self.resource_client = httpx.AsyncClient(
            timeout=RESOURCE_TIMEOUT,
            headers={},
            follow_redirects=True,
        )

    async def _make_request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Make an HTTP request with rate limiting and retry logic.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            **kwargs: Additional arguments for the request
            
        Returns:
            HTTP response (may have error status)
        """
        # Apply rate limiting if configured
        if self._rate_limiter:
            await self._rate_limiter.acquire()
        
        # Execute request with retry logic
        return await execute_with_retry(
            operation=lambda: self.client.request(method, url, **kwargs),
            config=self._retry_config,
            operation_name=f"{method} {url}"
        )

    async def get_all_datasets(
        self, query: Optional[str] = None, limit: int = 10, offset: int = 0
    ) -> List[Dataset]:
        """Fetch datasets with optional pagination.

        Args:
            query: Optional search query string
            limit: Maximum number of results to return (default: 10)
            offset: Number of results to skip for pagination (default: 0)

        Returns:
            List of Dataset objects matching the query
        """
        url = f"{self.base_url}{ENDPOINT_DATASETS_ALL}"
        params = {"format": "json", "limit": limit, "offset": offset}
        if query:
            params["q"] = query

        response = await self._make_request("GET", url, params=params)
        response.raise_for_status()
        data = response.json()

        return [Dataset(**ds) for ds in data.get("datasets", [])]

    async def iter_all_datasets(
        self, query: Optional[str] = None, batch_size: int = 100
    ) -> AsyncIterator[Dataset]:
        """Iterate through all datasets with automatic pagination.

        This async generator yields datasets one by one, handling pagination
        automatically to fetch all results from the API.

        Args:
            query: Optional search query string
            batch_size: Number of datasets to fetch per API call (default: 100)

        Yields:
            Dataset objects one at a time

        Example:
            async for dataset in client.iter_all_datasets(query="health"):
                print(dataset.title)
        """
        offset = 0
        while True:
            batch = await self.get_all_datasets(
                query=query, limit=batch_size, offset=offset
            )
            if not batch:
                break
            for dataset in batch:
                yield dataset
            offset += batch_size
            if len(batch) < batch_size:
                break

    async def get_dataset_details(self, dataset_id: str) -> Optional[Dataset]:
        """Get detailed information about a specific dataset.
        
        Args:
            dataset_id: Unique identifier of the dataset
            
        Returns:
            Dataset object if found, None otherwise
        """
        url = f"{self.base_url}{ENDPOINT_DATASETS_ALL}"
        params = {"format": "json", "q": dataset_id, "limit": 100}

        response = await self._make_request("GET", url, params=params)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.json()

        for ds in data.get("datasets", []):
            if ds.get("id") == dataset_id or ds.get("dataset_id") == dataset_id:
                return Dataset(**ds)
        return None

    async def bulk_upload(
        self, metadata: Dict[str, Any], files: List[str]
    ) -> Dict[str, Any]:
        """Upload multiple datasets in bulk.
        
        Args:
            metadata: Metadata for the datasets
            files: List of file paths to upload
            
        Returns:
            API response as dictionary
        """
        url = f"{self.base_url}{ENDPOINT_BULK_UPLOAD}"
        data = {"api_key": self.api_key} if self.api_key else {}

        response = await self._make_request("POST", url, json=metadata, params=data)
        response.raise_for_status()
        return response.json()

    async def get_organizations(self) -> List[str]:
        """Get list of all organizations publishing data.
        
        Returns:
            Sorted list of organization names
        """
        url = f"{self.base_url}{ENDPOINT_ORGANIZATIONS}"
        
        try:
            response = await self._make_request("GET", url)
            
            if response.status_code == 404:
                datasets = await self.get_all_datasets(limit=100)
                orgs = {ds.organization for ds in datasets if ds.organization}
                return sorted(list(orgs))

            response.raise_for_status()
            data = response.json()
            return [
                org.get("name") or org.get("title")
                for org in data.get("data", [])
                if org.get("name") or org.get("title")
            ]
        except httpx.HTTPStatusError:
            # Fallback to extracting from datasets
            datasets = await self.get_all_datasets(limit=100)
            orgs = {ds.organization for ds in datasets if ds.organization}
            return sorted(list(orgs))

    async def get_resource_preview(
        self, resource_url: str, max_rows: int = 10
    ) -> Tuple[List[str], List[List[str]]]:
        import csv
        from io import StringIO

        request_headers = get_resource_request_headers(f"{self.base_url}/")
        request_headers["Range"] = f"bytes=0-{MAX_PREVIEW_BYTES}"

        try:
            response = await self.resource_client.get(
                resource_url, headers=request_headers
            )

            if response.status_code == 416:
                del request_headers["Range"]
                response = await self.resource_client.get(
                    resource_url, headers=request_headers
                )

            if response.status_code == 403:
                raise PermissionError(
                    f"Access forbidden (403). Resource may require authentication. "
                    f"Manual download: {resource_url}"
                )

            response.raise_for_status()
            content = response.content.decode("utf-8", errors="replace")
            lines = content.split("\n")[: max_rows + 1]

            if not lines:
                return [], []

            reader = csv.reader(StringIO("\n".join(lines)))
            rows = list(reader)

            if not rows:
                return [], []

            return rows[0], rows[1 : max_rows + 1]

        except PermissionError:
            raise
        except Exception as e:
            raise Exception(f"Failed to preview resource: {str(e)}")

    async def download_resource(
        self, resource_url: str, max_size_mb: int = 10
    ) -> Tuple[bytes, str, str]:
        max_bytes = max_size_mb * 1024 * 1024
        request_headers = get_resource_request_headers(f"{self.base_url}/")

        try:
            async with self.resource_client.stream(
                "GET", resource_url, headers=request_headers
            ) as response:
                if response.status_code == 403:
                    raise PermissionError(
                        f"Access forbidden (403). Resource may require authentication. "
                        f"Manual download: {resource_url}"
                    )

                response.raise_for_status()

                content_length = response.headers.get("content-length")
                if content_length and int(content_length) > max_bytes:
                    raise ValueError(
                        f"File too large: {int(content_length) / 1024 / 1024:.2f}MB > {max_size_mb}MB"
                    )

                chunks = []
                total_size = 0

                async for chunk in response.aiter_bytes(chunk_size=8192):
                    chunks.append(chunk)
                    total_size += len(chunk)
                    if total_size > max_bytes:
                        raise ValueError(f"File exceeds {max_size_mb}MB limit")

                content = b"".join(chunks)

            parsed = urlparse(resource_url)
            filename = parsed.path.split("/")[-1] or "download"

            content_disp = response.headers.get("content-disposition", "")
            if "filename=" in content_disp:
                filename = content_disp.split("filename=")[-1].strip('"\'')

            mime_type = response.headers.get(
                "content-type", "application/octet-stream"
            )

            return content, filename, mime_type

        except PermissionError:
            raise
        except ValueError:
            raise
        except Exception as e:
            raise Exception(f"Failed to download resource: {str(e)}")

    async def close(self):
        await self.client.aclose()
        await self.resource_client.aclose()
