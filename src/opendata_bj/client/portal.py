import httpx
import base64
from typing import List, Optional, Dict, Any, Tuple
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


class BeninPortalClient:
    def __init__(self, base_url: str = DEFAULT_BASE_URL, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
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

    async def get_all_datasets(
        self, query: Optional[str] = None, limit: int = 10
    ) -> List[Dataset]:
        url = f"{self.base_url}{ENDPOINT_DATASETS_ALL}"
        params = {"format": "json", "limit": limit}
        if query:
            params["q"] = query

        response = await self.client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        return [Dataset(**ds) for ds in data.get("datasets", [])]

    async def get_dataset_details(self, dataset_id: str) -> Optional[Dataset]:
        url = f"{self.base_url}{ENDPOINT_DATASETS_ALL}"
        params = {"format": "json", "q": dataset_id, "limit": 100}

        response = await self.client.get(url, params=params)
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
        url = f"{self.base_url}{ENDPOINT_BULK_UPLOAD}"
        data = {"api_key": self.api_key} if self.api_key else {}

        response = await self.client.post(url, json=metadata, params=data)
        response.raise_for_status()
        return response.json()

    async def get_organizations(self) -> List[str]:
        url = f"{self.base_url}{ENDPOINT_ORGANIZATIONS}"
        response = await self.client.get(url)

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
