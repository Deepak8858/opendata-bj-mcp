import os
from typing import Optional
from pathlib import Path
from fastmcp import FastMCP
from opendata_bj.client.portal import BeninPortalClient
from opendata_bj.tools import datasets, admin
from opendata_bj.config import DEFAULT_PREVIEW_ROWS, DEFAULT_DOWNLOAD_SIZE_MB

mcp = FastMCP("opendata-bj")

_client: Optional[BeninPortalClient] = None


async def get_client() -> BeninPortalClient:
    global _client
    if _client is None:
        api_key = os.getenv("BENIN_OPEN_DATA_API_KEY")
        _client = BeninPortalClient(api_key=api_key)
    return _client


@mcp.tool()
async def search_datasets(query: Optional[str] = None, limit: int = 10, offset: int = 0) -> str:
    """Search for public datasets from Benin.
    
    Args:
        query: Search keyword to filter datasets
        limit: Maximum number of results (1-100, default: 10)
        offset: Skip N results for pagination (default: 0)
    """
    client = await get_client()
    return await datasets.search_datasets(client, query, limit, offset)


@mcp.tool()
async def get_dataset(dataset_id: str) -> str:
    """Retrieve details for a specific dataset."""
    client = await get_client()
    return await datasets.get_dataset(client, dataset_id)


@mcp.tool()
async def list_organizations() -> str:
    """List institutions that publish data."""
    client = await get_client()
    return await datasets.list_organizations(client)


@mcp.tool()
async def preview_dataset(
    dataset_id: str, resource_index: int = 0, rows: int = DEFAULT_PREVIEW_ROWS
) -> str:
    """
    Preview the first rows of a dataset resource.

    Args:
        dataset_id: The ID of the dataset to preview
        resource_index: Index of the resource (0 = first resource, default: 0)
        rows: Number of rows to show (1-50, default: 10)
    """
    client = await get_client()
    return await datasets.preview_dataset(client, dataset_id, resource_index, rows)


@mcp.tool()
async def download_dataset(
    dataset_id: str,
    resource_index: int = 0,
    max_size_mb: int = DEFAULT_DOWNLOAD_SIZE_MB,
    method: str = "auto",
) -> dict:
    """
    Download a dataset resource with adaptive method selection.

    Args:
        dataset_id: The ID of the dataset to download
        resource_index: Index of the resource (0 = first resource, default: 0)
        max_size_mb: Maximum file size in MB (1-50, default: 10)
        method: Download method - "auto" (default), "url", or "content"
            - "auto": Returns base64 if < 1MB, URL if >= 1MB or HTML format
            - "url": Always returns the direct download URL
            - "content": Always downloads and returns base64 content

    Returns:
        dict with success status. For method="url": download_url, filename, format.
        For method="content": content_base64, filename, size_bytes, mime_type.
    """
    client = await get_client()
    return await datasets.download_dataset(client, dataset_id, resource_index, max_size_mb, method)


@mcp.tool()
async def publish_datasets_bulk(metadata_json: str) -> str:
    """(Admin) Bulk upload datasets."""
    client = await get_client()
    return await admin.publish_datasets_bulk(client, metadata_json)


@mcp.resource(
    "system://skill",
    name="OpenData-BJ MCP Skill",
    description="Provides a comprehensive guide on how to use the tools available in this MCP.",
    mime_type="text/markdown",
)
async def get_skill_documentation() -> str:
    skill_file = Path(__file__).parent / "skill.md"
    if skill_file.exists():
        return skill_file.read_text()
    return "Error: skill.md file not found."


if __name__ == "__main__":
    mcp.run()
