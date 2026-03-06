import os
from typing import Optional
from fastmcp import FastMCP
from opendata_bj.client.portal import BeninPortalClient
from opendata_bj.tools import datasets, admin

# Initialize FastMCP server
mcp = FastMCP("opendata-bj")

# Singleton asynchronous client
_client: Optional[BeninPortalClient] = None

async def get_client() -> BeninPortalClient:
    global _client
    if _client is None:
        api_key = os.getenv("BENIN_OPEN_DATA_API_KEY")
        _client = BeninPortalClient(api_key=api_key)
    return _client

@mcp.tool()
async def search_datasets(query: Optional[str] = None, limit: int = 10) -> str:
    """Search for public datasets from Benin."""
    client = await get_client()
    return await datasets.search_datasets(client, query, limit)

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
async def download_all_datasets(limit: int = 1000, include_resources: bool = True) -> str:
    """Download all available datasets into a ZIP archive."""
    client = await get_client()
    return await datasets.download_all_datasets(client, limit, include_resources)

@mcp.tool()
async def publish_datasets_bulk(metadata_json: str) -> str:
    """Bulk upload datasets."""
    client = await get_client()
    return await admin.publish_datasets_bulk(client, metadata_json)

if __name__ == "__main__":
    mcp.run()
