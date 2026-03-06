import os
from typing import Optional
from fastmcp import FastMCP, resource
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

# --- Tools ---

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
async def publish_datasets_bulk(metadata_json: str) -> str:
    """(Admin) Bulk upload datasets."""
    client = await get_client()
    return await admin.publish_datasets_bulk(client, metadata_json)

# --- Skill Resource ---

@mcp.resource(
    "system://skill",
    name="OpenData-BJ MCP Skill",
    description="Provides a comprehensive guide on how to use the tools available in this MCP.",
    mime_type="text/markdown"
)
async def get_skill_documentation() -> str:
    """
    This resource acts as a dynamic SKILL.md, explaining the MCP's capabilities to another agent.
    """
    return """
# Skill: Benin Open Data MCP

This skill provides the capability to search, retrieve, and manage public datasets from Benin's official open data portal (`donneespubliques.gouv.bj`).

## Available Tools

### `search_datasets`
Searches for public datasets on the Benin open data portal. It's the primary entry point for discovering data.

- **Parameters:**
  - `query` (string, optional): The search keyword or phrase.
  - `limit` (integer, optional, default: 10): The maximum number of datasets to return.
- **Returns:** A formatted string listing the found datasets, including their ID, title, and organization.
- **When to use:** When the user asks to find data about a specific topic (e.g., "trouve des données sur la santé").

### `get_dataset`
Retrieves detailed information and resources for a specific dataset using its ID.

- **Parameters:**
  - `dataset_id` (string, required): The unique identifier of the dataset, usually obtained from `search_datasets`.
- **Returns:** A markdown-formatted string with the dataset's title, description, organization, tags, and a list of available resources with their URLs.
- **When to use:** After finding a dataset with `search_datasets`, use this to get more details or download links.

### `list_organizations`
Lists all the institutions and organizations that publish data on the portal.

- **Parameters:** None.
- **Returns:** A formatted string listing all available organizations.
- **When to use:** When the user asks "who publishes data?" or wants to filter by a specific organization.

### `publish_datasets_bulk`
**(Admin Tool)** Uploads multiple datasets to the portal in a single operation.

- **Parameters:**
  - `metadata_json` (string, required): A JSON string representing the metadata for the datasets to be uploaded.
- **Returns:** A confirmation message indicating success or failure.
- **When to use:** Only for administrative tasks involving bulk data publication. Requires a valid API key with write permissions.

## Recommended Workflow

1.  **Discover**: Start with a broad search using `search_datasets(query="...")`.
2.  **Clarify**: If multiple datasets are found, present the list to the user to let them choose.
3.  **Detail**: Use `get_dataset(dataset_id="...")` with the ID chosen by the user to get detailed information and resource URLs.
4.  **Explore**: If the user is unsure where to start, use `list_organizations()` to show them which institutions provide data.
"""

if __name__ == "__main__":
    mcp.run()
