import os
from typing import Optional
from pathlib import Path
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
    It reads the content from the skill.md file.
    """
    skill_file = Path(__file__).parent / "skill.md"
    if skill_file.exists():
        return skill_file.read_text()
    return "Error: skill.md file not found."

if __name__ == "__main__":
    mcp.run()
