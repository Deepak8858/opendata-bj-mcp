import os
from typing import Optional
from pathlib import Path
from fastmcp import FastMCP, Context
from fastmcp.dependencies import CurrentContext
from fastmcp.server.lifespan import lifespan
from opendata_bj.client.portal import BeninPortalClient
from opendata_bj.tools import datasets, admin
from opendata_bj.config import DEFAULT_PREVIEW_ROWS, DEFAULT_DOWNLOAD_SIZE_MB


@lifespan
async def app_lifespan(server):
    """
    Manage the HTTP client lifecycle.
    Creation at startup, clean shutdown.
    """
    # STARTUP: Create the client
    api_key = os.getenv("BENIN_OPEN_DATA_API_KEY")
    client = BeninPortalClient(api_key=api_key)
    
    # Store the client in the lifespan context
    yield {"client": client}
    
    # SHUTDOWN: Properly close connections
    await client.close()


# Create the MCP server with the lifespan
mcp = FastMCP("opendata-bj", lifespan=app_lifespan)


@mcp.tool()
async def search_datasets(
    query: Optional[str] = None, 
    limit: int = 10, 
    offset: int = 0,
    ctx: Context = CurrentContext()
) -> str:
    """Search for public datasets from Benin.
    
    Args:
        query: Search keyword to filter datasets
        limit: Maximum number of results (1-100, default: 10)
        offset: Skip N results for pagination (default: 0)
    """
    client = ctx.lifespan_context["client"]
    return await datasets.search_datasets(client, query, limit, offset)


@mcp.tool()
async def get_dataset(dataset_id: str, ctx: Context = CurrentContext()) -> str:
    """Retrieve details for a specific dataset."""
    client = ctx.lifespan_context["client"]
    return await datasets.get_dataset(client, dataset_id)


@mcp.tool()
async def list_organizations(ctx: Context = CurrentContext()) -> str:
    """List institutions that publish data."""
    client = ctx.lifespan_context["client"]
    return await datasets.list_organizations(client)


@mcp.tool()
async def preview_dataset(
    dataset_id: str, 
    resource_index: int = 0, 
    rows: int = DEFAULT_PREVIEW_ROWS,
    ctx: Context = CurrentContext()
) -> str:
    """
    Preview the first rows of a dataset resource.

    Args:
        dataset_id: The ID of the dataset to preview
        resource_index: Index of the resource (0 = first resource, default: 0)
        rows: Number of rows to show (1-50, default: 10)
    """
    client = ctx.lifespan_context["client"]
    return await datasets.preview_dataset(client, dataset_id, resource_index, rows)


@mcp.tool()
async def download_dataset(
    dataset_id: str,
    resource_index: int = 0,
    max_size_mb: int = DEFAULT_DOWNLOAD_SIZE_MB,
    method: str = "auto",
    ctx: Context = CurrentContext()
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
    client = ctx.lifespan_context["client"]
    return await datasets.download_dataset(client, dataset_id, resource_index, max_size_mb, method)


@mcp.tool()
async def publish_datasets_bulk(metadata_json: str, ctx: Context = CurrentContext()) -> str:
    """(Admin) Bulk upload datasets."""
    client = ctx.lifespan_context["client"]
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
