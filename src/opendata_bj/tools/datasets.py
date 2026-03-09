from typing import Optional
import base64

from opendata_bj.client.portal import BeninPortalClient
from opendata_bj.config import (
    MAX_PREVIEW_ROWS,
    DEFAULT_PREVIEW_ROWS,
    MAX_DOWNLOAD_SIZE_MB,
    DEFAULT_DOWNLOAD_SIZE_MB,
)


async def search_datasets(
    client: BeninPortalClient, query: Optional[str] = None, limit: int = 10, offset: int = 0
) -> str:
    """Search for public datasets from Benin.
    
    Args:
        client: The BeninPortalClient instance
        query: Optional search query string
        limit: Maximum number of results to return (default: 10)
        offset: Number of results to skip for pagination (default: 0)
    
    Returns:
        Formatted string with dataset information
    """
    datasets = await client.get_all_datasets(query=query, limit=limit, offset=offset)

    if not datasets:
        return "No datasets found for this search."

    return "\n".join(
        f"- [{ds.id}] {ds.title} (Organization: {ds.organization})"
        for ds in datasets
    )


async def get_dataset(client: BeninPortalClient, dataset_id: str) -> str:
    ds = await client.get_dataset_details(dataset_id)

    if not ds:
        return f"Dataset '{dataset_id}' not found."

    output = [
        f"# {ds.title}",
        f"**Description**: {ds.description}",
        f"**Organization**: {ds.organization}",
        f"**Tags**: {', '.join(ds.tags)}",
        "\n## Available Resources:",
    ]

    for res in ds.resources:
        output.append(f"- {res.name} ({res.format}) : {res.url}")

    return "\n".join(output)


async def list_organizations(client: BeninPortalClient) -> str:
    orgs = await client.get_organizations()
    if not orgs:
        return "No organizations found."
    return "Available Organizations:\n" + "\n".join(f"- {org}" for org in orgs)


async def preview_dataset(
    client: BeninPortalClient,
    dataset_id: str,
    resource_index: int = 0,
    rows: int = DEFAULT_PREVIEW_ROWS,
) -> str:
    rows = min(max(rows, 1), MAX_PREVIEW_ROWS)

    ds = await client.get_dataset_details(dataset_id)
    if not ds:
        return f"Dataset '{dataset_id}' not found."

    if not ds.resources:
        return f"Dataset '{dataset_id}' has no downloadable resources."

    if resource_index >= len(ds.resources):
        return f"Resource index {resource_index} out of range. Dataset has {len(ds.resources)} resources."

    resource = ds.resources[resource_index]

    supported_formats = ["CSV", "TXT", "JSON", "XLS", "XLSX"]
    if resource.format.upper() not in supported_formats:
        return (
            f"⚠️ Cannot preview resource '{resource.name}'.\n"
            f"**Format**: {resource.format} (not supported for preview)\n\n"
            f"Supported formats: {', '.join(supported_formats)}\n\n"
            f"You can download the raw file using `download_dataset`:\n"
            f"- Dataset ID: `{dataset_id}`\n"
            f"- Resource Index: `{resource_index}`\n"
            f"- URL: {resource.url}"
        )

    try:
        headers, data_rows = await client.get_resource_preview(resource.url, max_rows=rows)

        if not headers:
            return (
                f"⚠️ Preview failed for '{resource.name}'\n"
                f"**Format**: {resource.format}\n\n"
                f"The file may be empty, malformed, or in an unexpected format.\n\n"
                f"Try downloading the file instead:\n"
                f"`download_dataset(dataset_id='{dataset_id}', resource_index={resource_index})`"
            )

        output = [
            f"# Preview: {resource.name}",
            f"**Format**: {resource.format}",
            f"**URL**: {resource.url}",
            f"\nShowing first {len(data_rows)} row(s):\n",
        ]

        header_line = "| " + " | ".join(headers) + " |"
        separator = "|" + "|".join([" --- " for _ in headers]) + "|"

        output.extend([header_line, separator])

        for row in data_rows:
            padded_row = row + [""] * (len(headers) - len(row))
            output.append("| " + " | ".join(padded_row) + " |")

        return "\n".join(output)

    except PermissionError as e:
        return f"❌ Access denied: {str(e)}"
    except Exception as e:
        return f"Error previewing resource: {str(e)}"


async def download_dataset(
    client: BeninPortalClient,
    dataset_id: str,
    resource_index: int = 0,
    max_size_mb: int = DEFAULT_DOWNLOAD_SIZE_MB,
) -> dict:
    max_size_mb = min(max(max_size_mb, 1), MAX_DOWNLOAD_SIZE_MB)

    ds = await client.get_dataset_details(dataset_id)
    if not ds:
        return {"success": False, "error": f"Dataset '{dataset_id}' not found."}

    if not ds.resources:
        return {
            "success": False,
            "error": f"Dataset '{dataset_id}' has no downloadable resources.",
        }

    if resource_index >= len(ds.resources):
        return {
            "success": False,
            "error": f"Resource index {resource_index} out of range. Dataset has {len(ds.resources)} resources.",
        }

    resource = ds.resources[resource_index]

    try:
        content, filename, mime_type = await client.download_resource(
            resource.url, max_size_mb=max_size_mb
        )

        content_base64 = base64.b64encode(content).decode("utf-8")

        return {
            "success": True,
            "filename": filename,
            "content_base64": content_base64,
            "size_bytes": len(content),
            "mime_type": mime_type,
            "dataset_title": ds.title,
            "resource_name": resource.name,
        }

    except ValueError as e:
        return {"success": False, "error": str(e)}
    except PermissionError as e:
        return {
            "success": False,
            "error": f"Access denied (403): {str(e)}",
            "resource_url": resource.url,
        }
    except Exception as e:
        return {"success": False, "error": f"Download failed: {str(e)}"}
