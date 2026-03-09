from typing import Optional
import base64

from opendata_bj.client.portal import BeninPortalClient
from opendata_bj.tools.preview_handlers import get_handler, get_supported_formats
from opendata_bj.config import (
    MAX_PREVIEW_ROWS,
    DEFAULT_PREVIEW_ROWS,
    MAX_DOWNLOAD_SIZE_MB,
    DEFAULT_DOWNLOAD_SIZE_MB,
    DEFAULT_BASE_URL,
)

# Size threshold for auto mode (1MB)
AUTO_MODE_SIZE_THRESHOLD = 1 * 1024 * 1024


def get_full_resource_url(resource) -> str:
    """Convert resource URL to full URL.
    
    Handles relative URLs by adding the domain,
    and returns external URLs as-is.
    
    Args:
        resource: Resource object with url attribute
        
    Returns:
        Complete URL string
    """
    url = resource.url
    
    # Relative URL → add domain
    if url.startswith('/'):
        return f"{DEFAULT_BASE_URL}{url}"
    
    # External URL → return as-is
    return url


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
    """Preview a dataset resource.
    
    Args:
        client: The BeninPortalClient instance
        dataset_id: ID of the dataset to preview
        resource_index: Index of the resource within the dataset
        rows: Number of rows to preview
        
    Returns:
        Formatted preview string or error message
    """
    rows = min(max(rows, 1), MAX_PREVIEW_ROWS)

    ds = await client.get_dataset_details(dataset_id)
    if not ds:
        return f"Dataset '{dataset_id}' not found."

    if not ds.resources:
        return f"Dataset '{dataset_id}' has no downloadable resources."

    if resource_index >= len(ds.resources):
        return f"Resource index {resource_index} out of range. Dataset has {len(ds.resources)} resources."

    resource = ds.resources[resource_index]

    # Get the appropriate handler for this format
    handler = get_handler(resource.format, resource.mimetype)
    supported_formats = get_supported_formats()
    
    if handler is None:
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
        # Download content for preview (limit to preview bytes)
        full_url = get_full_resource_url(resource)
        content, _, _ = await client.download_resource(
            full_url, max_size_mb=10  # Limit preview to 10MB max
        )
        
        # Use the handler to extract preview data
        headers, data_rows = await handler.preview(content, max_rows=rows)

        if not headers and not data_rows:
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

        if headers:
            header_line = "| " + " | ".join(headers) + " |"
            separator = "|" + "|".join([" --- " for _ in headers]) + "|"
            output.extend([header_line, separator])

        for row in data_rows:
            if headers:
                padded_row = row + [""] * (len(headers) - len(row))
            else:
                padded_row = row
            output.append("| " + " | ".join(padded_row) + " |")

        return "\n".join(output)

    except PermissionError as e:
        return f"❌ Access denied: {str(e)}"
    except ImportError as e:
        return (
            f"⚠️ Preview unavailable for '{resource.name}'\n"
            f"**Format**: {resource.format}\n\n"
            f"Missing dependency: {str(e)}\n\n"
            f"Try downloading the file instead:\n"
            f"`download_dataset(dataset_id='{dataset_id}', resource_index={resource_index})`"
        )
    except Exception as e:
        return f"Error previewing resource: {str(e)}"


async def download_dataset(
    client: BeninPortalClient,
    dataset_id: str,
    resource_index: int = 0,
    max_size_mb: int = DEFAULT_DOWNLOAD_SIZE_MB,
    method: str = "auto",
) -> dict:
    """Download a dataset resource with adaptive method selection.
    
    This function intelligently chooses between returning a direct download URL
    or the file content as base64, based on file size and format.
    
    Args:
        client: The BeninPortalClient instance
        dataset_id: ID of the dataset to download from
        resource_index: Index of the resource within the dataset (default: 0)
        max_size_mb: Maximum file size in MB (1-50, default: 10)
        method: Download method - "auto" (default), "url", or "content"
            - "auto": Returns base64 if < 1MB, URL if >= 1MB or HTML format
            - "url": Always returns the direct download URL
            - "content": Always downloads and returns base64 content
            
    Returns:
        Dictionary with success status and either:
        - method="url": download_url, filename, format
        - method="content": content_base64, filename, size_bytes, mime_type
        - error: error message and suggestions
    """
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
    full_url = get_full_resource_url(resource)
    
    # Case 3: HTML format - cannot download directly
    if resource.format.upper() == "HTML":
        return {
            "success": False,
            "error": "Cannot download HTML resource directly",
            "format": "HTML",
            "resource_url": full_url,
            "suggestion": "This is an embeddable web page, not a downloadable file.",
            "alternative": "Use preview_dataset to extract data from the HTML page",
        }
    
    # Case 1: URL mode explicitly requested
    if method == "url":
        return {
            "success": True,
            "method": "url",
            "download_url": full_url,
            "filename": resource.name or "download",
            "format": resource.format,
            "size_bytes": None,
            "note": f"📥 Download the file directly from: {full_url}",
        }
    
    # Case 2: Download content (auto or content mode)
    try:
        content, filename, mime_type = await client.download_resource(
            full_url, max_size_mb=max_size_mb
        )
        size_bytes = len(content)
        
        # Check if we should return URL instead (auto mode + large file)
        if method == "auto" and size_bytes >= AUTO_MODE_SIZE_THRESHOLD:
            return {
                "success": True,
                "method": "url",
                "download_url": full_url,
                "filename": filename,
                "format": resource.format,
                "size_bytes": size_bytes,
                "note": f"📥 File is large ({size_bytes / 1024 / 1024:.1f}MB). Download from URL: {full_url}",
            }
        
        # Return content as base64
        content_base64 = base64.b64encode(content).decode("utf-8")
        
        return {
            "success": True,
            "method": "content",
            "content_base64": content_base64,
            "filename": filename,
            "size_bytes": size_bytes,
            "mime_type": mime_type,
            "format": resource.format,
        }

    except ValueError as e:
        return {"success": False, "error": str(e), "resource_url": full_url}
    except PermissionError as e:
        return {
            "success": False,
            "error": f"Access denied (403): {str(e)}",
            "resource_url": full_url,
        }
    except Exception as e:
        return {"success": False, "error": f"Download failed: {str(e)}", "resource_url": full_url}
