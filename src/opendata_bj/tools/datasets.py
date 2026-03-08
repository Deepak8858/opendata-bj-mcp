from typing import Optional, List
import os
import base64
from opendata_bj.client.portal import BeninPortalClient
from opendata_bj.models.dataset import Dataset

async def search_datasets(client: BeninPortalClient, query: Optional[str] = None, limit: int = 10) -> str:
    """Search for public datasets from Benin."""
    datasets = await client.get_all_datasets(query=query, limit=limit)
    
    if not datasets:
        return "No datasets found for this search."
    
    res = [f"- [{ds.id}] {ds.title} (Organization: {ds.organization})" for ds in datasets]
    return "\n".join(res)

async def get_dataset(client: BeninPortalClient, dataset_id: str) -> str:
    """Retrieve details and resources for a specific dataset."""
    ds = await client.get_dataset_details(dataset_id)
    
    if not ds:
        return f"Dataset '{dataset_id}' not found."
    
    output = [
        f"# {ds.title}",
        f"**Description**: {ds.description}",
        f"**Organization**: {ds.organization}",
        f"**Tags**: {', '.join(ds.tags)}",
        "\n## Available Resources:"
    ]
    
    for res in ds.resources:
        output.append(f"- {res.name} ({res.format}) : {res.url}")
        
    return "\n".join(output)

async def list_organizations(client: BeninPortalClient) -> str:
    """List institutions that publish data on the portal."""
    orgs = await client.get_organizations()
    if not orgs:
        return "No organizations found."
    return "Available Organizations:\n" + "\n".join([f"- {org}" for org in orgs])


async def preview_dataset(
    client: BeninPortalClient, 
    dataset_id: str, 
    resource_index: int = 0, 
    rows: int = 10
) -> str:
    """
    Preview the first rows of a dataset resource.
    
    Args:
        dataset_id: The ID of the dataset
        resource_index: Index of the resource to preview (default: 0)
        rows: Number of rows to preview (default: 10, max: 50)
    """
    # Security: limit rows
    rows = min(max(rows, 1), 50)
    
    # Get dataset details
    ds = await client.get_dataset_details(dataset_id)
    if not ds:
        return f"Dataset '{dataset_id}' not found."
    
    if not ds.resources:
        return f"Dataset '{dataset_id}' has no downloadable resources."
    
    if resource_index >= len(ds.resources):
        return f"Resource index {resource_index} out of range. Dataset has {len(ds.resources)} resources."
    
    resource = ds.resources[resource_index]
    
    try:
        headers, data_rows = await client.get_resource_preview(resource.url, max_rows=rows)
        
        if not headers:
            return f"Could not parse preview for resource '{resource.name}'. Format might not be supported."
        
        # Format output as markdown table
        output = [
            f"# Preview: {resource.name}",
            f"**Format**: {resource.format}",
            f"**URL**: {resource.url}",
            f"\nShowing first {len(data_rows)} row(s):\n"
        ]
        
        # Build markdown table
        header_line = "| " + " | ".join(headers) + " |"
        separator = "|" + "|".join([" --- " for _ in headers]) + "|"
        
        output.append(header_line)
        output.append(separator)
        
        for row in data_rows:
            # Pad row to match headers length
            padded_row = row + [""] * (len(headers) - len(row))
            output.append("| " + " | ".join(padded_row) + " |")
        
        return "\n".join(output)
        
    except Exception as e:
        return f"Error previewing resource: {str(e)}"


async def download_dataset(
    client: BeninPortalClient,
    dataset_id: str,
    resource_index: int = 0,
    max_size_mb: int = 10
) -> dict:
    """
    Download a dataset resource and return it as base64-encoded content.
    
    Args:
        dataset_id: The ID of the dataset
        resource_index: Index of the resource to download (default: 0)
        max_size_mb: Maximum file size in MB (default: 10, max: 50)
        
    Returns:
        dict with filename, content_base64, size_bytes, mime_type
    """
    # Security: limit file size
    max_size_mb = min(max(max_size_mb, 1), 50)
    
    # Get dataset details
    ds = await client.get_dataset_details(dataset_id)
    if not ds:
        return {
            "success": False,
            "error": f"Dataset '{dataset_id}' not found."
        }
    
    if not ds.resources:
        return {
            "success": False,
            "error": f"Dataset '{dataset_id}' has no downloadable resources."
        }
    
    if resource_index >= len(ds.resources):
        return {
            "success": False,
            "error": f"Resource index {resource_index} out of range. Dataset has {len(ds.resources)} resources."
        }
    
    resource = ds.resources[resource_index]
    
    try:
        content, filename, mime_type = await client.download_resource(
            resource.url, 
            max_size_mb=max_size_mb
        )
        
        # Encode to base64
        content_base64 = base64.b64encode(content).decode('utf-8')
        
        return {
            "success": True,
            "filename": filename,
            "content_base64": content_base64,
            "size_bytes": len(content),
            "mime_type": mime_type,
            "dataset_title": ds.title,
            "resource_name": resource.name
        }
        
    except ValueError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Download failed: {str(e)}"
        }

