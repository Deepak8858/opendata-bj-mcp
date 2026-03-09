import json
from opendata_bj.client.portal import BeninPortalClient


class APIKeyRequiredError(Exception):
    """Raised when an API key is required but not provided."""
    pass


async def publish_datasets_bulk(client: BeninPortalClient, metadata_json: str) -> str:
    """Upload datasets in bulk.
    
    This operation requires an API key with write permissions.
    
    Args:
        client: The BeninPortalClient instance
        metadata_json: JSON string containing the metadata for datasets to upload
        
    Returns:
        Success or error message
        
    Raises:
        APIKeyRequiredError: If no API key is configured
    """
    # Validate API key is present
    if not client.api_key:
        return (
            "❌ **API Key Required**\n\n"
            "The `publish_datasets_bulk` operation requires an API key with write permissions.\n\n"
            "**To fix this:**\n"
            "1. Obtain an API key from the Benin OpenData Portal administrators\n"
            "2. Set the `BENIN_OPEN_DATA_API_KEY` environment variable\n"
            "3. Restart the MCP server\n\n"
            "**Example:**\n"
            '```bash\n'
            'export BENIN_OPEN_DATA_API_KEY="your_api_key_here"\n'
            'fastmcp run src/opendata_bj/server.py\n'
            '```'
        )
    
    try:
        data = json.loads(metadata_json)
    except json.JSONDecodeError as e:
        return f"Error: Invalid JSON format - {str(e)}"
    
    try:
        result = await client.bulk_upload(data, [])
        
        if result.get("success"):
            return f"✅ Success! {result.get('uploaded_count', 0)} datasets have been uploaded."
        else:
            errors = result.get('errors', [])
            error_msg = f"Error during upload: {errors}"
            return error_msg
    except Exception as e:
        return f"Error during upload: {str(e)}"
