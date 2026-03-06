import json
from ..client.portal import BeninPortalClient

async def publish_datasets_bulk(client: BeninPortalClient, metadata_json: str) -> str:
    """Upload datasets in bulk."""
    try:
        data = json.loads(metadata_json)
    except json.JSONDecodeError:
        return "Error: Invalid JSON format."
        
    result = await client.bulk_upload(data, [])
    
    if result.get("success"):
        return f"Success! {result.get('uploaded_count', 0)} datasets have been uploaded."
    else:
        return f"Error during upload: {result.get('errors')}"
