import json
from ..client.portal import BeninPortalClient

async def publier_datasets_bulk(client: BeninPortalClient, metadata_json: str) -> str:
    """Permet d'uploader des jeux de données en masse (bulk upload)."""
    try:
        data = json.loads(metadata_json)
    except json.JSONDecodeError:
        return "Erreur : Le format JSON est invalide."
        
    result = await client.bulk_upload(data, [])
    
    if result.get("success"):
        return f"Succès ! {result.get('uploaded_count', 0)} datasets ont été uploadés."
    else:
        return f"Erreur lors de l'upload : {result.get('errors')}"
