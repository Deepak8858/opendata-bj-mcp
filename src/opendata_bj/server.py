import os
from typing import Optional
from fastmcp import FastMCP
from .client.portal import BeninPortalClient
from .tools import datasets, admin

# Initialisation du serveur FastMCP
mcp = FastMCP("opendata-bj")

# Client asynchrone singleton
_client: Optional[BeninPortalClient] = None

async def get_client() -> BeninPortalClient:
    global _client
    if _client is None:
        api_key = os.getenv("BENIN_OPEN_DATA_API_KEY")
        _client = BeninPortalClient(api_key=api_key)
    return _client

@mcp.tool()
async def rechercher_datasets(query: Optional[str] = None, limit: int = 10) -> str:
    """Recherche des jeux de données publiques du Bénin."""
    client = await get_client()
    return await datasets.rechercher_datasets(client, query, limit)

@mcp.tool()
async def consulter_dataset(dataset_id: str) -> str:
    """Récupère les détails d'un dataset spécifique."""
    client = await get_client()
    return await datasets.consulter_dataset(client, dataset_id)

@mcp.tool()
async def lister_organisations() -> str:
    """Donne la liste des institutions qui publient des données."""
    client = await get_client()
    return await datasets.lister_organisations(client)

@mcp.tool()
async def publier_datasets_bulk(metadata_json: str) -> str:
    """Upload de jeux de données en masse."""
    client = await get_client()
    return await admin.publier_datasets_bulk(client, metadata_json)

if __name__ == "__main__":
    mcp.run()
