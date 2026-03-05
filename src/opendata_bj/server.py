import os
from typing import Optional, List
from fastmcp import FastMCP
from .client.portal import BeninPortalClient

# Initialisation du serveur FastMCP
mcp = FastMCP("opendata-bj")

# Client asynchrone (singleton simplifié pour le serveur)
_client: Optional[BeninPortalClient] = None

async def get_client() -> BeninPortalClient:
    global _client
    if _client is None:
        api_key = os.getenv("BENIN_OPEN_DATA_API_KEY")
        _client = BeninPortalClient(api_key=api_key)
    return _client

@mcp.tool()
async def rechercher_datasets(query: Optional[str] = None, limit: int = 10) -> str:
    """
    Recherche des jeux de données publiques du Bénin.
    
    Args:
        query: Mots-clés de recherche (ex: 'santé', 'éducation').
        limit: Nombre maximum de résultats.
    """
    client = await get_client()
    datasets = await client.get_all_datasets(query=query, limit=limit)
    
    if not datasets:
        return "Aucun jeu de données trouvé pour cette recherche."
    
    res = []
    for ds in datasets:
        res.append(f"- [{ds.id}] {ds.title} (Organisation: {ds.organization})")
    
    return "\n".join(res)

@mcp.tool()
async def consulter_dataset(dataset_id: str) -> str:
    """
    Récupère les détails et les ressources d'un dataset spécifique.
    
    Args:
        dataset_id: L'identifiant unique du dataset (ex: 'vzedhob').
    """
    client = await get_client()
    ds = await client.get_dataset_details(dataset_id)
    
    if not ds:
        return f"Jeu de données '{dataset_id}' introuvable."
    
    output = [
        f"# {ds.title}",
        f"**Description**: {ds.description}",
        f"**Organisation**: {ds.organization}",
        f"**Tags**: {', '.join(ds.tags)}",
        "\n## Ressources disponibles:"
    ]
    
    for res in ds.resources:
        output.append(f"- {res.name} ({res.format}) : {res.url}")
        
    return "\n".join(output)

if __name__ == "__main__":
    mcp.run()
