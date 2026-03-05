from typing import Optional, List
from ..client.portal import BeninPortalClient
from ..models.dataset import Dataset

async def rechercher_datasets(client: BeninPortalClient, query: Optional[str] = None, limit: int = 10) -> str:
    """Recherche des jeux de données publiques du Bénin."""
    datasets = await client.get_all_datasets(query=query, limit=limit)
    
    if not datasets:
        return "Aucun jeu de données trouvé pour cette recherche."
    
    res = [f"- [{ds.id}] {ds.title} (Organisation: {ds.organization})" for ds in datasets]
    return "\n".join(res)

async def consulter_dataset(client: BeninPortalClient, dataset_id: str) -> str:
    """Récupère les détails et les ressources d'un dataset spécifique."""
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

async def lister_organisations(client: BeninPortalClient) -> str:
    """Donne la liste des institutions qui publient des données."""
    orgs = await client.get_organizations()
    if not orgs:
        return "Aucune organisation trouvée."
    return "Organisations disponibles :\n" + "\n".join([f"- {org}" for org in orgs])
