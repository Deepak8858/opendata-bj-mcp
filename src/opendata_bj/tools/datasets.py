from typing import Optional, List
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
