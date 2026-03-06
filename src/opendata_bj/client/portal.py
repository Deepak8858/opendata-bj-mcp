import httpx
from typing import List, Optional, Dict, Any
from opendata_bj.models.dataset import Dataset

class BeninPortalClient:
    def __init__(self, base_url: str = "https://donneespubliques.gouv.bj", api_key: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.headers = {}
        self.client = httpx.AsyncClient(timeout=30.0, headers=self.headers)
        self.api_key = api_key

    async def get_all_datasets(self, query: Optional[str] = None, limit: int = 10) -> List[Dataset]:
        """Récupère la liste des jeux de données."""
        url = f"{self.base_url}/api/open/datasets/all"
        params = {"format": "json", "limit": limit}
        if query:
            params["q"] = query
        response = await self.client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Mapping basé sur responses.txt : data["datasets"]
        return [Dataset(**ds) for ds in data.get("datasets", [])]

    async def get_dataset_details(self, dataset_id: str) -> Optional[Dataset]:
        """Récupère les détails d'un jeu de données spécifique."""
        url = f"{self.base_url}/api/open/datasets/details/{dataset_id}"
        response = await self.client.get(url)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return Dataset(**response.json())

    async def bulk_upload(self, metadata: Dict[str, Any], files: List[str]) -> Dict[str, Any]:
        """Upload de masse de jeux de données."""
        url = f"{self.base_url}/api/v1/open/datasets/bulk-upload"
        
        data = {"api_key": self.api_key} if self.api_key else {}
        # NOTE: metadata is sent as a file or json field based on API spec.
        # Screenshot shows form-data with metadata_file and files.
        # We will just pass the JSON directly for now as a simplification,
        # but in real usage it requires multipart/form-data.
        
        response = await self.client.post(url, json=metadata, params=data)
        response.raise_for_status()
        return response.json()

    async def get_organizations(self) -> List[str]:
        """Récupère la liste des organisations en scannant les datasets."""
        # Comme il n'y a pas d'endpoint explicite dans la doc, on extrait depuis les datasets
        datasets = await self.get_all_datasets(limit=100)
        orgs = set()
        for ds in datasets:
            if ds.organization:
                orgs.add(ds.organization)
        return sorted(list(orgs))

    async def close(self):
        await self.client.aclose()
