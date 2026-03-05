import httpx
from typing import List, Optional, Dict, Any
from ..models.dataset import Dataset

class BeninPortalClient:
    def __init__(self, base_url: str = "https://donneespubliques.gouv.bj", api_key: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.headers = {"X-API-Key": api_key} if api_key else {}
        self.client = httpx.AsyncClient(timeout=30.0, headers=self.headers)

    async def get_all_datasets(self, query: Optional[str] = None, limit: int = 10) -> List[Dataset]:
        """Récupère la liste des jeux de données."""
        url = f"{self.base_url}/api/open/datasets/all"
        params = {"q": query} if query else {}
        response = await self.client.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Mapping basé sur responses.txt : data["datasets"]
        return [Dataset(**ds) for ds in data.get("datasets", [])[:limit]]

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
        # Simulation simplifiée de l'upload de fichiers
        # Dans un vrai cas, on ouvrirait les fichiers
        response = await self.client.post(url, json=metadata)
        response.raise_for_status()
        return response.json()

    async def close(self):
        await self.client.aclose()
