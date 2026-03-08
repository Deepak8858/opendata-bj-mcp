import httpx
import base64
from typing import List, Optional, Dict, Any, Tuple
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
        # Comme l'endpoint details semble absent, on le recherche via l'endpoint de recherche
        url = f"{self.base_url}/api/open/datasets/all"
        params = {"format": "json", "q": dataset_id, "limit": 100}
        response = await self.client.get(url, params=params)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.json()
        for ds in data.get("datasets", []):
            if ds.get("id") == dataset_id or ds.get("dataset_id") == dataset_id:
                return Dataset(**ds)
        return None

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
        """Récupère la liste complète des organisations."""
        url = f"{self.base_url}/api/v1/organizations"
        response = await self.client.get(url)
        
        if response.status_code == 404:
            # Fallback en scannant les datasets
            datasets = await self.get_all_datasets(limit=100)
            orgs = set()
            for ds in datasets:
                if ds.organization:
                    orgs.add(ds.organization)
            return sorted(list(orgs))
            
        response.raise_for_status()
        data = response.json()
        orgs = []
        for org in data.get("data", []):
            name = org.get("name") or org.get("title")
            if name:
                orgs.append(name)
        return orgs

    async def close(self):
        await self.client.aclose()

    async def get_resource_preview(self, resource_url: str, max_rows: int = 10, max_bytes: int = 65536) -> Tuple[List[str], List[List[str]]]:
        """
        Récupère un aperçu d'une ressource (CSV ou autre) via streaming partiel.
        
        Args:
            resource_url: URL de la ressource
            max_rows: Nombre maximum de lignes à retourner
            max_bytes: Limite de bytes à télécharger pour le preview
            
        Returns:
            Tuple (headers, rows) où headers est la liste des colonnes
            et rows est une liste de listes (les données)
        """
        try:
            # Utilise Range header pour ne prendre que le début du fichier
            headers = {"Range": f"bytes=0-{max_bytes}"}
            response = await self.client.get(resource_url, headers=headers, follow_redirects=True)
            
            # Si Range n'est pas supporté, on prend tout mais on limite le parsing
            if response.status_code == 416:  # Range not satisfiable
                response = await self.client.get(resource_url, follow_redirects=True)
            else:
                response.raise_for_status()
            
            content = response.content.decode('utf-8', errors='replace')
            lines = content.split('\n')[:max_rows + 1]  # +1 pour le header
            
            if not lines:
                return [], []
            
            # Parsing simple CSV
            import csv
            from io import StringIO
            
            reader = csv.reader(StringIO('\n'.join(lines)))
            rows = list(reader)
            
            if not rows:
                return [], []
                
            headers = rows[0]
            data_rows = rows[1:max_rows + 1]
            
            return headers, data_rows
            
        except Exception as e:
            raise Exception(f"Failed to preview resource: {str(e)}")

    async def download_resource(self, resource_url: str, max_size_mb: int = 10) -> Tuple[bytes, str, str]:
        """
        Télécharge une ressource et retourne son contenu en bytes.
        
        Args:
            resource_url: URL de la ressource
            max_size_mb: Taille maximale en MB (sécurité)
            
        Returns:
            Tuple (content_bytes, filename, mime_type)
        """
        try:
            max_bytes = max_size_mb * 1024 * 1024
            
            # Streaming download avec vérification de taille
            async with self.client.stream("GET", resource_url, follow_redirects=True) as response:
                response.raise_for_status()
                
                # Vérifie Content-Length si disponible
                content_length = response.headers.get('content-length')
                if content_length and int(content_length) > max_bytes:
                    raise ValueError(f"File too large: {int(content_length) / 1024 / 1024:.2f}MB > {max_size_mb}MB limit")
                
                chunks = []
                total_size = 0
                
                async for chunk in response.aiter_bytes(chunk_size=8192):
                    chunks.append(chunk)
                    total_size += len(chunk)
                    
                    if total_size > max_bytes:
                        raise ValueError(f"File exceeds {max_size_mb}MB limit")
                
                content = b''.join(chunks)
                
            # Détermine le filename depuis l'URL ou Content-Disposition
            from urllib.parse import urlparse
            parsed = urlparse(resource_url)
            filename = parsed.path.split('/')[-1] or 'download'
            
            # Content-Disposition header
            content_disp = response.headers.get('content-disposition', '')
            if 'filename=' in content_disp:
                filename = content_disp.split('filename=')[-1].strip('"\'')
            
            mime_type = response.headers.get('content-type', 'application/octet-stream')
            
            return content, filename, mime_type
            
        except Exception as e:
            raise Exception(f"Failed to download resource: {str(e)}")
