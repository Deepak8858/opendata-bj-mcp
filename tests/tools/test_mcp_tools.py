import pytest
import respx
from httpx import Response
from opendata_bj.server import rechercher_datasets, lister_organisations

@pytest.mark.asyncio
async def test_rechercher_datasets_tool():
    mock_data = {
        "success": True,
        "datasets": [
            {"id": "ds1", "title": "Dataset 1", "organization": "Org A", "resources": [], "name": "ds1"}
        ]
    }
    
    with respx.mock:
        respx.get("https://donneespubliques.gouv.bj/api/open/datasets/all").mock(
            return_value=Response(200, json=mock_data)
        )
        
        result = await rechercher_datasets(query="test")
        assert "Dataset 1" in result
        assert "Org A" in result

@pytest.mark.asyncio
async def test_lister_organisations_tool():
    mock_data = {"organisations": ["INSTAD", "CDIJ"]}
    
    with respx.mock:
        respx.get("https://donneespubliques.gouv.bj/api/open/organisations/all").mock(
            return_value=Response(200, json=mock_data)
        )
        
        result = await lister_organisations()
        assert "INSTAD" in result
        assert "CDIJ" in result
