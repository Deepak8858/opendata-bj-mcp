import pytest
import respx
from httpx import Response
from opendata_bj.server import search_datasets, list_organizations

@pytest.mark.asyncio
async def test_search_datasets_tool():
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
        
        result = await search_datasets(query="test")
        assert "Dataset 1" in result
        assert "Org A" in result

@pytest.mark.asyncio
async def test_list_organizations_tool():
    mock_data = {"organisations": ["INSTAD", "CDIJ"]}
    
    with respx.mock:
        respx.get("https://donneespubliques.gouv.bj/api/open/organisations/all").mock(
            return_value=Response(200, json=mock_data)
        )
        
        # Note: the client was using /api/open/organisations/all, 
        # I should double check the client implementation
        
        result = await list_organizations()
        assert "INSTAD" in result
        assert "CDIJ" in result
