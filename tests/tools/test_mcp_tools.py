import pytest
import respx
from httpx import Response
from opendata_bj.server import search_datasets, list_organizations


@pytest.mark.asyncio
async def test_search_datasets_tool():
    mock_data = {
        "datasets": [
            {
                "id": "ds1",
                "title": "Dataset 1",
                "organization": "Org A",
                "resources": [],
                "name": "ds1",
            }
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
async def test_search_datasets_tool_with_pagination():
    """Test search_datasets tool with offset parameter for pagination."""
    mock_data = {
        "datasets": [
            {
                "id": "ds5",
                "title": "Dataset Page 2",
                "organization": "Org B",
                "resources": [],
                "name": "ds5",
            }
        ]
    }

    with respx.mock:
        # Verify that offset parameter is passed correctly
        route = respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all",
            params={"format": "json", "limit": 5, "offset": 10}
        ).mock(return_value=Response(200, json=mock_data))

        result = await search_datasets(query=None, limit=5, offset=10)
        assert "Dataset Page 2" in result
        assert "Org B" in result
        assert route.called


@pytest.mark.asyncio
async def test_search_datasets_tool_empty_results():
    """Test search_datasets tool when no results are found."""
    mock_data = {"datasets": []}

    with respx.mock:
        respx.get("https://donneespubliques.gouv.bj/api/open/datasets/all").mock(
            return_value=Response(200, json=mock_data)
        )

        result = await search_datasets(query="nonexistent", limit=10, offset=0)
        assert "No datasets found" in result


@pytest.mark.asyncio
async def test_list_organizations_tool():
    """Test list_organizations tool with correct endpoint."""
    mock_data = {
        "data": [
            {"name": "INSTAD", "title": "Institut National de la Statistique"},
            {"name": "CDIJ", "title": "Centre de Documentation et d'Information"},
        ]
    }

    with respx.mock:
        respx.get("https://donneespubliques.gouv.bj/api/v1/organizations").mock(
            return_value=Response(200, json=mock_data)
        )

        result = await list_organizations()
        assert "INSTAD" in result
        assert "CDIJ" in result
