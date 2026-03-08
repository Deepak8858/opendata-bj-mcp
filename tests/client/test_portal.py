import pytest
import respx
from httpx import Response
from opendata_bj.client.portal import BeninPortalClient


@pytest.mark.asyncio
async def test_get_all_datasets_success():
    client = BeninPortalClient()
    mock_data = {
        "datasets": [
            {
                "id": "vzedhob",
                "name": "test-dataset",
                "title": "MICS_Santé",
                "organization": "INSTAD",
                "resources": [],
            }
        ]
    }

    with respx.mock:
        respx.get("https://donneespubliques.gouv.bj/api/open/datasets/all").mock(
            return_value=Response(200, json=mock_data)
        )

        datasets = await client.get_all_datasets()
        assert len(datasets) == 1
        assert datasets[0].id == "vzedhob"
        assert datasets[0].title == "MICS_Santé"

    await client.close()


@pytest.mark.asyncio
async def test_get_dataset_details_not_found():
    """Test that get_dataset_details returns None when dataset is not found."""
    client = BeninPortalClient()
    mock_data = {"datasets": []}

    with respx.mock:
        respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all",
            params={"format": "json", "q": "unknown", "limit": 100},
        ).mock(return_value=Response(200, json=mock_data))

        details = await client.get_dataset_details("unknown")
        assert details is None

    await client.close()
