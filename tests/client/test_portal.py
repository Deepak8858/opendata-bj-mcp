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
async def test_get_all_datasets_with_pagination():
    """Test that get_all_datasets correctly passes offset parameter."""
    client = BeninPortalClient()
    
    # First page (offset=0)
    mock_page1 = {
        "datasets": [
            {"id": "ds1", "name": "dataset-1", "title": "Dataset 1", "organization": "Org1", "resources": []},
            {"id": "ds2", "name": "dataset-2", "title": "Dataset 2", "organization": "Org1", "resources": []},
        ]
    }
    
    # Second page (offset=2)
    mock_page2 = {
        "datasets": [
            {"id": "ds3", "name": "dataset-3", "title": "Dataset 3", "organization": "Org2", "resources": []},
        ]
    }

    with respx.mock:
        # Mock first page request
        route1 = respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all",
            params={"format": "json", "limit": 2, "offset": 0}
        ).mock(return_value=Response(200, json=mock_page1))
        
        # Mock second page request
        route2 = respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all",
            params={"format": "json", "limit": 2, "offset": 2}
        ).mock(return_value=Response(200, json=mock_page2))

        # Fetch first page
        datasets_page1 = await client.get_all_datasets(limit=2, offset=0)
        assert len(datasets_page1) == 2
        assert datasets_page1[0].id == "ds1"
        assert datasets_page1[1].id == "ds2"
        
        # Fetch second page
        datasets_page2 = await client.get_all_datasets(limit=2, offset=2)
        assert len(datasets_page2) == 1
        assert datasets_page2[0].id == "ds3"
        
        # Verify both endpoints were called
        assert route1.called
        assert route2.called

    await client.close()


@pytest.mark.asyncio
async def test_iter_all_datasets():
    """Test the async iterator for automatic pagination."""
    client = BeninPortalClient()
    
    # Simulate 3 pages of results
    mock_page1 = {
        "datasets": [
            {"id": "ds1", "name": "dataset-1", "title": "Dataset 1", "organization": "Org1", "resources": []},
        ]
    }
    mock_page2 = {
        "datasets": [
            {"id": "ds2", "name": "dataset-2", "title": "Dataset 2", "organization": "Org1", "resources": []},
        ]
    }
    mock_page3 = {"datasets": []}  # Empty page signals end

    with respx.mock:
        # Mock paginated requests
        route1 = respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all",
            params={"format": "json", "limit": 1, "offset": 0}
        ).mock(return_value=Response(200, json=mock_page1))
        
        route2 = respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all",
            params={"format": "json", "limit": 1, "offset": 1}
        ).mock(return_value=Response(200, json=mock_page2))
        
        route3 = respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all",
            params={"format": "json", "limit": 1, "offset": 2}
        ).mock(return_value=Response(200, json=mock_page3))

        # Collect all datasets via iterator
        results = []
        async for dataset in client.iter_all_datasets(batch_size=1):
            results.append(dataset)
        
        assert len(results) == 2
        assert results[0].id == "ds1"
        assert results[1].id == "ds2"
        
        # Verify all pages were fetched
        assert route1.called
        assert route2.called
        assert route3.called

    await client.close()


@pytest.mark.asyncio
async def test_iter_all_datasets_with_query():
    """Test the iterator with a search query."""
    client = BeninPortalClient()
    
    mock_data = {
        "datasets": [
            {"id": "health1", "name": "health-data", "title": "Health Data", "organization": "Ministry", "resources": []},
        ]
    }

    with respx.mock:
        route = respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all",
            params={"format": "json", "limit": 100, "offset": 0, "q": "health"}
        ).mock(return_value=Response(200, json=mock_data))

        results = []
        async for dataset in client.iter_all_datasets(query="health"):
            results.append(dataset)
        
        assert len(results) == 1
        assert results[0].id == "health1"
        assert route.called

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


# Cache-related tests

@pytest.mark.asyncio
async def test_cache_hit_avoids_api_call():
    """Test that cached results avoid unnecessary API calls."""
    client = BeninPortalClient(enable_cache=True, cache_max_size=100)
    mock_data = {
        "datasets": [
            {"id": "cached_ds", "name": "cached-dataset", "title": "Cached Dataset", "organization": "TestOrg", "resources": []}
        ]
    }

    with respx.mock:
        route = respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all",
            params={"format": "json", "limit": 10, "offset": 0}
        ).mock(return_value=Response(200, json=mock_data))

        # First call - should hit API
        result1 = await client.get_all_datasets(limit=10, offset=0)
        assert result1[0].id == "cached_ds"
        assert route.call_count == 1

        # Second call - should use cache
        result2 = await client.get_all_datasets(limit=10, offset=0)
        assert result2[0].id == "cached_ds"
        # API should not be called again
        assert route.call_count == 1

    await client.close()


@pytest.mark.asyncio
async def test_cache_disabled_makes_multiple_calls():
    """Test that disabled cache results in multiple API calls."""
    client = BeninPortalClient(enable_cache=False, cache_max_size=100)
    mock_data = {
        "datasets": [
            {"id": "ds", "name": "dataset", "title": "Dataset", "organization": "Org", "resources": []}
        ]
    }

    with respx.mock:
        route = respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all",
            params={"format": "json", "limit": 10, "offset": 0}
        ).mock(return_value=Response(200, json=mock_data))

        # Multiple calls should all hit API when cache is disabled
        await client.get_all_datasets(limit=10, offset=0)
        await client.get_all_datasets(limit=10, offset=0)
        await client.get_all_datasets(limit=10, offset=0)
        
        assert route.call_count == 3

    await client.close()


@pytest.mark.asyncio
async def test_clear_cache_async():
    """Test that clear_cache_async removes all cached data."""
    client = BeninPortalClient(enable_cache=True, cache_max_size=100)
    mock_data = {
        "datasets": [
            {"id": "ds", "name": "dataset", "title": "Dataset", "organization": "Org", "resources": []}
        ]
    }

    with respx.mock:
        route = respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all",
            params={"format": "json", "limit": 10, "offset": 0}
        ).mock(return_value=Response(200, json=mock_data))

        # First call - cache miss
        await client.get_all_datasets(limit=10, offset=0)
        assert route.call_count == 1

        # Clear cache
        await client.clear_cache_async()

        # Next call should hit API again
        await client.get_all_datasets(limit=10, offset=0)
        assert route.call_count == 2

    await client.close()


@pytest.mark.asyncio
async def test_get_cache_stats():
    """Test that cache stats are accurate."""
    client = BeninPortalClient(enable_cache=True, cache_max_size=100)
    mock_data = {"datasets": [{"id": "ds1", "name": "ds1", "title": "DS1", "organization": "Org", "resources": []}]}

    with respx.mock:
        respx.get("https://donneespubliques.gouv.bj/api/open/datasets/all").mock(
            return_value=Response(200, json=mock_data)
        )

        # Get initial stats
        stats = await client.get_cache_stats()
        assert stats["enabled"] is True
        assert stats["datasets"]["size"] == 0

        # Make a request
        await client.get_all_datasets()

        # Check updated stats
        stats = await client.get_cache_stats()
        assert stats["datasets"]["size"] == 1

    await client.close()


@pytest.mark.asyncio
async def test_dataset_details_caching():
    """Test that get_dataset_details uses cache correctly."""
    client = BeninPortalClient(enable_cache=True, cache_max_size=100)
    mock_data = {
        "datasets": [
            {"id": "specific_id", "name": "dataset", "title": "Dataset", "organization": "Org", "resources": []}
        ]
    }

    with respx.mock:
        route = respx.get(
            "https://donneespubliques.gouv.bj/api/open/datasets/all",
            params={"format": "json", "q": "specific_id", "limit": 100}
        ).mock(return_value=Response(200, json=mock_data))

        # First call - should hit API
        result1 = await client.get_dataset_details("specific_id")
        assert result1.id == "specific_id"
        assert route.call_count == 1

        # Second call - should use cache
        result2 = await client.get_dataset_details("specific_id")
        assert result2.id == "specific_id"
        assert route.call_count == 1  # No additional API call

    await client.close()


@pytest.mark.asyncio
async def test_organizations_caching():
    """Test that get_organizations uses cache correctly."""
    client = BeninPortalClient(enable_cache=True, cache_max_size=100)
    mock_data = {
        "data": [
            {"name": "INSTAD", "title": "Institut"},
            {"name": "CDIJ", "title": "Centre"},
        ]
    }

    with respx.mock:
        route = respx.get("https://donneespubliques.gouv.bj/api/v1/organizations").mock(
            return_value=Response(200, json=mock_data)
        )

        # First call - should hit API
        result1 = await client.get_organizations()
        assert "INSTAD" in result1
        assert route.call_count == 1

        # Second call - should use cache
        result2 = await client.get_organizations()
        assert "INSTAD" in result2
        assert route.call_count == 1  # No additional API call

    await client.close()
