"""Tests for admin tools."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from opendata_bj.tools.admin import publish_datasets_bulk


@pytest.mark.asyncio
async def test_publish_datasets_bulk_no_api_key():
    """Test that publish_datasets_bulk returns error when no API key is set."""
    # Create a mock client without API key
    mock_client = MagicMock()
    mock_client.api_key = None
    
    metadata_json = '[{"title": "Test Dataset"}]'
    result = await publish_datasets_bulk(mock_client, metadata_json)
    
    assert "API Key Required" in result
    assert "publish_datasets_bulk" in result
    assert "BENIN_OPEN_DATA_API_KEY" in result
    # Ensure bulk_upload was not called
    mock_client.bulk_upload.assert_not_called()


@pytest.mark.asyncio
async def test_publish_datasets_bulk_with_api_key():
    """Test that publish_datasets_bulk works when API key is set."""
    # Create a mock client with API key
    mock_client = MagicMock()
    mock_client.api_key = "test_api_key_123"
    mock_client.bulk_upload = AsyncMock(return_value={
        "success": True,
        "uploaded_count": 2
    })
    
    metadata_json = '[{"title": "Dataset 1"}, {"title": "Dataset 2"}]'
    result = await publish_datasets_bulk(mock_client, metadata_json)
    
    assert "Success" in result
    assert "2 datasets have been uploaded" in result
    mock_client.bulk_upload.assert_called_once()


@pytest.mark.asyncio
async def test_publish_datasets_bulk_invalid_json():
    """Test that publish_datasets_bulk handles invalid JSON."""
    mock_client = MagicMock()
    mock_client.api_key = "test_api_key_123"
    
    metadata_json = "not valid json{{"
    result = await publish_datasets_bulk(mock_client, metadata_json)
    
    assert "Invalid JSON format" in result
    mock_client.bulk_upload.assert_not_called()


@pytest.mark.asyncio
async def test_publish_datasets_bulk_upload_failure():
    """Test that publish_datasets_bulk handles upload failure."""
    mock_client = MagicMock()
    mock_client.api_key = "test_api_key_123"
    mock_client.bulk_upload = AsyncMock(return_value={
        "success": False,
        "errors": ["Invalid metadata format"]
    })
    
    metadata_json = '[{"title": "Test Dataset"}]'
    result = await publish_datasets_bulk(mock_client, metadata_json)
    
    assert "Error during upload" in result
    assert "Invalid metadata format" in result


@pytest.mark.asyncio
async def test_publish_datasets_bulk_exception():
    """Test that publish_datasets_bulk handles exceptions."""
    mock_client = MagicMock()
    mock_client.api_key = "test_api_key_123"
    mock_client.bulk_upload = AsyncMock(side_effect=Exception("Network error"))
    
    metadata_json = '[{"title": "Test Dataset"}]'
    result = await publish_datasets_bulk(mock_client, metadata_json)
    
    assert "Error during upload" in result
    assert "Network error" in result
