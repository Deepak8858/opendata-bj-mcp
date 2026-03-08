import pytest
import base64
import respx
from httpx import Response
from opendata_bj.server import preview_dataset, download_dataset
from opendata_bj.client.portal import BeninPortalClient


@pytest.mark.asyncio
async def test_preview_dataset_success():
    """Test previewing a CSV resource successfully."""
    # Mock dataset details
    mock_dataset = {
        "datasets": [{
            "id": "test-ds",
            "name": "test-dataset",
            "title": "Test Dataset",
            "organization": "Test Org",
            "resources": [{
                "id": "res-1",
                "name": "data.csv",
                "url": "https://example.com/data.csv",
                "format": "CSV",
                "package_id": "test-ds"
            }],
            "tags": [],
            "description": ""
        }]
    }
    
    # Mock CSV content
    csv_content = b"col1,col2,col3\nvalue1,value2,value3\nvalue4,value5,value6"
    
    with respx.mock:
        # Mock dataset API
        respx.get("https://donneespubliques.gouv.bj/api/open/datasets/all").mock(
            return_value=Response(200, json=mock_dataset)
        )
        # Mock resource download
        respx.get("https://example.com/data.csv").mock(
            return_value=Response(200, content=csv_content, headers={
                "Content-Type": "text/csv"
            })
        )
        
        result = await preview_dataset("test-ds", resource_index=0, rows=2)
        
        assert "data.csv" in result  # Resource name, not dataset title
        assert "col1" in result
        assert "value1" in result
        assert "|" in result  # Markdown table format


@pytest.mark.asyncio
async def test_preview_dataset_not_found():
    """Test previewing a non-existent dataset."""
    mock_empty = {"datasets": []}
    
    with respx.mock:
        respx.get("https://donneespubliques.gouv.bj/api/open/datasets/all").mock(
            return_value=Response(200, json=mock_empty)
        )
        
        result = await preview_dataset("nonexistent")
        
        assert "not found" in result


@pytest.mark.asyncio
async def test_preview_dataset_no_resources():
    """Test previewing a dataset with no resources."""
    mock_dataset = {
        "datasets": [{
            "id": "test-ds",
            "name": "test-dataset",
            "title": "Test Dataset",
            "organization": "Test Org",
            "resources": [],
            "tags": [],
            "description": ""
        }]
    }
    
    with respx.mock:
        respx.get("https://donneespubliques.gouv.bj/api/open/datasets/all").mock(
            return_value=Response(200, json=mock_dataset)
        )
        
        result = await preview_dataset("test-ds")
        
        assert "no downloadable resources" in result


@pytest.mark.asyncio
async def test_preview_dataset_invalid_resource_index():
    """Test previewing with an out-of-range resource index."""
    mock_dataset = {
        "datasets": [{
            "id": "test-ds",
            "name": "test-dataset",
            "title": "Test Dataset",
            "organization": "Test Org",
            "resources": [{
                "id": "res-1",
                "name": "data.csv",
                "url": "https://example.com/data.csv",
                "format": "CSV",
                "package_id": "test-ds"
            }],
            "tags": [],
            "description": ""
        }]
    }
    
    with respx.mock:
        respx.get("https://donneespubliques.gouv.bj/api/open/datasets/all").mock(
            return_value=Response(200, json=mock_dataset)
        )
        
        result = await preview_dataset("test-ds", resource_index=5)
        
        assert "out of range" in result


@pytest.mark.asyncio
async def test_download_dataset_success():
    """Test downloading a resource as base64 successfully."""
    mock_dataset = {
        "datasets": [{
            "id": "test-ds",
            "name": "test-dataset",
            "title": "Test Dataset",
            "organization": "Test Org",
            "resources": [{
                "id": "res-1",
                "name": "data.csv",
                "url": "https://example.com/data.csv",
                "format": "CSV",
                "package_id": "test-ds"
            }],
            "tags": [],
            "description": ""
        }]
    }
    
    file_content = b"col1,col2\nvalue1,value2"
    expected_base64 = base64.b64encode(file_content).decode()
    
    with respx.mock:
        respx.get("https://donneespubliques.gouv.bj/api/open/datasets/all").mock(
            return_value=Response(200, json=mock_dataset)
        )
        respx.get("https://example.com/data.csv").mock(
            return_value=Response(200, content=file_content, headers={
                "Content-Type": "text/csv",
                "Content-Length": str(len(file_content))
            })
        )
        
        result = await download_dataset("test-ds", resource_index=0, max_size_mb=10)
        
        assert result["success"] is True
        assert result["filename"] == "data.csv"
        assert result["content_base64"] == expected_base64
        assert result["size_bytes"] == len(file_content)
        assert result["mime_type"] == "text/csv"
        assert result["dataset_title"] == "Test Dataset"


@pytest.mark.asyncio
async def test_download_dataset_not_found():
    """Test downloading from a non-existent dataset."""
    mock_empty = {"datasets": []}
    
    with respx.mock:
        respx.get("https://donneespubliques.gouv.bj/api/open/datasets/all").mock(
            return_value=Response(200, json=mock_empty)
        )
        
        result = await download_dataset("nonexistent")
        
        assert result["success"] is False
        assert "not found" in result["error"]


@pytest.mark.asyncio
async def test_download_dataset_file_too_large():
    """Test download rejection when file exceeds size limit."""
    mock_dataset = {
        "datasets": [{
            "id": "test-ds",
            "name": "test-dataset",
            "title": "Test Dataset",
            "organization": "Test Org",
            "resources": [{
                "id": "res-1",
                "name": "huge.csv",
                "url": "https://example.com/huge.csv",
                "format": "CSV",
                "package_id": "test-ds"
            }],
            "tags": [],
            "description": ""
        }]
    }
    
    with respx.mock:
        respx.get("https://donneespubliques.gouv.bj/api/open/datasets/all").mock(
            return_value=Response(200, json=mock_dataset)
        )
        respx.get("https://example.com/huge.csv").mock(
            return_value=Response(200, content=b"x" * (2 * 1024 * 1024), headers={
                "Content-Length": str(2 * 1024 * 1024)  # 2MB
            })
        )
        
        result = await download_dataset("test-ds", max_size_mb=1)
        
        assert result["success"] is False
        assert "too large" in result["error"] or "exceeds" in result["error"]


@pytest.mark.asyncio
async def test_download_dataset_content_disposition_filename():
    """Test extracting filename from Content-Disposition header."""
    mock_dataset = {
        "datasets": [{
            "id": "test-ds",
            "name": "test-dataset",
            "title": "Test Dataset",
            "organization": "Test Org",
            "resources": [{
                "id": "res-1",
                "name": "download",
                "url": "https://example.com/download",
                "format": "CSV",
                "package_id": "test-ds"
            }],
            "tags": [],
            "description": ""
        }]
    }
    
    file_content = b"data"
    
    with respx.mock:
        respx.get("https://donneespubliques.gouv.bj/api/open/datasets/all").mock(
            return_value=Response(200, json=mock_dataset)
        )
        respx.get("https://example.com/download").mock(
            return_value=Response(200, content=file_content, headers={
                "Content-Type": "text/csv",
                "Content-Disposition": 'attachment; filename="actual_filename.csv"'
            })
        )
        
        result = await download_dataset("test-ds")
        
        assert result["success"] is True
        assert result["filename"] == "actual_filename.csv"


# --- Tests for Client methods ---

@pytest.mark.asyncio
async def test_client_get_resource_preview():
    """Test the client's get_resource_preview method."""
    client = BeninPortalClient()
    
    csv_content = b"col1,col2\nval1,val2\nval3,val4"
    
    with respx.mock:
        respx.get("https://example.com/test.csv").mock(
            return_value=Response(200, content=csv_content)
        )
        
        headers, rows = await client.get_resource_preview("https://example.com/test.csv", max_rows=2)
        
        assert headers == ["col1", "col2"]
        assert len(rows) <= 2
        assert ["val1", "val2"] in rows or ["val3", "val4"] in rows
    
    await client.close()


@pytest.mark.asyncio
async def test_client_download_resource():
    """Test the client's download_resource method."""
    client = BeninPortalClient()
    
    file_content = b"test data content"
    
    with respx.mock:
        respx.get("https://example.com/file.csv").mock(
            return_value=Response(200, content=file_content, headers={
                "Content-Type": "text/csv",
                "Content-Length": str(len(file_content))
            })
        )
        
        content, filename, mime_type = await client.download_resource(
            "https://example.com/file.csv",
            max_size_mb=10
        )
        
        assert content == file_content
        assert filename == "file.csv"
        assert mime_type == "text/csv"
    
    await client.close()


@pytest.mark.asyncio
async def test_client_download_resource_size_limit():
    """Test that download respects size limits."""
    client = BeninPortalClient()
    
    large_content = b"x" * (2 * 1024 * 1024)  # 2MB
    
    with respx.mock:
        respx.get("https://example.com/large.csv").mock(
            return_value=Response(200, content=large_content, headers={
                "Content-Length": str(2 * 1024 * 1024)
            })
        )
        
        with pytest.raises(Exception, match="too large|exceeds"):
            await client.download_resource(
                "https://example.com/large.csv",
                max_size_mb=1
            )
    
    await client.close()
