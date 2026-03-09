"""Tests for download_dataset hybrid mode functionality."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from opendata_bj.tools.datasets import (
    download_dataset,
    get_full_resource_url,
    AUTO_MODE_SIZE_THRESHOLD,
)


class TestGetFullResourceUrl:
    """Tests for get_full_resource_url utility function."""

    def test_relative_url_adds_domain(self):
        """Test that relative URLs get the domain prepended."""
        resource = MagicMock()
        resource.url = "/app/export_ckan/dataset.csv"
        
        result = get_full_resource_url(resource)
        assert result == "https://donneespubliques.gouv.bj/app/export_ckan/dataset.csv"

    def test_external_url_unchanged(self):
        """Test that external URLs remain unchanged."""
        resource = MagicMock()
        resource.url = "https://benin.opendataforafrica.org/resource/embed/vzedhob"
        
        result = get_full_resource_url(resource)
        assert result == "https://benin.opendataforafrica.org/resource/embed/vzedhob"

    def test_absolute_url_unchanged(self):
        """Test that absolute URLs remain unchanged."""
        resource = MagicMock()
        resource.url = "https://example.com/data.csv"
        
        result = get_full_resource_url(resource)
        assert result == "https://example.com/data.csv"


class TestDownloadDatasetHTML:
    """Tests for HTML format handling."""

    @pytest.mark.asyncio
    async def test_html_format_returns_error(self):
        """Test that HTML resources return an error with suggestions."""
        mock_client = MagicMock()
        mock_dataset = MagicMock()
        mock_dataset.resources = [MagicMock()]
        mock_dataset.resources[0].format = "HTML"
        mock_dataset.resources[0].name = "Embed Page"
        mock_dataset.resources[0].url = "/resource/embed/vzedhob"
        
        mock_client.get_dataset_details = AsyncMock(return_value=mock_dataset)
        
        result = await download_dataset(mock_client, "test-dataset")
        
        assert result["success"] is False
        assert "Cannot download HTML resource directly" in result["error"]
        assert result["format"] == "HTML"
        assert "suggestion" in result
        assert "alternative" in result
        assert "preview_dataset" in result["alternative"]

    @pytest.mark.asyncio
    async def test_htm_format_returns_error(self):
        """Test that HTM resources also return an error."""
        mock_client = MagicMock()
        mock_dataset = MagicMock()
        mock_dataset.resources = [MagicMock()]
        mock_dataset.resources[0].format = "htm"  # lowercase
        mock_dataset.resources[0].name = "Embed Page"
        mock_dataset.resources[0].url = "/resource/embed/vzedhob"
        
        mock_client.get_dataset_details = AsyncMock(return_value=mock_dataset)
        
        result = await download_dataset(mock_client, "test-dataset")
        
        assert result["success"] is False
        assert "Cannot download HTML resource directly" in result["error"]


class TestDownloadDatasetURLMode:
    """Tests for URL mode download."""

    @pytest.mark.asyncio
    async def test_url_mode_returns_url(self):
        """Test that method='url' returns the download URL."""
        mock_client = MagicMock()
        mock_dataset = MagicMock()
        mock_dataset.resources = [MagicMock()]
        mock_dataset.resources[0].format = "CSV"
        mock_dataset.resources[0].name = "data.csv"
        mock_dataset.resources[0].url = "/data.csv"
        
        mock_client.get_dataset_details = AsyncMock(return_value=mock_dataset)
        
        result = await download_dataset(mock_client, "test-dataset", method="url")
        
        assert result["success"] is True
        assert result["method"] == "url"
        assert "download_url" in result
        assert result["filename"] == "data.csv"
        assert result["format"] == "CSV"
        assert "note" in result
        # Should not download anything
        mock_client.download_resource.assert_not_called()


class TestDownloadDatasetContentMode:
    """Tests for content mode download."""

    @pytest.mark.asyncio
    async def test_content_mode_returns_base64(self):
        """Test that method='content' returns base64 content."""
        mock_client = MagicMock()
        mock_dataset = MagicMock()
        mock_dataset.resources = [MagicMock()]
        mock_dataset.resources[0].format = "CSV"
        mock_dataset.resources[0].name = "data.csv"
        mock_dataset.resources[0].url = "/data.csv"
        
        mock_client.get_dataset_details = AsyncMock(return_value=mock_dataset)
        mock_client.download_resource = AsyncMock(return_value=(
            b"col1,col2\nval1,val2",  # content
            "data.csv",  # filename
            "text/csv",  # mime_type
        ))
        
        result = await download_dataset(mock_client, "test-dataset", method="content")
        
        assert result["success"] is True
        assert result["method"] == "content"
        assert "content_base64" in result
        assert result["filename"] == "data.csv"
        assert result["size_bytes"] == 22
        assert result["mime_type"] == "text/csv"
        mock_client.download_resource.assert_called_once()


class TestDownloadDatasetAutoMode:
    """Tests for auto mode adaptive behavior."""

    @pytest.mark.asyncio
    async def test_auto_mode_small_file_returns_content(self):
        """Test that auto mode returns content for files < 1MB."""
        mock_client = MagicMock()
        mock_dataset = MagicMock()
        mock_dataset.resources = [MagicMock()]
        mock_dataset.resources[0].format = "CSV"
        mock_dataset.resources[0].name = "small.csv"
        mock_dataset.resources[0].url = "/small.csv"
        
        mock_client.get_dataset_details = AsyncMock(return_value=mock_dataset)
        # Small content (under 1MB)
        mock_client.download_resource = AsyncMock(return_value=(
            b"x" * 1000,  # 1KB content
            "small.csv",
            "text/csv",
        ))
        
        result = await download_dataset(mock_client, "test-dataset", method="auto")
        
        assert result["success"] is True
        assert result["method"] == "content"
        assert "content_base64" in result

    @pytest.mark.asyncio
    async def test_auto_mode_large_file_returns_url(self):
        """Test that auto mode returns URL for files >= 1MB."""
        mock_client = MagicMock()
        mock_dataset = MagicMock()
        mock_dataset.resources = [MagicMock()]
        mock_dataset.resources[0].format = "CSV"
        mock_dataset.resources[0].name = "large.csv"
        mock_dataset.resources[0].url = "/large.csv"
        
        mock_client.get_dataset_details = AsyncMock(return_value=mock_dataset)
        # Large content (over 1MB)
        mock_client.download_resource = AsyncMock(return_value=(
            b"x" * (AUTO_MODE_SIZE_THRESHOLD + 1000),  # > 1MB content
            "large.csv",
            "text/csv",
        ))
        
        result = await download_dataset(mock_client, "test-dataset", method="auto")
        
        assert result["success"] is True
        assert result["method"] == "url"
        assert "download_url" in result
        assert "content_base64" not in result
        assert "1.0MB" in result["note"] or "1.1MB" in result["note"]


class TestDownloadDatasetEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_dataset_not_found(self):
        """Test error when dataset is not found."""
        mock_client = MagicMock()
        mock_client.get_dataset_details = AsyncMock(return_value=None)
        
        result = await download_dataset(mock_client, "nonexistent")
        
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_no_resources(self):
        """Test error when dataset has no resources."""
        mock_client = MagicMock()
        mock_dataset = MagicMock()
        mock_dataset.resources = []
        
        mock_client.get_dataset_details = AsyncMock(return_value=mock_dataset)
        
        result = await download_dataset(mock_client, "test-dataset")
        
        assert result["success"] is False
        assert "no downloadable resources" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_resource_index_out_of_range(self):
        """Test error when resource index is out of range."""
        mock_client = MagicMock()
        mock_dataset = MagicMock()
        mock_dataset.resources = [MagicMock()]
        
        mock_client.get_dataset_details = AsyncMock(return_value=mock_dataset)
        
        result = await download_dataset(mock_client, "test-dataset", resource_index=5)
        
        assert result["success"] is False
        assert "out of range" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_download_error(self):
        """Test error handling when download fails."""
        mock_client = MagicMock()
        mock_dataset = MagicMock()
        mock_dataset.resources = [MagicMock()]
        mock_dataset.resources[0].format = "CSV"
        mock_dataset.resources[0].name = "data.csv"
        mock_dataset.resources[0].url = "/data.csv"
        
        mock_client.get_dataset_details = AsyncMock(return_value=mock_dataset)
        mock_client.download_resource = AsyncMock(side_effect=Exception("Network error"))
        
        result = await download_dataset(mock_client, "test-dataset", method="content")
        
        assert result["success"] is False
        assert "Download failed" in result["error"]
        assert "resource_url" in result

    @pytest.mark.asyncio
    async def test_permission_error(self):
        """Test handling of permission errors."""
        mock_client = MagicMock()
        mock_dataset = MagicMock()
        mock_dataset.resources = [MagicMock()]
        mock_dataset.resources[0].format = "CSV"
        mock_dataset.resources[0].name = "data.csv"
        mock_dataset.resources[0].url = "/data.csv"
        
        mock_client.get_dataset_details = AsyncMock(return_value=mock_dataset)
        mock_client.download_resource = AsyncMock(side_effect=PermissionError("Access denied"))
        
        result = await download_dataset(mock_client, "test-dataset", method="content")
        
        assert result["success"] is False
        assert "Access denied" in result["error"]
        assert "403" in result["error"]

    @pytest.mark.asyncio
    async def test_value_error(self):
        """Test handling of value errors (e.g., file too large)."""
        mock_client = MagicMock()
        mock_dataset = MagicMock()
        mock_dataset.resources = [MagicMock()]
        mock_dataset.resources[0].format = "CSV"
        mock_dataset.resources[0].name = "huge.csv"
        mock_dataset.resources[0].url = "/huge.csv"
        
        mock_client.get_dataset_details = AsyncMock(return_value=mock_dataset)
        mock_client.download_resource = AsyncMock(side_effect=ValueError("File exceeds size limit"))
        
        result = await download_dataset(mock_client, "test-dataset", method="content")
        
        assert result["success"] is False
        assert "File exceeds size limit" in result["error"]
