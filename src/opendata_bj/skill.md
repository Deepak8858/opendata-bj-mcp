# Skill: Benin Open Data MCP

This skill provides the capability to search, retrieve, preview, and download public datasets from Benin's official open data portal (`donneespubliques.gouv.bj`).

## Available Tools

### `search_datasets`
Searches for public datasets on the Benin open data portal. It's the primary entry point for discovering data.

- **Parameters:**
  - `query` (string, optional): The search keyword or phrase.
  - `limit` (integer, optional, default: 10): The maximum number of datasets to return (1-100).
  - `offset` (integer, optional, default: 0): The number of results to skip, used for pagination through large result sets.
- **Returns:** A formatted string listing the found datasets, including their ID, title, and organization.
- **When to use:** When the user asks to find data about a specific topic (e.g., "trouve des données sur la santé").
- **Pagination:** For large result sets, use `offset` to navigate through pages of results (e.g., `offset=0` for page 1, `offset=10` for page 2 with `limit=10`).

### `get_dataset`
Retrieves detailed information and resources for a specific dataset using its ID.

- **Parameters:**
  - `dataset_id` (string, required): The unique identifier of the dataset, usually obtained from `search_datasets`.
- **Returns:** A markdown-formatted string with the dataset's title, description, organization, tags, and a list of available resources with their URLs.
- **When to use:** After finding a dataset with `search_datasets`, use this to get more details or download links.

### `preview_dataset`
Previews the content of a dataset resource directly in the chat. Supports multiple formats including CSV, JSON, Excel, and **HTML**.

- **Parameters:**
  - `dataset_id` (string, required): The ID of the dataset containing the resource.
  - `resource_index` (integer, optional, default: 0): The index of the resource to preview (0 = first resource).
  - `rows` (integer, optional, default: 10): Number of rows to show (1-50).
- **Returns:** A markdown table showing the preview data, or an error message if the format is not supported.
- **When to use:** When the user wants to see the actual data content before downloading.
- **Supported Formats:**
  - **CSV/TSV/TXT**: Tabular data with automatic delimiter detection
  - **JSON**: Structured data (objects and arrays) flattened for display
  - **Excel (XLS/XLSX)**: Spreadsheet data (requires `pandas`)
  - **HTML/HTM**: Extracts tables, definition lists, or structured content from web pages (requires `beautifulsoup4`)

### `download_dataset`
Downloads a dataset resource with adaptive method selection. Intelligently chooses between returning a direct download URL or the file content as base64, based on file size and format.

- **Parameters:**
  - `dataset_id` (string, required): The ID of the dataset to download from.
  - `resource_index` (integer, optional, default: 0): The index of the resource to download.
  - `max_size_mb` (integer, optional, default: 10): Maximum file size in MB (1-50).
  - `method` (string, optional, default: "auto"): Download method selection:
    - `"auto"`: Returns base64 if < 1MB, URL if >= 1MB or HTML format
    - `"url"`: Always returns the direct download URL
    - `"content"`: Always downloads and returns base64 content
- **Returns:**
  - For `method="url"`: Dictionary with `success`, `method="url"`, `download_url`, `filename`, `format`, `note`
  - For `method="content"`: Dictionary with `success`, `method="content"`, `content_base64`, `filename`, `size_bytes`, `mime_type`
  - For HTML resources: Error with `suggestion` to use `preview_dataset`
- **When to use:** When the user needs the actual file data for further processing or analysis.
- **Size Threshold:** Files >= 1MB automatically return a URL instead of base64 to avoid performance issues.
- **HTML Handling:** HTML resources (like OpenDataForAfrica embeds) cannot be downloaded directly. Use `preview_dataset` instead.

**Examples:**

```python
# Auto mode - smart selection based on file size
result = await download_dataset(dataset_id="abc123", method="auto")

# Always get URL (recommended for large files)
result = await download_dataset(dataset_id="abc123", method="url")

# Always get content (for small files you want to process)
result = await download_dataset(dataset_id="abc123", method="content")
```

### `list_organizations`
Lists all the institutions and organizations that publish data on the portal.

- **Parameters:** None.
- **Returns:** A formatted string listing all available organizations.
- **When to use:** When the user asks "who publishes data?" or wants to filter by a specific organization.

### `publish_datasets_bulk`
**(Admin Tool)** Uploads multiple datasets to the portal in a single operation.

- **Parameters:**
  - `metadata_json` (string, required): A JSON string representing the metadata for the datasets to be uploaded.
- **Returns:** A confirmation message indicating success or failure.
- **When to use:** Only for administrative tasks involving bulk data publication. Requires a valid API key with write permissions.

## Recommended Workflow

1.  **Discover**: Start with a broad search using `search_datasets(query="...")`.
2.  **Clarify**: If multiple datasets are found, present the list to the user to let them choose.
3.  **Detail**: Use `get_dataset(dataset_id="...")` with the ID chosen by the user to get detailed information and resource URLs.
4.  **Preview**: Use `preview_dataset(dataset_id="...", resource_index=0)` to show a sample of the data content.
5.  **Download**: If the user needs the full file, use `download_dataset(dataset_id="...")`.
6.  **Explore**: If the user is unsure where to start, use `list_organizations()` to show them which institutions provide data.

## HTML Preview Support

The HTML preview handler can extract data from:
- **HTML Tables**: Standard `<table>` elements with `<thead>` and `<tbody>`
- **Definition Lists**: `<dl>` elements with `<dt>` (term) and `<dd>` (definition)
- **Structured Sections**: Headings (`<h1>`-`<h6>`) with associated content
- **Fallback**: Meaningful paragraphs when no structured data is found

Note: Some HTML resources may be protected by Cloudflare or require authentication. In such cases, the tool will suggest using `download_dataset` instead.
