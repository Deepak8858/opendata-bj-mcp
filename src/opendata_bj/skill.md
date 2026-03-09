# Skill: Benin Open Data MCP

This skill provides the capability to search, retrieve, and manage public datasets from Benin's official open data portal (`donneespubliques.gouv.bj`).

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
4.  **Explore**: If the user is unsure where to start, use `list_organizations()` to show them which institutions provide data.
