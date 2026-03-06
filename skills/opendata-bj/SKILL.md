# Skill: Benin Open Data MCP

This skill provides the capability to search, retrieve, and manage public datasets from Benin's official open data portal (`donneespubliques.gouv.bj`) via its dedicated MCP server.

## 1. MCP Server Configuration

To use these tools, the MCP client must be configured to connect to the `opendata-bj-mcp` server.

**Option A: Using Docker (Recommended)**

This is the most reliable method as it encapsulates the server's environment.

```json
{
  "mcpServers": {
    "opendata-bj": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "-e", "BENIN_OPEN_DATA_API_KEY=your_api_key_here",
        "opendata-bj-mcp"
      ]
    }
  }
}
```

**Option B: Using a local Python environment**

This requires the project to be installed and accessible in the environment where the MCP client is launched.

```json
{
  "mcpServers": {
    "opendata-bj": {
      "command": "fastmcp",
      "args": ["run", "/path/to/your/opendata-bj-mcp/src/opendata_bj/server.py"],
      "env": {
        "BENIN_OPEN_DATA_API_KEY": "your_api_key_here"
      }
    }
  }
}
```

---

## 2. Available Tools

Once configured, the following tools become available under the `opendata-bj` MCP namespace.

### `search_datasets`
Searches for public datasets on the Benin open data portal.

**Parameters:**
- `query` (string, optional): The search keyword or phrase.
- `limit` (integer, optional, default: 10): The maximum number of datasets to return.

**Example Usage:**
I need to find datasets related to healthcare in Benin.
*MCP Call:* `opendata-bj/search_datasets(query="santé", limit=5)`

**Returns:** A formatted string listing the found datasets, including their ID, title, and organization.

### `get_dataset`
Retrieves detailed information and resources for a specific dataset using its ID.

**Parameters:**
- `dataset_id` (string, required): The unique identifier of the dataset (e.g., `vzedhob`).

**Example Usage:**
A user wants to know more about the "MICS_Santé" dataset I found.
*MCP Call:* `opendata-bj/get_dataset(dataset_id="vzedhob")`

**Returns:** A markdown-formatted string with the dataset's title, description, organization, tags, and a list of available resources with their URLs.

### `list_organizations`
Lists all the institutions and organizations that publish data on the portal.

**Parameters:**
- None.

**Example Usage:**
A user asks which government bodies publish data.
*MCP Call:* `opendata-bj/list_organizations()`

**Returns:** A formatted string listing all available organizations.

### `publish_datasets_bulk`
*Admin Tool.* Uploads multiple datasets to the portal in a single operation. This is an advanced tool for data administration.

**Parameters:**
- `metadata_json` (string, required): A JSON string representing the metadata for the datasets to be uploaded.

**Example Usage:**
An administrator needs to publish a batch of new datasets.
*MCP Call:* `opendata-bj/publish_datasets_bulk(metadata_json='[{"title": "New Dataset", ...}]')`

**Returns:** A confirmation message indicating success or failure.

## 3. Best Practices

- **Always `search` before `get`**: Use `search_datasets` to find the correct `dataset_id` before calling `get_dataset`.
- **Check Resource Formats**: When a user asks to process a dataset, first use `get_dataset` to inspect the available resource formats (e.g., CSV, HTML, PDF) to determine the next steps.
- **Clarify with the User**: If a search returns multiple results, present the list to the user to let them choose which one they are interested in before fetching details.
