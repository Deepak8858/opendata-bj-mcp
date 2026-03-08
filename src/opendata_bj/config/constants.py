"""
Configuration constants for OpenData BJ MCP.
"""

# API Configuration
DEFAULT_BASE_URL = "https://donneespubliques.gouv.bj"
API_TIMEOUT = 30.0
RESOURCE_TIMEOUT = 60.0

# Security Limits
MAX_PREVIEW_ROWS = 50
DEFAULT_PREVIEW_ROWS = 10
MAX_DOWNLOAD_SIZE_MB = 50
DEFAULT_DOWNLOAD_SIZE_MB = 10
MAX_PREVIEW_BYTES = 65536

# Endpoints
ENDPOINT_DATASETS_ALL = "/api/open/datasets/all"
ENDPOINT_ORGANIZATIONS = "/api/v1/organizations"
ENDPOINT_BULK_UPLOAD = "/api/v1/open/datasets/bulk-upload"
