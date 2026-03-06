FROM python:3.13-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml .
# We use a trick to install dependencies from pyproject.toml without copying the whole code first
# if we just run pip install it will try to find the package but we haven't copied it yet, 
# actually since it's a pyproject.toml with build system, we can copy everything and install.
COPY . .

RUN pip install --no-cache-dir .

# We can specify BENIN_OPEN_DATA_API_KEY at runtime
ENV BENIN_OPEN_DATA_API_KEY=""

# The server listens via FastMCP. FastMCP uses stdio by default for MCP, 
# so we run the server using fastmcp run.
ENTRYPOINT ["fastmcp", "run", "src/opendata_bj/server.py"]
