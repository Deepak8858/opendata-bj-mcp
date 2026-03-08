FROM python:3.13-slim

WORKDIR /app

# Install dependencies first
COPY pyproject.toml .
RUN pip install --no-cache-dir fastmcp httpx "pydantic>=2.0" python-dotenv

# Copy source code
COPY . .

# Install our package
RUN pip install --no-cache-dir .

# We can specify BENIN_OPEN_DATA_API_KEY at runtime
ENV BENIN_OPEN_DATA_API_KEY=""

# Ensure local bin is in PATH
ENV PATH="/usr/local/bin:${PATH}"

# The server listens via FastMCP. FastMCP uses stdio by default for MCP, 
# so we run the server using fastmcp run.
ENTRYPOINT ["fastmcp", "run", "src/opendata_bj/server.py"]