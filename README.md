# Benin OpenData MCP

Server MCP (Model Context Protocol) pour naviguer et consommer les données publiques du Bénin.

## Installation

```bash
pip install -e ".[test]"
```

## Usage

Lancer le serveur :
```bash
fastmcp run src/opendata_bj/server.py
```

### Usage avec Docker

Vous pouvez également exécuter ce serveur MCP via Docker.

1. Construire l'image Docker :
```bash
docker build -t opendata-bj-mcp .
```

2. Lancer le conteneur :
```bash
docker run -i --rm -e BENIN_OPEN_DATA_API_KEY="votre_cle_api_ici" opendata-bj-mcp
```
*(L'option `-i` est utilisée car le serveur MCP communique via `stdio`)*

## Architecture
Inspirée du projet `Collegue`, cette architecture privilégie la modularité et la validation stricte des données via Pydantic v2.
