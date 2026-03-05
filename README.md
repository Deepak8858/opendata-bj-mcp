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

## Architecture
Inspirée du projet `Collegue`, cette architecture privilégie la modularité et la validation stricte des données via Pydantic v2.
