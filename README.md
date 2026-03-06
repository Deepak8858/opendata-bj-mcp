# Benin OpenData MCP

Server MCP (Model Context Protocol) pour naviguer et consommer les données publiques du Bénin.

## Installation

```bash
pip install -e ".[test]"
```

## Usage

Lancer le serveur en local :
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

## Configuration dans un client MCP

Une fois l'application fonctionnelle, vous pouvez l'intégrer à un client compatible MCP (comme Claude Desktop, Cursor, OpenClaw, etc.) en renseignant la configuration suivante :

### Exemple pour Claude Desktop (`claude_desktop_config.json`) ou Cursor :

**Option 1 : En utilisant Python localement**
```json
{
  "mcpServers": {
    "opendata-bj": {
      "command": "fastmcp",
      "args": ["run", "/chemin/absolu/vers/opendata-bj-mcp/src/opendata_bj/server.py"],
      "env": {
        "BENIN_OPEN_DATA_API_KEY": "votre_cle_api_ici"
      }
    }
  }
}
```
*(Assurez-vous que `fastmcp` est bien accessible globalement ou précisez le chemin absolu vers votre environnement virtuel)*

**Option 2 : En utilisant Docker**
```json
{
  "mcpServers": {
    "opendata-bj": {
      "command": "docker",
      "args": [
        "run",
        "-i",
        "--rm",
        "-e",
        "BENIN_OPEN_DATA_API_KEY=votre_cle_api_ici",
        "opendata-bj-mcp"
      ]
    }
  }
}
```

## Architecture
Inspirée du projet `Collegue`, cette architecture privilégie la modularité et la validation stricte des données via Pydantic v2.
