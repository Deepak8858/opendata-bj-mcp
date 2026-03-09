# Benin OpenData MCP

Server MCP (Model Context Protocol) pour naviguer et consommer les données publiques du Bénin.

## Installation

```bash
pip install -e ".[test]"
```

## Authentification

Ce serveur MCP interagit avec le portail OpenData du Bénin. L'authentification dépend du type d'opération :

| Opération | Clé API requise | Description |
|-----------|-----------------|-------------|
| **Lecture** (search, preview, download) | ❌ Optionnelle | Les données publiques sont accessibles sans authentification |
| **Écriture** (bulk upload) | ✅ Obligatoire | Nécessite une clé API avec permissions d'écriture |

### Obtenir une clé API

Pour les opérations d'écriture (bulk upload), contactez l'administration du portail OpenData du Bénin pour obtenir une clé API.

### Configuration de la clé API

Si vous avez une clé API, configurez-la via la variable d'environnement :

```bash
export BENIN_OPEN_DATA_API_KEY="votre_cle_api_ici"
```

## Usage

Lancer le serveur en local :

```bash
# Sans clé API (lecture seule)
fastmcp run src/opendata_bj/server.py

# Avec clé API (lecture + écriture)
BENIN_OPEN_DATA_API_KEY="votre_cle" fastmcp run src/opendata_bj/server.py
```

### Usage avec Docker

Vous pouvez également exécuter ce serveur MCP via Docker.

1. Construire l'image Docker :
```bash
docker build -t opendata-bj-mcp .
```

2. Lancer le conteneur (lecture seule) :

```bash
docker run -i --rm opendata-bj-mcp
```

3. Lancer le conteneur (avec clé API pour les opérations d'écriture) :

```bash
docker run -i --rm \
  -e BENIN_OPEN_DATA_API_KEY="votre_cle_api_ici" \
  opendata-bj-mcp
```

*(L'option `-i` est utilisée car le serveur MCP communique via `stdio`)*

## Configuration dans un client MCP

Une fois l'application fonctionnelle, vous pouvez l'intégrer à un client compatible MCP (comme Claude Desktop, Cursor, OpenClaw, etc.) en renseignant la configuration suivante :

### Exemple pour Claude Desktop (`claude_desktop_config.json`) ou Cursor :

**Option 1 : En utilisant Python localement (lecture seule)**
```json
{
  "mcpServers": {
    "opendata-bj": {
      "command": "fastmcp",
      "args": ["run", "/chemin/absolu/vers/opendata-bj-mcp/src/opendata_bj/server.py"]
    }
  }
}
```

**Option 2 : En utilisant Python localement (avec clé API)**
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

**Option 3 : En utilisant Docker (lecture seule)**
```json
{
  "mcpServers": {
    "opendata-bj": {
      "command": "docker",
      "args": [
        "run",
        "-i",
        "--rm",
        "opendata-bj-mcp"
      ]
    }
  }
}
```

**Option 4 : En utilisant Docker (avec clé API)**
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

*(Assurez-vous que `fastmcp` est bien accessible globalement ou précisez le chemin absolu vers votre environnement virtuel)*

## Architecture

Inspirée du projet `Collegue`, cette architecture privilégie la modularité et la validation stricte des données via Pydantic v2.
