"""
Test manuel contre le serveur réel.
Usage: python tests/manual/test_real_server.py
"""
import asyncio
import sys

sys.path.insert(0, "src")

from opendata_bj.client.portal import BeninPortalClient
from opendata_bj.tools.datasets import preview_dataset, download_dataset
from opendata_bj.config import DEFAULT_HEADERS, RESOURCE_HEADERS


async def test_preview_real():
    print("\n" + "=" * 60)
    print("TEST: Preview d'une ressource réelle")
    print("=" * 60)

    client = BeninPortalClient()

    try:
        datasets = await client.get_all_datasets(limit=5)
        print(f"\n✓ {len(datasets)} datasets trouvés")

        target = next((ds for ds in datasets if ds.resources), None)
        if not target:
            print("✗ Aucun dataset avec ressources")
            return

        resource = target.resources[0]
        print(f"\nDataset: {target.title}")
        print(f"Ressource: {resource.name} ({resource.format})")

        result = await preview_dataset(client, target.id, resource_index=0, rows=5)
        print(f"\nRésultat:\n{result}")

        if "Access denied" in result:
            print("\n⚠️ ERREUR 403 détectée")
        else:
            print("\n✓ Preview réussi!")

    except Exception as e:
        print(f"✗ Erreur: {e}")
    finally:
        await client.close()


async def test_download_real():
    print("\n" + "=" * 60)
    print("TEST: Download d'une ressource réelle (max 1MB)")
    print("=" * 60)

    client = BeninPortalClient()

    try:
        datasets = await client.get_all_datasets(limit=10)

        target = None
        resource = None

        for ds in datasets:
            for res in ds.resources:
                if res.format.upper() in ["CSV", "JSON"]:
                    target = ds
                    resource = res
                    break
            if target:
                break

        if not target:
            target = next((ds for ds in datasets if ds.resources), None)
            if target:
                resource = target.resources[0]

        if not target:
            print("✗ Aucun dataset avec ressources")
            return

        print(f"\nDataset: {target.title}")
        print(f"Ressource: {resource.name}")
        print(f"URL: {resource.url}")

        result = await download_dataset(client, target.id, max_size_mb=1)

        if result.get("success"):
            print(f"\n✓ Download réussi!")
            print(f"  Fichier: {result['filename']}")
            print(f"  Taille: {result['size_bytes']} bytes")
            print(f"  Type: {result['mime_type']}")
        else:
            error = result.get("error", "Unknown")
            print(f"\n✗ Erreur: {error}")

            if "403" in error:
                print(f"\n⚠️ ERREUR 403")
                print(f"💡 URL manuelle: {resource.url}")

    except Exception as e:
        print(f"✗ Exception: {e}")
    finally:
        await client.close()


async def test_headers():
    print("\n" + "=" * 60)
    print("TEST: Vérification des headers")
    print("=" * 60)

    print("\nHeaders API:")
    for key, value in DEFAULT_HEADERS.items():
        display = value[:50] + "..." if len(str(value)) > 50 else value
        print(f"  {key}: {display}")

    print("\nHeaders Resource:")
    for key, value in RESOURCE_HEADERS.items():
        display = value[:50] + "..." if len(str(value)) > 50 else value
        print(f"  {key}: {display}")

    print("\n✓ Headers configurés")


async def main():
    print("\n" + "=" * 60)
    print("TESTS MANUELS - OpenData BJ MCP")
    print("=" * 60)

    await test_headers()
    await test_preview_real()
    await test_download_real()

    print("\n" + "=" * 60)
    print("TESTS TERMINÉS")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
