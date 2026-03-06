import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath("src"))
os.environ["BENIN_OPEN_DATA_API_KEY"] = "bju-d286e7654d098c02f4cac502e0f959f4"

from opendata_bj.server import get_dataset

async def main():
    try:
        ds1 = await get_dataset("vzedhob")
        print(f"DS1: {ds1[:100]}...\n")
    except Exception as e:
        print("ERROR:", str(e))

asyncio.run(main())
