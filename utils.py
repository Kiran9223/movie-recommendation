import weaviate
from weaviate.client import WeaviateClient
import os
from dotenv import load_dotenv

load_dotenv()

def connect_to_db() -> WeaviateClient:
    client = weaviate.connect_to_wcs(
        cluster_url="https://onutedejt86xslgfyicogw.c0.us-west3.gcp.weaviate.cloud",
        auth_credentials=weaviate.auth.AuthApiKey(""),
        headers={"X-OpenAI-Api-Key": ""}
    )
    return client

client = connect_to_db()

print(client.is_ready())

client.close()
