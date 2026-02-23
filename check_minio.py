from minio import Minio
from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv(Path(__file__).resolve().parent / ".env")

client = Minio(
    "minio.fdi.ucm.es",
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
)

print("Conectando...")
print("Buckets:", [b.name for b in client.list_buckets()])
