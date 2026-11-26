import os
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]

load_dotenv(ROOT_DIR / ".env", override=True)


def get_postgres_connection():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        database=os.getenv("PGDATABASE", "traffic_db"),
        user=os.getenv("PGUSER", "traffic_user"),
        password=os.getenv("PGPASSWORD"),
        cursor_factory=RealDictCursor,
        connect_timeout=5
    )


def get_mongo_database():
    uri = os.getenv("MONGODB_URI")

    if not uri:
        raise RuntimeError("MONGODB_URI is not configured")

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)

    client.admin.command("ping")

    return client["traffic_management"]
