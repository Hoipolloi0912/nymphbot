import psycopg
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
PATH = "cache"
os.makedirs(PATH, exist_ok=True)

def load_anime_dict():
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ann_id, name_en, name_ja
                FROM anime
                WHERE name_en IS NOT NULL OR name_ja IS NOT NULL;
            """)
            rows = cur.fetchall()
            return {id:[name_en.strip() if name_en else None,name_ja.strip() if name_ja else None] for id, name_en, name_ja in rows}

def load_artist_dict():
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name
                FROM artist;
            """)
            rows = cur.fetchall()
            return {id: name.strip() for id, name in rows}

def load_song_dict():
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.amq_song_id, s.name, a.name
                FROM song s join artist a on s.artist_id = a.id;
            """)
            rows = cur.fetchall()
            return {id: [s_name.strip(),a_name.strip()] for id, s_name,a_name in rows}