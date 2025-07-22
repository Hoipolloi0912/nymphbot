import psycopg
import json
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
PATH = "cache/"

def make_anime_json():
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ann_id, name_en, name_ja
                FROM anime
                WHERE name_en IS NOT NULL OR name_ja IS NOT NULL;
            """)
            rows = cur.fetchall()
            anime_dict = {id:[name_en.strip() if name_en else None,name_ja.strip() if name_ja else None] for id, name_en, name_ja in rows}

    with open(f"{PATH}anime_map.json", "w", encoding="utf-8") as f:
        json.dump(anime_dict, f, ensure_ascii=False, indent=2)
    print("anime_map updated.")

def make_artist_json():
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name
                FROM artist;
            """)
            rows = cur.fetchall()
            artist_dict = {id: name.strip() for id, name in rows}

    with open(f"{PATH}artist_map.json", "w", encoding="utf-8") as f:
        json.dump(artist_dict, f, ensure_ascii=False, indent=2)
    print("artist_map updated.")

def make_song_json():
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.amq_song_id, s.name, a.name
                FROM song s join artist a on s.artist_id = a.id;
            """)
            rows = cur.fetchall()
            artist_dict = {id: [s_name.strip(),a_name.strip()] for id, s_name,a_name in rows}

    with open(f"{PATH}song_map.json", "w", encoding="utf-8") as f:
        json.dump(artist_dict, f, ensure_ascii=False, indent=2)
    print("song_map updated.")

def get_artist_dict():
    with open(f"{PATH}artist_map.json", "r", encoding="utf-8") as f:
        return json.load(f)

def get_anime_dict():
    with open(f"{PATH}anime_map.json","r", encoding="utf-8") as f:
        return json.load(f)
    
def get_song_dict():
    with open(f"{PATH}song_map.json","r", encoding="utf-8") as f:
        return json.load(f)

if __name__ == "__main__":
    make_anime_json()
    make_artist_json()
    make_song_json()