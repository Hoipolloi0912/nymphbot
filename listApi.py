import os
from datetime import datetime, timedelta, timezone
import requests
from dotenv import load_dotenv
import db

load_dotenv()

TOKEN_URL = "https://myanimelist.net/v1/oauth2/token"
API_URL = "https://api.myanimelist.net/v2"

def refresh_mal_token():
    client_id = os.environ["MAL_CLIENT_ID"]
    client_secret = os.environ["MAL_CLIENT_SECRET"]

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT refresh_token
                FROM tokens
                WHERE provider = 'mal';
            """)
            row = cur.fetchone()

    refresh_token = row[0]

    response = requests.post(
        TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()

    access_token = data["access_token"]
    new_refresh_token = data.get("refresh_token") or refresh_token
    expires_at = (
        datetime.now(timezone.utc)
        + timedelta(seconds=data["expires_in"])
    )

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tokens (
                    provider, access_token,
                    refresh_token, expires_at
                )
                VALUES ('mal', %s, %s, %s)
                ON CONFLICT (provider) DO UPDATE SET
                    access_token = EXCLUDED.access_token,
                    refresh_token = EXCLUDED.refresh_token,
                    expires_at = EXCLUDED.expires_at;
            """, (
                access_token,
                new_refresh_token,
                expires_at,
            ))

    return access_token

def get_mal_token():
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT access_token, expires_at
                FROM tokens
                WHERE provider = 'mal';
            """)
            row = cur.fetchone()

    if row:
        access_token, expires_at = row

        if expires_at > datetime.now(timezone.utc) + timedelta(days=5):
            return access_token

    return refresh_mal_token()

def get_anilist(name, status_flags):
    labels = ['Watching', 'Completed', 'Planning', 'Paused', 'Dropped']
    
    query = '''
    query ($username: String) {
        MediaListCollection(userName: $username, type: ANIME) {
            lists {
                name
                entries {
                    media {
                        id
                        title {
                            romaji
                            english
                        }
                    }
                }
            }
        }
    }
    '''

    variables = {
        "username": name
    }

    url = 'https://graphql.anilist.co'

    try:
        response = requests.post(url, json={'query': query, 'variables': variables}, timeout=10)

        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            return []

        data = response.json()
        results = []

        active_statuses = {labels[i] for i, flag in enumerate(status_flags) if flag}

        for media_list in data['data']['MediaListCollection']['lists']:
            if media_list['name'] in active_statuses:
                for entry in media_list['entries']:
                    results.append(entry['media']['id'])

        return results

    except requests.exceptions.RequestException as e:
        print(f"API error: {e}")
        return []

def get_mal(username, status_flags):
    status_labels = ['watching', 'completed', 'plan_to_watch', 'on_hold', 'dropped']

    api_token = get_mal_token()
    headers = {
        "Authorization": f"Bearer {api_token}"
    }

    all_ids = set()

    for i, flag in enumerate(status_flags):
        if not flag:
            continue

        status = status_labels[i]
        url = f"https://api.myanimelist.net/v2/users/{username}/animelist"
        params = {
            "limit": 100,
            "status": status,
            "nsfw" : True,
            "fields": "anime{id}"
        }

        while url:
            try:
                resp = requests.get(url, headers=headers, params=params if '?' not in url else None)
                resp.raise_for_status()
                data = resp.json()
                for entry in data.get("data", []):
                    anime_id = entry["node"]["id"]
                    all_ids.add(anime_id)
                url = data.get("paging", {}).get("next")
            except requests.exceptions.RequestException as e:
                print(f"Error fetching {status} list: {e}")
                break

    return list(all_ids)

get_list = {"anilist":get_anilist,"mal":get_mal}