import requests,os
from dotenv import load_dotenv

load_dotenv()
API_TOKEN = os.getenv("MAL_TOKEN")

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

    headers = {
        "Authorization": f"Bearer {API_TOKEN}"
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