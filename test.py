import psycopg

def make_tree(artist_id, data):
    artist_data = data.get(artist_id)
    if not artist_data:
        return None
    
    name, alt_names, members = artist_data
    node = {
        "names": [name] + [data[alt_id]["name"] for alt_id in alt_names],
        "members": [],
        "guessed": False
    }
    
    for member_id in members:
        member_tree = make_tree(member_id, data)
        if member_tree:
            node["members"].append(member_tree)
    
    return node

with psycopg.connect("postgresql://poi:FUJITAAKANE@137.66.10.78:5432/postgres") as conn:
    cur = conn.cursor()
    cur.execute(f"""
            WITH RECURSIVE artist_tree AS (
            SELECT * FROM hoi.artist WHERE id = ANY(%s)
            UNION
            SELECT a.* FROM hoi.artist a
            JOIN artist_tree at ON a.id = ANY(at.member_id)
        )
        SELECT * FROM artist_tree;
        """,([5320],))
    results = cur.fetchall()

    data = {id: [name,alts,members] for id,name,alts,members in results}


    alt_ids = [i for row in results for i in row[2]]
    cur.execute("""
        SELECT id,name FROM hoi.artist WHERE id = ANY(%s)
    """, (alt_ids,))
    results = cur.fetchall()
    data |= {id: [name, [],[]] for id, name in results}

print(data)
tree = make_tree(5320,data)

def _check(root,a):
    if root["guessed"]:#already guessed, no improvement
        return False
    if any(a == name.strip() for name in root["names"]):
        root["guessed"]= True
        return True
    
    for children in root["members"]:
        if _check(children,a):return True
    return False

def _check2(root):
    if root["guessed"]: return True
    if not root["members"]: return False
    members_guessed = []
    for child in root["members"]:
        members_guessed.append(_check2(child))
    if all(members_guessed):
        root["guessed"]= True
        return True
    return False

print(tree)

