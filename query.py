import psycopg, os
from psycopg.rows import dict_row
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()
DB_URL=os.getenv('DB_URL')

def query(sql, params=None):
    try:
        cur.execute("SET search_path TO hoi;")
        cur.execute(sql, params)
        if cur.description:
            return cur.fetchall()
        else:
            conn.commit()
            return cur.rowcount
    except Exception as e:
        print(f"[ERROR] Query failed: {e}")
        print(f"SQL: {sql}")
        print(f"Params: {params}")
        return None

conn = psycopg.connect(DB_URL)
cur = conn.cursor()#row_factory=dict_row
results = query("""
        WITH RECURSIVE artist_tree AS (
            SELECT * FROM hoi.artist WHERE id = ANY(%s)
            UNION
            SELECT a.* FROM hoi.artist a
            JOIN artist_tree at ON a.id = ANY(at.member_id)
        )
        SELECT * FROM artist_tree;
""",([1008400],))

data_map = {id: (name, alt_ids or [], member_ids or []) for id, name, alt_ids, member_ids in results}

lines = []
stack = [(1008400, 0)]  # (id, level)

while stack:
    current_id, level = stack.pop()
    if current_id not in data_map:
        continue
    name, alt_ids, member_ids = data_map[current_id]
    lines.append("   " * level + name)
    for member_id in reversed(member_ids):  # reverse to keep original order
        stack.append((member_id, level + 1))

print("\n".join(lines))

'''if results:
    if isinstance(results, list):
        for row in results:
            print(row)
    else:print(results)
else:
    print("No results found.")'''