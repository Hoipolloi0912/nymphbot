import psycopg
import os
from contextlib import contextmanager

MAX_LEARNING = 12

@contextmanager
def get_conn():
    DSN = os.getenv("DB_URL")
    conn = psycopg.connect(DSN)
    try:
        yield conn
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def get_random_links(count):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            select link from anison order by random() limit %s
        """, (count,))
        return [row[0] for row in cur.fetchall()]


def upsert_user(discord_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (discord_id)
            VALUES (%s)
            ON CONFLICT (discord_id)
            DO UPDATE SET
                date_updated = NOW();
        """, (discord_id,))

def deactivate_old_songs(discord_id: int, song_ids: list[int]):
    # If song_ids is empty, deactivate everything
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            DELETE FROM user_song
            WHERE discord_id = %s
              AND total_reviews = 0
        """, (discord_id,))
        cur.execute("""
            UPDATE user_song
            SET is_active = FALSE
            WHERE discord_id = %s
              AND NOT (amq_song_id = ANY(%s::BIGINT[]))
        """, (discord_id, song_ids))

def get_current_round(discord_id: int) -> int:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT current_round
            FROM users
            WHERE discord_id = %s
        """, (discord_id,))
        row = cur.fetchone()
        return row[0] if row else 0
    
def upsert_user_song_list(discord_id: int, song_ids: list[int], current_round: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO user_song (
                discord_id,
                amq_song_id,
                last_review_round,
                next_review_round,
                is_active
            )
            SELECT
                %s,
                t.amq_song_id,
                %s,
                %s,
                TRUE
            FROM (
                SELECT DISTINCT unnest(%s::BIGINT[]) AS amq_song_id
            ) t
            ON CONFLICT (discord_id, amq_song_id)
            DO UPDATE SET
                is_active = TRUE
            WHERE user_song.is_active = FALSE
        """, (discord_id, current_round, current_round, song_ids))

def get_ann_song_ids_from_anime_ids(website: str, anime_ids: list[int]) -> list[int]:
    if website not in ("mal", "anilist","ann"):
        raise ValueError("website must be 'mal' or 'anilist'")
    if not anime_ids:
        return []

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT b.ann_song_id
            FROM anison b join anime a on b.anime_id = a.ann_id
            WHERE a.{website}_id = ANY(%s::BIGINT[])
            """,
            (anime_ids,)
        )
        return list({row[0] for row in cur.fetchall()})
    
def get_amq_song_ids_from_anime_ids(website: str, anime_ids: list[int]) -> list[int]:
    if website not in ("mal", "anilist","ann"):
        raise ValueError("website must be 'mal' or 'anilist'")
    if not anime_ids:
        return []

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT b.amq_song_id
            FROM anison b join anime a on b.anime_id = a.ann_id join song c on b.amq_song_id = c.amq_song_id
            WHERE a.{website}_id = ANY(%s::BIGINT[])
            AND c.category != 2
            """,
            (anime_ids,)
        )
        return list({row[0] for row in cur.fetchall()})
    
def get_amq_song_ids_from_user_ids(user_ids,limit):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT a.amq_song_id
            FROM user_song a join anison b ON a.amq_song_id = b.amq_song_id
            WHERE discord_id = ANY(%s)
            AND b.link IS NOT NULL AND b.dub IS FALSE AND b.rebroad IS FALSE
            AND is_active = TRUE
            GROUP BY amq_song_id
            ORDER BY RANDOM()
            LIMIT %s""",(user_ids,limit))
        return [row[0] for row in cur.fetchall()]

def get_ann_song_ids_from_artist_id(artist_id: int, limit) -> list[int]:
    query = """
    WITH RECURSIVE containing_groups AS (
    SELECT id, name, member_id
    FROM artist
    WHERE id = %s

    UNION

    SELECT a.id, a.name, a.member_id
    FROM artist a
    JOIN containing_groups cg ON cg.id = ANY(a.member_id)
    ),
    distinct_songs AS (
    SELECT DISTINCT ON (a.amq_song_id) a.ann_song_id 
    FROM anison a
    JOIN anime b ON a.anime_id = b.ann_id
    JOIN song c ON a.amq_song_id = c.amq_song_id
    WHERE c.artist_id IN (SELECT id FROM containing_groups)
        AND a.link IS NOT NULL AND a.dub IS FALSE AND a.rebroad IS FALSE
    ORDER BY a.amq_song_id, random()
    )
    SELECT *
    FROM distinct_songs
    ORDER BY random()
    LIMIT %s;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(query, (artist_id, limit))
        return list({row[0] for row in cur.fetchall()})

def fetch_artist_tree_for_song(amq_song_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT artist_id
            FROM song
            WHERE amq_song_id = %s
        """, (amq_song_id,))
        row = cur.fetchone()
        if not row:return None, []
        artist_id = row[0]
    tree = fetch_artist_tree([artist_id,])
    return artist_id, tree

def fetch_from_amq_song_id(amq_song_ids):
    placeholders = ','.join(['%s'] * len(amq_song_ids))
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT DISTINCT ON (a.amq_song_id)
                a.amq_song_id,
                a.link,
                d.name_en,
                d.name_ja,
                b.name,
                c.name,
                c.id
            FROM anison a
            JOIN song b ON a.amq_song_id = b.amq_song_id
            JOIN artist c ON b.artist_id = c.id
            JOIN anime d ON a.anime_id = d.ann_id
            WHERE a.amq_song_id IN ({placeholders})
            ORDER BY a.amq_song_id, RANDOM();
        """, amq_song_ids)
        return cur.fetchall()
    
def fetch_alt_anime_names(ann_song_ids):
    placeholders = ','.join(['%s'] * len(ann_song_ids))
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT b.amq_song_id, a.name_en, a.name_ja
            FROM anime a
            JOIN anison b ON b.anime_id = a.ann_id
            WHERE b.amq_song_id IN (
                SELECT b2.amq_song_id
                FROM anison b2
                WHERE b2.ann_song_id IN ({placeholders})
            )
            AND b.ann_song_id NOT IN ({placeholders});
        """, ann_song_ids * 2)
        return cur.fetchall()
    
def fetch_artist_tree(artist_ids):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            WITH RECURSIVE artist_tree AS (
                SELECT * FROM artist WHERE id = ANY(%s)
                UNION
                SELECT a.* FROM artist a
                JOIN artist_tree at ON a.id = ANY(at.member_id)
            )
            SELECT * FROM artist_tree;
        """, (artist_ids,))
        return cur.fetchall()

def fetch_artists_by_ids(ids):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id,name FROM artist WHERE id = ANY(%s)
        """, (ids,))
        return cur.fetchall()

def fetch_songs_srs(player_id, limit):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            WITH dedup AS (
                SELECT DISTINCT ON (u.discord_id, u.amq_song_id)
                    u.amq_song_id,
                    a.link,
                    ani.name_en,
                    ani.name_ja,
                    s.name AS song_name,
                    ar.name AS artist_name,
                    ar.id AS artist_id,
                    ur.current_round,
                    u.next_review_round,
                    u.state
                FROM user_song u
                JOIN anison a ON u.amq_song_id = a.amq_song_id
                JOIN song s ON a.amq_song_id = s.amq_song_id
                JOIN artist ar ON s.artist_id = ar.id
                JOIN anime ani ON a.anime_id = ani.ann_id
                JOIN users ur ON u.discord_id = ur.discord_id
                WHERE u.discord_id = %s
                AND u.is_active = TRUE
                AND a.dub = FALSE
                AND a.rebroad = FALSE
                AND a.link IS NOT NULL
                ORDER BY
                    u.discord_id,
                    u.amq_song_id,
                    a.anime_id,
                    a.ann_song_id
            ),
            weighted_main AS (
                -- Main picks: weighted toward due/learning/new
                SELECT *
                FROM dedup
                ORDER BY
                    (
                        CASE
                            WHEN next_review_round <= current_round THEN 100
                            WHEN state = 'learning' THEN 40
                            WHEN state = 'new' THEN 20
                            ELSE 5
                        END
                        + (RANDOM() * 10)  -- noise so it isn't deterministic
                    ) DESC
                LIMIT GREATEST(%s - 1, 1)
            ),
            surprise AS (
                -- Surprise pick: intentionally pull from not-due mature (or otherwise low priority)
                -- If none exist, it may return 0 rows; main will still fill.
                SELECT *
                FROM dedup
                WHERE NOT (next_review_round <= current_round)
                  AND state = 'mature'
                ORDER BY RANDOM()
                LIMIT 1
            ),
            combined AS (
                SELECT * FROM weighted_main
                UNION ALL
                SELECT * FROM surprise
            )
            SELECT *
            FROM combined
            ORDER BY RANDOM()
            LIMIT %s;
        """, (player_id, limit, limit))
        return cur.fetchall()

def update_srs_correct(discord_id, song_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            #update current round
            cur.execute("""
                UPDATE users
                SET current_round = current_round + 1
                WHERE discord_id = %s
                RETURNING current_round
            """, (discord_id,))
            current_round = cur.fetchone()[0]

            #get srs metrics
            cur.execute("""
                SELECT
                    ease_factor,
                    interval_rounds
                FROM user_song
                WHERE discord_id = %s
                AND amq_song_id = %s
            """, (discord_id, song_id))
            ef, interval = cur.fetchone()

            #calculate new metrics
            if interval == 0:
                new_interval = 1
            elif interval == 1:
                new_interval = 6
            else:
                new_interval = int(interval * ef)

            ef += 0.1
            if ef < 1.3:
                ef = 1.3

            next_round = current_round + new_interval

            cur.execute("""
                UPDATE user_song
                SET
                    ease_factor = %s,
                    interval_rounds = %s,
                    last_review_round = %s,
                    next_review_round = %s,

                    total_reviews = total_reviews + 1,
                    correct_reviews = correct_reviews + 1,

                    state = 'learning'

                WHERE discord_id = %s
                AND amq_song_id = %s
            """, (
                ef,
                new_interval,
                current_round,
                next_round,
                discord_id,
                song_id
            ))

        conn.commit()

def update_srs_wrong(discord_id, song_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            #update current round
            cur.execute("""
                UPDATE users
                SET current_round = current_round + 1
                WHERE discord_id = %s
                RETURNING current_round
            """, (discord_id,))
            current_round = cur.fetchone()[0]

            #get current srs metrics
            cur.execute("""
                SELECT ease_factor
                FROM user_song
                WHERE discord_id = %s
                AND amq_song_id = %s
            """, (discord_id, song_id))
            ef = cur.fetchone()[0]

            #calculate new srs values
            ef -= 0.2
            if ef < 1.3:
                ef = 1.3

            new_interval = 1
            next_round = current_round + 1

            #update db
            cur.execute("""
                UPDATE user_song
                SET
                    ease_factor = %s,
                    interval_rounds = %s,
                    last_review_round = %s,
                    next_review_round = %s,

                    total_reviews = total_reviews + 1,

                    state = 'learning'

                WHERE discord_id = %s
                AND amq_song_id = %s
            """, (
                ef,
                new_interval,
                current_round,
                next_round,
                discord_id,
                song_id
            ))

        conn.commit()

def list_check(player_id) -> bool:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM user_song
            WHERE discord_id = %s AND is_active = TRUE
            LIMIT 1
        """, (player_id,))
        return cur.fetchone() is not None