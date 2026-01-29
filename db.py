import psycopg
import os
from contextlib import contextmanager

MAX_LEARNING = 12
BATCH_SIZE = 5

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

def get_song_ids_from_anime_ids(website: str, anime_ids: list[int]) -> list[int]:
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
    
def get_song_ids_from_artist_id(artist_id: int, limit) -> list[int]:
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

def fetch_from_ann_song_id(ann_song_ids):
    placeholders = ','.join(['%s'] * len(ann_song_ids))
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT a.amq_song_id, a.link, d.name_en, d.name_ja, b.name, c.name, c.id
            FROM anison a
            JOIN song b ON a.amq_song_id = b.amq_song_id
            JOIN artist c ON b.artist_id = c.id
            JOIN anime d ON a.anime_id = d.ann_id
            WHERE a.ann_song_id IN ({placeholders})
            ORDER BY RANDOM();
        """, ann_song_ids)
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
    
def fetch_songs_srs(player_id, limit=BATCH_SIZE):
    """
    Weighted SRS selection + intentional 'surprise' picks.

    - Heavy bias toward overdue + learning, then new, then mature.
    - Adds a small 'surprise' slice (usually mature/not-due) to keep rounds from feeling samey.
    - Still dedups (discord_id, amq_song_id) via your existing dedup CTE.
    """
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


def update_current_round(player_id, step):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE users
                SET current_round = current_round + %s
                WHERE discord_id = %s
            """, (step, player_id))
        conn.commit()

def update_srs(player_id, right_ids, wrong_ids):
    if not right_ids and not wrong_ids:
        return

    right_ids = list(right_ids or [])
    wrong_ids = list(wrong_ids or [])

    with get_conn() as conn, conn.cursor() as cur:
        # Fetch current_round once
        cur.execute("""
            SELECT current_round
            FROM users
            WHERE discord_id = %s
        """, (player_id,))
        row = cur.fetchone()
        if not row:
            return
        current_round = row[0]

        # 1) WRONG: always push into learning + reset interval
        if wrong_ids:
            cur.execute("""
                UPDATE user_song u
                SET
                    total_reviews     = u.total_reviews + 1,
                    interval_rounds   = 1,
                    ease_factor       = GREATEST(1.3, u.ease_factor - 0.20),
                    state             = 'learning',
                    last_review_round = %s,
                    next_review_round = %s + 1
                WHERE u.discord_id = %s
                  AND u.amq_song_id = ANY(%s::BIGINT[])
            """, (current_round, current_round, player_id, wrong_ids))

        # 2) Learning buffer AFTER wrongs (wrongs consume slots)
        cur.execute("""
            SELECT COUNT(*)
            FROM user_song
            WHERE discord_id = %s
              AND state = 'learning'
              AND is_active = TRUE
        """, (player_id,))
        learning_count = cur.fetchone()[0]
        slots = max(0, MAX_LEARNING - learning_count)

        # 3) RIGHT: update stats + EF for all correct first
        if right_ids:
            cur.execute("""
                UPDATE user_song u
                SET
                    total_reviews     = u.total_reviews + 1,
                    correct_reviews   = u.correct_reviews + 1,
                    ease_factor       = LEAST(2.8, u.ease_factor + 0.10),
                    last_review_round = %s
                WHERE u.discord_id = %s
                  AND u.amq_song_id = ANY(%s::BIGINT[])
            """, (current_round, player_id, right_ids))

            # 3a) learning -> mature (always), compute next interval
            cur.execute("""
                UPDATE user_song u
                SET
                    state = 'mature',
                    interval_rounds = CASE
                        WHEN u.interval_rounds <= 0 THEN 1
                        WHEN u.interval_rounds = 1 THEN 2
                        ELSE CEIL(u.interval_rounds * u.ease_factor)::BIGINT
                    END,
                    next_review_round = %s + CASE
                        WHEN u.interval_rounds <= 0 THEN 1
                        WHEN u.interval_rounds = 1 THEN 2
                        ELSE CEIL(u.interval_rounds * u.ease_factor)::BIGINT
                    END
                WHERE u.discord_id = %s
                  AND u.state = 'learning'
                  AND u.amq_song_id = ANY(%s::BIGINT[])
            """, (current_round, player_id, right_ids))

            # 3b) mature stays mature: grow interval again
            cur.execute("""
                UPDATE user_song u
                SET
                    interval_rounds = CASE
                        WHEN u.interval_rounds <= 0 THEN 1
                        WHEN u.interval_rounds = 1 THEN 2
                        ELSE CEIL(u.interval_rounds * u.ease_factor)::BIGINT
                    END,
                    next_review_round = %s + CASE
                        WHEN u.interval_rounds <= 0 THEN 1
                        WHEN u.interval_rounds = 1 THEN 2
                        ELSE CEIL(u.interval_rounds * u.ease_factor)::BIGINT
                    END
                WHERE u.discord_id = %s
                  AND u.state = 'mature'
                  AND u.amq_song_id = ANY(%s::BIGINT[])
            """, (current_round, player_id, right_ids))

            # 3c) new -> learning (BUFFERED): only promote up to slots
            if slots > 0:
                cur.execute("""
                    UPDATE user_song u
                    SET
                        state = 'learning',
                        interval_rounds = 1,
                        next_review_round = %s + 1
                    WHERE u.discord_id = %s
                      AND u.amq_song_id IN (
                          SELECT amq_song_id
                          FROM user_song
                          WHERE discord_id = %s
                            AND state = 'new'
                            AND amq_song_id = ANY(%s::BIGINT[])
                          ORDER BY next_review_round NULLS FIRST
                          LIMIT %s
                      )
                """, (current_round, player_id, player_id, right_ids, slots))

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