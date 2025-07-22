from difflib import SequenceMatcher as sm
import random,re

def clean(s):
    return re.sub(r'[^\x00-\x7F]+', '', s.replace(" ", ""))

class Tree:
    def __init__(self,root_id,data):
        name, alt_names, members = data.get(root_id)
        self.names = [name] + [data[alt_id][0] for alt_id in alt_names]
        self.members=[]
        self.guessed = False
        for member_id in members:
            self.members.append(Tree(member_id,data))
    
    def check(self,a):
        if self.guessed:return False

        for ans in self.names:
            if a == clean(ans) or (len(ans.split(" ")) == 2 and a == clean(" ".join(ans.split(" ")[::-1]))):
                self.guessed = True
                return True
    
        members_completed = []
        matched = False
        for member in self.members:
            if member.check(a): matched = True
            members_completed.append(member.guessed)
        if matched:
            if all(members_completed):
                self.guessed = True
            return True
        return False

class Round:
    def __init__(self,id,link,en,jp,sn,a,alts):
        self.id = id
        self.link=link
        self.en=en
        self.jp=jp
        self.sn=sn
        self.a=a
        self.alts = alts
        self.guessed = [False,False]

class Game:
    def __init__(self, song_ids, cur, players, skip_a = False):
        self.skip_a = skip_a
        self.players = players
        self.count = 0
        self.score = 0
        self.error = 0
        self.songs = []
        self.current = None
        self.alt_names = {}
        self.init_data(cur,song_ids)

    def getlink(self):
        return self.current.link if self.current else None
    
    def next(self):
        try:
            row = self.songs.pop()
            self.current = self.make_round(row)
            self.count += 1
            print(f"{self.count}: {self.get_ans()}")
        except IndexError:
            return False
        return True

    def get_ans(self):
        cur = self.current
        return f"[{cur.id}] {cur.sn} by {cur.a} from {cur.jp or cur.en}"

class GameAnime(Game):
    def init_data(self,cur,song_ids):
        song_ids = [x[0] for x in song_ids]
        placeholders = ','.join(['%s'] * len(song_ids))

        cur.execute(f"""
            SELECT a.amq_song_id, a.link, d.name_en, d.name_ja, b.name, c.name
            FROM anison a
            JOIN song b ON a.amq_song_id = b.amq_song_id
            JOIN artist c ON b.artist_id = c.id
            JOIN anime d ON a.anime_id = d.ann_id
            WHERE a.ann_song_id IN ({placeholders})
            ORDER BY RANDOM();
        """, song_ids)
        self.songs += cur.fetchall()

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
        """, song_ids * 2)

        rows = cur.fetchall()
        for id, name_en, name_ja in rows:
            for name in (name_en, name_ja):
                if name:
                    self.alt_names.setdefault(id, set()).add(name)
        
        random.shuffle(self.songs)

    def make_round(self, row):
        return Round(*row, list(self.alt_names.get(row[0], [])))
    
    def check(self, a):
        a = clean(a).lower()
        tar = self.current
        correct =  any(
            sm(lambda x: x == " ", a, clean(name).lower()).ratio() > 0.9
            for name in [tar.en,tar.jp]+tar.alts
            if name is not None
        )
        self.score += correct
        return correct

class GameSA(Game):
    def init_data(self,cur,song_ids):
        song_ids = [x[0] for x in song_ids]
        placeholders = ','.join(['%s'] * len(song_ids))
        cur.execute(f"""
            SELECT a.amq_song_id, a.link, d.name_en, d.name_ja, b.name, c.name, c.id
            FROM anison a
            JOIN song b ON a.amq_song_id = b.amq_song_id
            JOIN artist c ON b.artist_id = c.id
            JOIN anime d ON a.anime_id = d.ann_id
            WHERE a.ann_song_id IN ({placeholders})
            ORDER BY RANDOM();
        """, song_ids)
        songs = cur.fetchall()
        self.songs += songs

        artist_ids = [song[6] for song in songs]
        cur.execute("""
            WITH RECURSIVE artist_tree AS (
                SELECT * FROM artist WHERE id = ANY(%s)
                UNION
                SELECT a.* FROM artist a
                JOIN artist_tree at ON a.id = ANY(at.member_id)
            )
            SELECT * FROM artist_tree;
        """, (artist_ids,))
        results = cur.fetchall()
        self.alt_names |= {id: [name, alts, members] for id, name, alts, members in results}

        alt_ids = [i for row in results for i in row[2]]
        cur.execute("""
            SELECT id,name FROM artist WHERE id = ANY(%s)
        """, (alt_ids,))
        results = cur.fetchall()
        for id, name in results:
            if id not in self.alt_names:
                self.alt_names[id] = [name, [], []]
        
        random.shuffle(self.songs)

    def make_round(self, row):
        return Round(*row[:6], Tree(row[6], self.alt_names))
    
    def check(self,a):
        a = clean(a)
        cur = self.current
        r=0
        if not cur.guessed[0] and a == clean(cur.sn):
            cur.guessed[0] = True
            r= 2

        if self.skip_a:
            if cur.guessed[0]:
                self.score +=1
                return 1
        else:
            if cur.alts.check(a):
                r=2
            if cur.alts.guessed:
                cur.guessed[1]=True
            if all(cur.guessed):
                self.score +=1
                r=1
            return r

gamemode = {"anime":GameAnime,"sa":GameSA}