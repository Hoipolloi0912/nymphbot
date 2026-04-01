from difflib import SequenceMatcher as sm
import re
import db
import os
import aiohttp
import asyncio
from collections import deque

QUEUE_SIZE = 8
CACHE_SIZE = 50
CACHE_DIR = "cache"
HEADER = "https://naedist.animemusicquiz.com/"

def clean(s):
    return re.sub(r'[^\x00-\x7F]+', '', s.replace(" ", ""))

async def download_audio(song_id, url):
    os.makedirs(CACHE_DIR, exist_ok=True)
    file_path = f"{CACHE_DIR}/{song_id}.mp3"

    if os.path.exists(file_path):
        return file_path

    async with aiohttp.ClientSession() as session:
        async with session.get(HEADER+url) as resp:
            if resp.status != 200:
                raise Exception("Download failed")

            with open(file_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(8192):
                    f.write(chunk)

    return file_path

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
        self.link = link
        self.en=en
        self.jp=jp
        self.sn=sn
        self.a=a
        self.alts = alts
        self.guessed = [False,False]

class Game:
    def __init__(self, song_ids, skip_a = False):
        self.skip_a = skip_a
        self.players = 1
        self.count = 0
        self.score = 0
        self.error = 0
        self.songs = []
        self.current = None
        self.alt_names = {}
        self.init_data(song_ids)

    def getlink(self):
        return self.current.file_path if self.current else None
    
    def next(self,correct):
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
    
    def close(self):
        pass

    def init_data(self,song_ids):
        pass

class GameAnime(Game):
    def init_data(self,song_ids):
        songs = db.fetch_from_ann_song_id(song_ids)
        self.songs += songs
        rows = db.fetch_alt_anime_names([x[0] for x in songs])
        for id, name_en, name_ja in rows:
            for name in (name_en, name_ja):
                if name:
                    self.alt_names.setdefault(id, set()).add(name)

    def make_round(self, row):
        return Round(*row[:6], list(self.alt_names.get(row[0], [])))
    
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
    def init_data(self, ann_ids):
        songs = db.fetch_from_ann_song_id(ann_ids)
        self.songs += songs

        artist_ids = [song[6] for song in songs]
        tree = db.fetch_artist_tree(artist_ids)

        self.alt_names |= {id: [name, alts, members] for id, name, alts, members in tree}

        alt_ids = [i for _, _, alts, _ in tree for i in alts]
        for id, name in db.fetch_artists_by_ids(alt_ids):
            self.alt_names.setdefault(id, [name, [], []])

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

class GameTrain(GameSA):
    def __init__(self, player_id):
        self.skip_a = False
        self.players = None
        self.count = 0
        self.score = 0
        self.error = 0

        self.player_id = player_id

        self.songs = deque()
        self.refilling = False

        self.current = None
        self.alt_names = {}

    async def refill(self):
        if self.refilling or len(self.songs) >= QUEUE_SIZE:
            return
        
        self.refilling = True
        existing_files = sorted((os.path.join(CACHE_DIR, f) for f in os.listdir(CACHE_DIR) if f.endswith(".mp3")),
                                key=os.path.getmtime)
        cache_files = deque(existing_files)
        while len(cache_files) > CACHE_SIZE:
            os.remove(cache_files.popleft())

        rows = db.fetch_songs_srs(self.player_id)
        if not rows:
            return

        artist_ids = [row[6] for row in rows]
        tree = db.fetch_artist_tree(artist_ids)
        self.alt_names |= {id: [name, alts, members] for id, name, alts, members in tree}
        alt_ids = [i for _, _, alts, _ in tree for i in alts]
        for id, name in db.fetch_artists_by_ids(alt_ids):
            self.alt_names.setdefault(id, [name, [], []])

        async def download(rows):
            try:
                for row in rows:
                    try:
                        file_path = await download_audio(row[0],row[1])
                        self.songs.append((row[0],file_path,*row[2:]))
                    except Exception as e:
                        print(f"download failed: {row[0]}", e)
            finally:
                self.refilling = False

        asyncio.create_task(download(rows))
    
    async def next(self, correct=True):
        if self.current:
            if correct:
                db.update_srs_correct(self.player_id, self.current.id)
            else:
                db.update_srs_wrong(self.player_id, self.current.id)

        await self.refill()

        #wait for refill, error after 10s
        start = asyncio.get_event_loop().time()
        while len(self.songs) < 1:
            if asyncio.get_event_loop().time() - start > 30:
                print("no songs?")
                return None
            await asyncio.sleep(0.1)
        
        row = self.songs.popleft()
        self.current = self.make_round(row)
        self.count += 1
        print(f"{self.count}: {self.get_ans()}")
        return self.current.link

gamemode = {"anime":GameAnime,"sa":GameSA,"train":GameTrain}