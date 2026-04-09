from difflib import SequenceMatcher as sm
import re
import db
import os
import aiohttp
import asyncio
from collections import deque

QUEUE_SIZE = 5
CACHE_SIZE = 10
CACHE_DIR = "cache"
HEADER = "https://naedist.animemusicquiz.com/"
os.makedirs(CACHE_DIR, exist_ok=True)

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
        self.link = link
        self.en=en
        self.jp=jp
        self.sn=sn
        self.a=a
        self.alts = alts
        self.guessed = [False,False]

class Game:
    def __init__(self, song_ids, skip_a=False, server_id="default"):
        self.skip_a = skip_a
        self.players = 1
        self.count = 0
        self.score = 0
        self.error = 0
        self.song_ids = deque(song_ids)
        self.queue = asyncio.Queue()
        self.refill_task = None
        self.current = None
        self.alt_names = {}
        self.server_id = server_id

    def getlink(self):
        return self.current.link if self.current else None

    async def next(self, correct=True):
        if self.queue.qsize() < QUEUE_SIZE and (self.refill_task is None or self.refill_task.done()):
            self.refill_task = asyncio.create_task(self.refill())
        try:
            self.current = await asyncio.wait_for(self.queue.get(), timeout=99)
        except asyncio.TimeoutError:
            print("no songs?")
            return None

        self.count += 1
        print(f"{self.count}: {self.get_ans()}")
        return self.getlink()

    def get_ans(self):
        cur = self.current
        return f"[{cur.id}] {cur.sn} by {cur.a} from {cur.jp or cur.en}"

    async def clear_cache(self):
        directory = f"{CACHE_DIR}/{self.server_id}"
        os.makedirs(directory, exist_ok=True)

        existing_files = sorted((os.path.join(directory, f)
                                 for f in os.listdir(directory)
                                 if f.endswith(".mp3")),
                                key=os.path.getmtime,)
        cache_files = deque(existing_files)
        queue_files = set()
        tmp_queue = []

        while not self.queue.empty():
            item = await self.queue.get()
            queue_files.add(item.link)
            tmp_queue.append(item)

        for item in tmp_queue:
            await self.queue.put(item)

        while len(cache_files) > CACHE_SIZE:
            file_to_remove = cache_files.popleft()
            if file_to_remove not in queue_files:
                try:
                    os.remove(file_to_remove)
                except FileNotFoundError:
                    pass
            else:
                cache_files.append(file_to_remove)

    async def refill(self):
        await self.clear_cache()
        ids = []
        while self.song_ids and len(ids) < QUEUE_SIZE:
            ids.append(self.song_ids.popleft())

        if not ids:
            return

        rows = db.fetch_from_ann_song_id(ids)
        self.prepare_alt_names(rows)

        for row in rows:
            try:
                file_path = await self.download_audio(row[1])
                round_obj = self.make_round((row[0], file_path, *row[2:]))
                await self.queue.put(round_obj)
            except Exception as e:
                print(f"download failed: {row[0]}", e)

    async def download_audio(self, url):
        directory = f"{CACHE_DIR}/{self.server_id}"
        os.makedirs(directory, exist_ok=True)

        file_path = f"{directory}/{url}"

        if os.path.exists(file_path):
            return file_path

        async with aiohttp.ClientSession() as session:
            async with session.get(HEADER + url) as resp:
                if resp.status != 200:
                    raise Exception("Download failed")
                print("downloading "+url)
                with open(file_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(8192):
                        f.write(chunk)
                print("downloaded "+ url)

        return file_path

    def prepare_alt_names(self, rows):
        pass


class GameAnime(Game):
    def prepare_alt_names(self, rows):
        ids = [row[0] for row in rows]

        alt_rows = db.fetch_alt_anime_names(ids)

        for id, name_en, name_ja in alt_rows:
            for name in (name_en, name_ja):
                if name:
                    self.alt_names.setdefault(id, set()).add(name)

    def make_round(self, row):
        return Round(*row[:6],list(self.alt_names.get(row[0], [])),)

    def check(self, a):
        a = clean(a).lower()
        tar = self.current

        correct = any(sm(lambda x: x == " ", a, clean(name).lower()).ratio() > 0.9
                      for name in [tar.en, tar.jp] + tar.alts
                      if name is not None)

        self.score += correct
        return correct


class GameSA(Game):
    def prepare_alt_names(self, rows):
        artist_ids = [row[6] for row in rows]
        tree = db.fetch_artist_tree(artist_ids)
        self.alt_names |= {id: [name, alts, members] for id, name, alts, members in tree}
        alt_ids = [i for _, _, alts, _ in tree for i in alts]

        for id, name in db.fetch_artists_by_ids(alt_ids):
            self.alt_names.setdefault(id, [name, [], []])

    def make_round(self, row):
        return Round(*row[:6],Tree(row[6], self.alt_names))

    def check(self, a):
        a = clean(a)
        cur = self.current
        r = 0
        if not cur.guessed[0] and a == clean(cur.sn):
            cur.guessed[0] = True
            r = 2

        if self.skip_a:
            if cur.guessed[0]:
                self.score += 1
                return 1
        else:
            if cur.alts.check(a):
                r = 2
            if cur.alts.guessed:
                cur.guessed[1] = True
            if all(cur.guessed):
                self.score += 1
                r = 1

        return r


class GameTrain(GameSA):
    def __init__(self, player_id, server_id):
        self.count = 0
        self.score = 0
        self.error = 0
        self.player_id = player_id
        self.server_id = server_id
        self.song_ids = deque()
        self.queue = asyncio.Queue()
        self.refill_task = None
        self.current = None
        self.alt_names = {}

    async def refill(self):
        await self.clear_cache()
        rows = db.fetch_songs_srs(self.player_id, QUEUE_SIZE)
        if not rows:
            return

        self.prepare_alt_names(rows)

        for row in rows:
            try:
                file_path = await self.download_audio(row[1])
                round_obj = self.make_round((row[0], file_path, *row[2:]))
                await self.queue.put(round_obj)

            except Exception as e:
                print(f"download failed: {row[0]}",e)

    async def next(self, correct=True):
        if self.current:
            if correct: db.update_srs_correct(self.player_id,self.current.id)
            else: db.update_srs_wrong(self.player_id,self.current.id)

        if self.queue.qsize() < QUEUE_SIZE and (self.refill_task is None or self.refill_task.done()):
            self.refill_task = asyncio.create_task(self.refill())

        try:
            self.current = await asyncio.wait_for(self.queue.get(),timeout=50)
        except asyncio.TimeoutError:
            print("no songs?")
            return None

        self.count += 1
        print(f"{self.count}: [{self.current.link}] {self.get_ans()}")
        return self.current.link

gamemode = {"anime":GameAnime,"sa":GameSA,"train":GameTrain}