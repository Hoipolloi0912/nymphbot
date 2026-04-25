from difflib import SequenceMatcher as sm
import re
import db
import os
import aiohttp
import asyncio
import subprocess
import random
import shutil
from collections import deque
from discord import FFmpegOpusAudio

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
    def __init__(self, settings):
        self.skip_a = False
        self.count = 0
        self.score = 0
        self.song_ids = deque(db.get_amq_song_ids_from_user_ids(list(settings.players),settings.rounds))
        self.queue = asyncio.Queue()
        self.refill_lock = asyncio.Lock()
        self.current = None
        self.alt_names = {}
        self.server_id = settings.guild.id
        self.vc = settings.vc
        self.trash = []
    
    async def start(self):
        self.vc = await self.vc.connect()
        return await self.next()

    async def end(self):
        await self.vc.disconnect()
        path = f"{CACHE_DIR}/{self.server_id}"
        if path and os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)

    def getlink(self):
        return self.current.link if self.current else None

    async def next(self):
        if not self.song_ids and self.queue.empty() and not self.refill_lock.locked():
            
            return False
        if self.song_ids and self.queue.qsize() < QUEUE_SIZE:
            if not self.refill_lock.locked():
                self.refill_task = asyncio.create_task(self.refill())
        try:
            self.current = await asyncio.wait_for(self.queue.get(), timeout=20)
        except asyncio.TimeoutError:
            print("no songs?")
            return False

        self.count += 1
        print(f"{self.count}: {self.get_ans()}")
        
        file_path = self.getlink()
        cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration","-of", "default=noprint_wrappers=1:nokey=1", file_path]
        duration = float(subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True).stdout)
        start_time = random.uniform(0, max(duration - 45, 0))
        source = FFmpegOpusAudio(file_path,
                                 before_options=f'-ss {start_time}',
                                 options='-vn -af "loudnorm=I=-20:TP=-1.5:LRA=11"')

        if self.vc.is_playing():self.vc.stop()
        self.vc.play(source,)
        if self.trash:
            try:os.remove(self.trash.pop(0))
            except (PermissionError, FileNotFoundError):pass
        return True

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

        while len(existing_files) > CACHE_SIZE:
            file_to_remove = existing_files.pop(0)
            try:
                os.remove(file_to_remove)
            except FileNotFoundError:
                pass

    async def refill(self):
        async with self.refill_lock:
            await self.clear_cache()
            ids = []
            while self.song_ids and len(ids) < QUEUE_SIZE:
                ids.append(self.song_ids.popleft())
            if not ids:return False

            rows = db.fetch_from_amq_song_id(ids)
            self.prepare_alt_names(rows)

            for row in rows:
                try:
                    file_path = await self.download_audio(row[1])
                    round_obj = self.make_round((row[0], file_path, *row[2:]))
                    await self.queue.put(round_obj)
                    print(row[0])
                except Exception as e:
                    print(f"download failed: {row[0]}", e)

    async def download_audio(self, url):
        directory = f"{CACHE_DIR}/{self.server_id}"
        os.makedirs(directory, exist_ok=True)

        file_path = f"{directory}/{url}"

        async with aiohttp.ClientSession() as session:
            async with session.get(HEADER + url) as resp:
                if resp.status != 200:
                    raise Exception("Download failed")
                print("downloading "+url)
                with open(file_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(8192):
                        f.write(chunk)

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
        if not tar: return False

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
        if not cur: return False
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

game = {"Anime":GameAnime,"Song/Artist":GameSA,"Train":GameTrain}