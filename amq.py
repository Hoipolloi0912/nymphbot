from difflib import SequenceMatcher as sm
import re
import db
import asyncio
import unicodedata
from collections import deque
from audio import AudioManager

HEADER = "https://naedist.animemusicquiz.com/"

def clean(s):
    s = s.replace(" ", "")
    normalized = unicodedata.normalize('NFKD', s)
    ascii_str = normalized.encode('ascii', 'ignore').decode('utf-8')
    return re.sub(r'[^\x00-\x7F]+', '', ascii_str)

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

        for name in self.names:
            ans = [name]

            if "(" in name and ")" in name and name.index("(") < name.index(")"):
                ans.append(name.split("(", 1)[0].strip())

            for an in ans:
                if (a == clean(an) or len(an.split(" ")) == 2 and a == clean("".join(an.split(" ")[::-1]))):
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
        self.correct = False

class Game:
    def __init__(self, settings=None, *, player_id=None, server_id=None, vc=None):
        self.count = 0
        self.score = 0
        self.player_id = player_id
        self.server_id = settings.guild.id if settings else server_id
        self.text_channel = settings.text_channel
        self.voice_channel = settings.vc if settings else vc
        self.voice_client = None
        self.songs = deque(db.get_amq_song_ids_from_user_ids(list(settings.players), settings.rounds)) if settings else deque()
        self.pending = deque()
        self.current = None
        self.alt_names = {}
        self.ending = False
        self.audio = None
        self.duration = 30

    async def start(self):
        self.voice_client = await self.voice_channel.connect()
        self.audio = AudioManager(self.voice_client, self.duration, header=HEADER, on_finished=self.next)
        await self.audio.start()
        return await self.next()

    async def next(self):
        if self.current:
            if self.current.correct:
                await self.text_channel.send(f"{self.count}: ✅ `{self.get_ans()}`")
            else:
                await self.text_channel.send(f"{self.count}: `{self.get_ans()}`")

        self.current = None

        while not self.ending:
            if not self.pending:
                try:
                    queued = await self.enqueue_next()
                    if self.ending:
                        return False

                    if not queued:
                        await self.end()
                        return False

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"Failed to queue song: {e}")
                    if self.ending:
                        return False
                    continue

            upcoming = self.pending.popleft()

            audio = self.audio
            if self.ending or audio is None:
                return False

            success = await audio.play_next()
            if self.ending:
                return False

            if not success:
                print(f"Skipping failed download: {upcoming.id}")
                continue

            self.current = upcoming
            self.count += 1
            print(f"{self.count}: {self.get_ans()}")

            try:
                await self.enqueue_next()
                if self.ending:
                    return False
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"Failed to queue next song: {e}")

            return True

        return False
    
    async def enqueue_next(self):
        rows = await self.get_rows()
        if self.ending or not rows:
            return False

        await asyncio.to_thread(self.prepare_alt_names, rows)
        if self.ending:
            return False

        self.pending.append(self.make_round(rows[0]))
        self.audio.enqueue(rows[0][1])
        return True

    async def end(self):
        if self.ending:
            return

        self.ending = True

        if self.audio:
            self.audio.on_finished = None
            await self.audio.close()
            self.audio = None

        if self.voice_client:
            await self.voice_client.disconnect()
            self.voice_client = None

        self.pending.clear()

    async def get_rows(self):
        if not self.songs:
            return []

        song_id = self.songs.popleft()
        return await asyncio.to_thread(db.fetch_from_amq_song_id, [song_id])
  
    def skip(self):
        if self.voice_client and self.voice_client.is_playing():
            self.voice_client.stop()

    def get_ans(self):
        cur = self.current
        return f"[{cur.id}] {cur.sn} by {cur.a} from {cur.jp or cur.en}"

    def prepare_alt_names(self, rows):
        raise NotImplementedError

    def make_round(self, row):
        raise NotImplementedError
    
    async def check(self, a):
        raise NotImplementedError

class GameAnime(Game):
    def prepare_alt_names(self, rows):
        ids = [row[0] for row in rows]
        alt_rows = db.fetch_alt_anime_names(ids)

        for id, name_en, name_ja in alt_rows:
            for name in (name_en, name_ja):
                if name:
                    self.alt_names.setdefault(id, set()).add(name)

    def make_round(self, row):
        return Round(*row[:6], list(self.alt_names.get(row[0], [])))

    async def check(self, a):
        a = clean(a).lower()
        tar = self.current

        if not tar:
            return False

        correct = any(sm(lambda x: x == " ", a, clean(name).lower()).ratio() > 0.9 for name in [tar.en, tar.jp] + tar.alts if name is not None)

        if correct:
            self.score += 1
            self.current.correct = True
            self.skip()

        return correct

class GameSA(Game):
    def prepare_alt_names(self, rows):
        artist_ids = [row[6] for row in rows]
        tree = db.fetch_artist_tree(artist_ids)

        self.alt_names.update({id: [name, alts, members] for id, name, alts, members in tree})
        alt_ids = [i for _, _, alts, _ in tree for i in alts]

        for id, name in db.fetch_artists_by_ids(alt_ids):
            self.alt_names.setdefault(id, [name, [], []])

    def make_round(self, row):
        return Round(*row[:6], Tree(row[6], self.alt_names))

    async def check(self, a):
        b = clean(a)
        cur = self.current

        if not cur:
            return False

        r = 0

        if not cur.guessed[0] and b == clean(cur.sn):
            cur.guessed[0] = True
            r = 2
            await self.text_channel.send(f"✅ {a}")

        if cur.alts.check(b):
            r = 2
            await self.text_channel.send(f"✅ {a}")

        if cur.alts.guessed:
            cur.guessed[1] = True

        if all(cur.guessed):
            self.score += 1
            r = 1
            self.current.correct = True
            self.skip()
            
        return r

class GameSolo(GameSA):
    def __init__(self, player_id, server_id, vc):
        super().__init__(player_id=player_id, server_id=server_id, vc=vc)

    async def get_rows(self):
        rows = await asyncio.to_thread(db.fetch_songs_srs, self.player_id, 10)
        excluded = {r.id for r in self.pending}

        if self.current:
            excluded.add(self.current.id)

        return [row for row in rows if row[0] not in excluded][:1]

    async def next(self):
        if self.ending:
            return False

        if self.current:
            if self.current.correct:
                await asyncio.to_thread(db.update_srs_correct, self.player_id, self.current.id)
            else:
                await asyncio.to_thread(db.update_srs_wrong, self.player_id, self.current.id)

        return await super().next()

game = {"Anime": GameAnime, "Song/Artist": GameSA, "Solo": GameSolo}