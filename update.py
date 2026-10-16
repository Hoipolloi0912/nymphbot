import os
from dotenv import load_dotenv
import asyncio
import logging
import aiohttp
import client
import db
import listApi

load_dotenv()

LIB_URL = "https://animemusicquiz.com/libraryMasterList"
BATCH_SIZE = 100
MAX_ATTEMPTS = 3
REQUEST_DELAY = 1
RESPONSE_WAIT = 10

logger = logging.getLogger("bot")
def init_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,
    )

async def get_libraryMaster():
    timeout = aiohttp.ClientTimeout(total=300)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(LIB_URL) as response:
            response.raise_for_status()
            data = await response.json(content_type=None)

    logger.info("Master list downloaded")
    return data

def load_names(data):
    animes = []
    songs = []
    song_types = ("OP", "ED", "INS")

    for anime in data["animeMap"].values():
        ann_id = anime["annId"]
        names = anime.get("mainNames") or {}

        animes.append((
            ann_id,
            names.get("JA"),
            names.get("EN"),
        ))

        links = anime.get("songLinks") or {}

        for song_type in song_types:
            for song in links.get(song_type, []):
                songs.append((
                    song["annSongId"],
                    ann_id,
                    song_type,
                    song["songId"],
                    bool(song.get("uploaded")),
                    bool(song.get("rebroadcast")),
                    bool(song.get("dub")),
                ))

    artists = [
        (
            a["songArtistId"],
            a["name"],
            a.get("altNameLinks") or [],
            [],
        )
        for a in data["artistMap"].values()
    ]

    artists += [
        (
            g["groupId"],
            g["name"],
            g.get("altNameLinks") or [],
            g.get("artistMembers") or g.get("groupMembers") or [],
        )
        for g in data["groupMap"].values()
    ]

    songz = [
        (
            s["songId"],
            s["name"],
            s.get("songArtistId") or s.get("songGroupId"),
            s.get("category"),
        )
        for s in data["songMap"].values()
    ]
    
    db.upsert_master_list(animes, artists, songz, songs)


class AMQCollector:
    def __init__(self):
        self.songs = {}
        self.animes = {}
        self.pending_songs = {}
        self.pending_animes = {}
        self.login_ready = asyncio.Event()

    def on_login(self, data):
        self.login_ready.set()
        logger.info("AMQ login complete")

    def on_song(self, data):
        song_id = data.get("annSongId")
        filename = data.get("fileName")

        if song_id is None or not filename:
            logger.warning("Invalid song response: %s", data)
            return

        if song_id not in self.pending_songs:
            logger.warning("Unexpected or late song response: %s", song_id)
            return
    
        self.songs[song_id] = filename

        future = self.pending_songs.get(song_id)
        if future and not future.done():
            future.set_result(filename)

    def on_anime(self, data):
        anime_id = data.get("annId")

        if anime_id is None:
            logger.warning("Invalid anime response: %s", data)
            return
        
        if anime_id not in self.pending_animes:
            logger.warning("Unexpected or late song response: %s", anime_id)
            return

        self.animes[anime_id] = data

        future = self.pending_animes.get(anime_id)
        if future and not future.done():
            future.set_result(data)

    def register(self):
        client.register_handler(
            client.ServerEvent.login_complete,
            self.on_login,
        )
        client.register_handler(
            client.ServerEvent.get_song_info,
            self.on_song,
        )
        client.register_handler(
            client.ServerEvent.get_anime_info,
            self.on_anime,
        )

async def fetch_batches(
    ids,
    results,
    pending_futures,
    request_func,
    save_func,
    row_func,
):
    total = len(ids)
    logger.info("Fetching %d entries", total)

    loop = asyncio.get_running_loop()

    for start in range(0, total, BATCH_SIZE):
        batch = ids[start:start + BATCH_SIZE]

        # Register all expected responses before sending.
        futures = {
            item_id: loop.create_future()
            for item_id in batch
            if item_id not in results
        }

        pending_futures.update(futures)

        try:
            for attempt in range(1, MAX_ATTEMPTS + 1):
                missing = [
                    item_id
                    for item_id in batch
                    if item_id not in results
                ]

                if not missing:
                    break

                for item_id in missing:
                    await request_func(item_id)
                    await asyncio.sleep(REQUEST_DELAY)

                # Wait until every expected response arrives,
                # or until the timeout expires.
                await asyncio.wait(
                    [
                        pending_futures[item_id]
                        for item_id in missing
                    ],
                    timeout=30,
                    return_when=asyncio.ALL_COMPLETED,
                )

                missing = [
                    item_id
                    for item_id in batch
                    if item_id not in results
                ]

                if missing:
                    logger.warning(
                        "Attempt %d: %d responses missing",
                        attempt,
                        len(missing),
                    )

            missing = [
                item_id
                for item_id in batch
                if item_id not in results
            ]

            if missing:
                raise TimeoutError(
                    f"Failed to receive responses for IDs: {missing}"
                )

            # At this point, EVERY ID in the batch has a response.
            rows = [
                row_func(item_id, results[item_id])
                for item_id in batch
            ]

            await asyncio.to_thread(save_func, rows)
            saved = True

            logger.info(
                "Batch %d: %d/%d saved",
                start // BATCH_SIZE + 1,
                len(rows),
                len(batch),
            )

        finally:
                for item_id in batch:
                    pending_futures.pop(item_id, None)

                    future = futures.get(item_id)
                    if future is not None and not future.done():
                        future.cancel()

                    if saved:
                        results.pop(item_id, None)

async def load_links(collector):
    ids = await asyncio.to_thread(db.get_missing_song_ids)

    await fetch_batches(
        ids=ids,
        results=collector.songs,
        pending_futures=collector.pending_songs,
        request_func=lambda item_id: client.get_song_info(item_id, True),
        save_func=db.update_song_data,
        row_func=lambda item_id, filename: (filename, item_id),
    )

async def load_anime_info(collector):
    ids = await asyncio.to_thread(db.get_missing_anime_ids)

    await fetch_batches(
        ids=ids,
        results=collector.animes,
        pending_futures=collector.pending_animes,
        request_func=client.get_anime_info,
        save_func=db.update_anime_data,
        row_func=lambda item_id, data: (
            data.get("aniListId"),
            data.get("malId"),
            data.get("kitsuId"),
            data.get("genres"),
            data.get("tags"),
            item_id,
        ),
    )

async def interrogate():
    collector = AMQCollector()
    collector.register()

    try:
        await asyncio.wait_for(
            client.connect(
                client.sio,
                username=os.environ["AMQ_USERNAME"],
                password=os.environ["AMQ_PASSWORD"],
            ),
            timeout=60,
        )

        await asyncio.wait_for(
            collector.login_ready.wait(),
            timeout=30,
        )

        await load_links(collector)
        await load_anime_info(collector)

    finally:
        if client.sio.connected:
            await client.sio.disconnect()

async def main():
    init_logger()
    print("test")
    logger.info("Starting weekly update")

    #refresh api token
    await asyncio.to_thread(listApi.refresh_mal_token)

    #update amq database
    data = await get_libraryMaster()
    await asyncio.to_thread(load_names, data)
    del data
    await interrogate()

    #update user lists
    db.update_all_users()

    db.log_update()
    logger.info("Weekly db update completed successfully")

if __name__ == "__main__":
    asyncio.run(main())
