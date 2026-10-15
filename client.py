import logging
import json
import aiohttp
import socketio
import inspect
import enum

logger = logging.getLogger("bot")

BASE_URL = "https://animemusicquiz.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://animemusicquiz.com/",
    "Origin": "https://animemusicquiz.com",
}

async def login(session, username, password):
    async with session.post(
        BASE_URL + "/signIn",
        data={"username": username, "password": password},
        headers=HEADERS,
    ) as res:
        text = await res.text()
        logger.info(
            "Login: status=%s, url=%s",
            res.status,
            res.url
        )
        res.raise_for_status()


async def get_token(username: str, password: str) -> dict:
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        await login(session, username, password)

        async with session.get(BASE_URL + "/socketToken") as res:
            text = await res.text()

            logger.info(
                "Socket token: status=%s, content_type=%s, url=%s",
                res.status,
                res.content_type,
                res.url
            )

            res.raise_for_status()
            return json.loads(text)

async def connect(
    sio: socketio.AsyncClient,
    username: str,
    password: str,
):
    token = await get_token(username, password)

    await sio.connect(
        f"https://socket.animemusicquiz.com:{token['port']}?token={token['token']}",
        headers=HEADERS,
    )

#===client===
sio = socketio.AsyncClient()
SESSION_ID = None

class ServerEvent(str, enum.Enum):
    login_complete = "login complete"
    player_count = "online player count change"
    get_song_info = "get song extended info"
    get_anime_info = "get anime extended info"

    default = ""

class ClientEvent(str, enum.Enum):
    get_song_info = "get song extended info"
    get_anime_info = "get anime extended info"

HANDLERS = {
    ServerEvent.login_complete: lambda _: (),
    ServerEvent.player_count: lambda _: (),
}

def register_handler(command: ServerEvent, handler):
    global HANDLERS
    HANDLERS[command] = handler

async def get_song_info(ann_song_id: int, include_filenames: bool):
    # TODO: unify with pydantic models, make structure generic
    await sio.emit("command", {
        "type": "library",
        "command": ClientEvent.get_song_info,
        "data": {
            "annSongId": ann_song_id,
            "includeFileNames": include_filenames,
        }
    })

async def get_anime_info(ann_anime_id: int):
    # TODO: unify with pydantic models, make structure generic
    await sio.emit("command", {
        "type": "library",
        "command": ClientEvent.get_anime_info,
        "data": {
            "annId": ann_anime_id,
        }
    })

@sio.on("connect")
async def on_connect():
    logger.info("Connected")

@sio.on("disconnect")
async def on_disconnect():
    logger.info("Disconnected")

@sio.on("sessionId")
async def on_session_id(data: int):
    global SESSION_ID
    SESSION_ID = data
    logger.info("Session ID: %s", data)

@sio.on("command")
async def on_command(data: dict):
    global HANDLERS
    command = data["command"]
    data = data["data"]
    if command in HANDLERS:
        task = HANDLERS[command](data)
        if inspect.isawaitable(task):
            await task
    else:
        logger.debug("Unhandled command: %s", command)
        if ServerEvent.default in HANDLERS:
            task = HANDLERS[ServerEvent.default](command, data)
            if inspect.isawaitable(task):
                await task