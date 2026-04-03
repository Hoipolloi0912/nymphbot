import discord,asyncio,os,psycopg,random
from discord.ext import commands
from discord import app_commands
from discord.utils import get
from collections import defaultdict
from dotenv import load_dotenv
from amq import gamemode
from listApi import get_list
from cache_autofill import get_anime_dict, get_artist_dict, get_song_dict
import db
import random
import subprocess
import time
import aiohttp

load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")
GUILD_IDS = [discord.Object(id=int(gid.strip())) for gid in os.getenv("GUILD_IDS", "").split(",")]
HEADER = "https://naedist.animemusicquiz.com/"
DB_URL=os.getenv('DB_URL')
TEST_DURATION = 10
CHUNK_SIZE = 64 * 1024

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents, help_command=None)
games = {}
guild_locks = defaultdict(asyncio.Lock)
conn = None
amq_group = app_commands.Group(name="amq", description="start a game of amq")
anime_dict = get_anime_dict()
artist_dict = get_artist_dict()
song_dict = get_song_dict()

def get_cursor():
    global conn
    if not conn:
        conn=psycopg.connect(DB_URL)
    try: return conn.cursor()
    except psycopg.OperationalError:
        conn=psycopg.connect(DB_URL)
        return conn.cursor()
    
def get_conn():
    global conn
    if not conn:
        conn=psycopg.connect(DB_URL)
    try: return conn
    except psycopg.OperationalError:
        conn=psycopg.connect(DB_URL)
        return conn

@bot.event
async def on_ready():
    await bot.change_presence(activity=discord.Game('anime music quiz'))
    for guild in GUILD_IDS:
        bot.tree.add_command(amq_group, guild=guild)
        await bot.tree.sync(guild=guild)
    print(f"{bot.user} at your service!")

async def next(vc, gid, correct = True):
    file_path = await games[gid].next(correct)
    if not file_path:
        print("no audio")
        return False

    cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration","-of", "default=noprint_wrappers=1:nokey=1", file_path]
    duration = float(subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True).stdout)

    start_time = random.uniform(0, max(duration - 45, 0))

    source = discord.FFmpegOpusAudio(
        file_path,
        before_options=f'-ss {start_time}',
        options='-vn -af "loudnorm=I=-20:TP=-1.5:LRA=11"'
    )

    if vc.is_playing():
        vc.stop()
    vc.play(
        source,
        #after=lambda e: asyncio.run_coroutine_threadsafe(next(vc, gid), bot.loop)
    )

    return True

async def terminate(interaction):
    vc = get(bot.voice_clients, guild__id=interaction.guild.id)
    if vc:
        await vc.disconnect()
    if interaction.guild.id in games:
        games[interaction.guild.id].close()
        del games[interaction.guild.id]

@amq_group.command(name="update", description="update user's anime list")
@app_commands.describe(name="list username")
@app_commands.choices(website=[app_commands.Choice(name="anilist",value="anilist"),
                               app_commands.Choice(name="myanimelist",value="mal")])
async def user_update(interaction: discord.Interaction,
                      name: str,
                      website: str,
                      watching: bool = True,
                      completed: bool = True,
                      planning: bool = False,
                      paused: bool = False,
                      dropped: bool = False):
    anime_ids = get_list[website](name,[watching,completed,planning,paused,dropped])
    song_ids = db.get_amq_song_ids_from_anime_ids(website, anime_ids)
    await interaction.response.send_message(f"updating list to {len(song_ids)} song(s)", ephemeral=True)

    db.upsert_user(interaction.user.id)
    db.deactivate_old_songs(interaction.user.id, song_ids)
    current_round = db.get_current_round(interaction.user.id)
    db.upsert_user_song_list(interaction.user.id, song_ids, current_round)

    await interaction.followup.send(f"update sucessful.", ephemeral=True)

@amq_group.command(name="test",description="check current download speed")
async def amq_test(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)
    links = db.get_random_links(10)
    downloaded = 0
    start_time = time.time()
    timeout = aiohttp.ClientTimeout(total=None)
    count = 0

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                for link in links:
                    url = f"{HEADER}{link}?nocache={random.randint(1,999999)}"
                    async with session.get(url) as resp:
                        if resp.status != 200:continue
                        async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                            downloaded += len(chunk)
                            elapsed = time.time() - start_time
                            if elapsed >= TEST_DURATION: break
                    if time.time() - start_time >= TEST_DURATION:break
                    count +=1
                if time.time() - start_time >= TEST_DURATION:break

        elapsed = time.time() - start_time
        speed_bps = downloaded / elapsed
        speed_mbps = speed_bps / (1024 * 1024)

        await interaction.followup.send(f"Downloaded: {count} files at {speed_mbps:.2f} MB/s")

    except Exception as e:
        await interaction.followup.send(f"❌ Speed test failed:\n`{e}`")

@amq_group.command(name="practice", description="training mode")
async def amq_practice(interaction:discord.Interaction):
    lock = guild_locks.setdefault(interaction.guild.id, asyncio.Lock())
    if lock.locked():
        await interaction.response.send_message("wait")
        return
    async with lock:
        if not db.list_check(interaction.user.id):
            await interaction.response.send_message("No active songs found. Run `/update` to import your list first.",ephemeral=True)
            return
        vc = await _amq(interaction)
        if not vc:return
        await interaction.response.send_message("starting practice mode")

        games[interaction.guild.id] = gamemode["train"](interaction.user.id,interaction.guild.id)
        await next(vc,interaction.guild.id)

@amq_group.command(name="anime-list", description="play songs from your anime list")
@app_commands.describe(name="list username",
                       num ="how many rounds",
                       mode = "guess the anime name or song/artist")
@app_commands.choices(website=[app_commands.Choice(name="anilist",value="anilist"),
                               app_commands.Choice(name="myanimelist",value="mal")],
                      mode=[app_commands.Choice(name="anime",value="anime"),
                            app_commands.Choice(name="songartist",value="sa")])
async def amq_animelist(interaction: discord.Interaction,
                         name: str,
                         website: str="anilist",
                         num: app_commands.Range[int, 1, 999] =20,
                         mode: str="anime",
                         watching: bool = True,
                         completed: bool = True,
                         planning: bool = False,
                         paused: bool = False,
                         dropped: bool = False):
    lock = guild_locks.setdefault(interaction.guild.id, asyncio.Lock())
    if lock.locked():
        await interaction.response.send_message("wait")
        return
    async with lock:
        vc = await _amq(interaction)
        if not vc:return

        await interaction.response.send_message(f"starting game of [{name}]. guess the {mode}")

        anime_ids = get_list[website](name,[watching,completed,planning,paused,dropped])
        song_ids = db.get_ann_song_ids_from_anime_ids(website,anime_ids)
        if len(song_ids) <= num: random.shuffle(song_ids)
        else: song_ids = random.sample(song_ids, k=num)

        if song_ids:
            await interaction.followup.send(f"loaded {len(song_ids)} songs")
            games[interaction.guild.id] = gamemode[mode](song_ids)
            await next(vc,interaction.guild.id)
        else:
            await interaction.followup.send("no songs")
            await terminate(interaction)

async def anime_autocomplete(interaction, current: str):
    suggestions = []
    current =current.lower()
    for id, names in anime_dict.items():
        for name in names:
            if name and current in name.lower():
                label = f"{id} : {name}"
                suggestions.append(
                    app_commands.Choice(name=label, value=id)
                )
                break
        if len(suggestions) >= 25:
            break
    return suggestions[:25]

@amq_group.command(name="anime-name", description="play song/artist from an anime")
@app_commands.describe(name="anime's name",
                       num ="how many rounds",)
@app_commands.autocomplete(name=anime_autocomplete)
async def amq_animeid(interaction: discord.Interaction,
                         name: int,
                         num: app_commands.Range[int, 1, 999] =20):
    lock = guild_locks.setdefault(interaction.guild.id, asyncio.Lock())
    if lock.locked():
        await interaction.response.send_message("wait")
        return
    async with lock:
        vc = await _amq(interaction)
        if not vc:return

        await interaction.response.send_message(f"starting game of [{anime_dict[str(name)][1] or anime_dict[str(name)][0]}]. guess the songartist")

        song_ids = db.get_ann_song_ids_from_anime_ids("ann",[name,])
        if len(song_ids) <= num: random.shuffle(song_ids)
        else: song_ids = random.sample(song_ids, k=num)

        if song_ids:
            await interaction.followup.send(f"loaded {len(song_ids)} songs")
            games[interaction.guild.id] = gamemode["sa"](song_ids)
            await next(vc,interaction.guild.id)
        else:
            await interaction.followup.send("no songs")
            await terminate(interaction)

async def artist_autocomplete(interaction, current: str):
    suggestions = []
    current =current.lower()
    for id, name in artist_dict.items():
        if current in name.lower():
            label = f"{id} : {name}"
            suggestions.append(app_commands.Choice(name=label[:100], value=id))
        if len(suggestions) >= 25:
            break
    return suggestions

@amq_group.command(name="artist-name", description="play songs from an artist")
@app_commands.describe(name="artist's name",
                       num ="how many rounds",)
@app_commands.autocomplete(name=artist_autocomplete)
async def amq_animeid(interaction: discord.Interaction,
                         name: int,
                         num: app_commands.Range[int, 1, 999] =20):
    lock = guild_locks.setdefault(interaction.guild.id, asyncio.Lock())
    if lock.locked():
        await interaction.response.send_message("wait")
        return
    async with lock:
        vc = await _amq(interaction)
        if not vc:return
        await interaction.response.send_message(f"starting game of [{artist_dict[str(name)]}]. guess the song name")

        #ids = name.split(",").strip()
        song_ids = db.get_ann_song_ids_from_artist_id(name,num)
        if len(song_ids) <= num: random.shuffle(song_ids)
        else: song_ids = random.sample(song_ids, k=num)

        if song_ids:
            await interaction.followup.send(f"loaded {len(song_ids)} songs")
            games[interaction.guild.id] = gamemode["sa"](song_ids,skip_a=True)
            await next(vc,interaction.guild.id)
        else:
            await interaction.followup.send("no songs")
            await terminate(interaction)

@amq_group.command(name="join-list", description="join the current game with your anime list")
@app_commands.describe(name="list username. separate multiple entries with comma")
@app_commands.choices(website=[app_commands.Choice(name="anilist",value="anilist"),
                               app_commands.Choice(name="myanimelist",value="mal")])
async def amq_joinlist(interaction: discord.Interaction,
                         name: str,
                         website: str="anilist",
                         watching: bool = True,
                         completed: bool = True,
                         planning: bool = False,
                         paused: bool = False,
                         dropped: bool = False):
    if interaction.guild.id not in games:
        await interaction.response.send_message("no game in progress", ephemeral=True)
        return
    if not interaction.user.voice:
        await interaction.response.send_message("join a voice channel", ephemeral=True)
        return

    await interaction.response.send_message(f"adding list [{name}]")
    
    game = games[interaction.guild.id]
    num = int(len(game.songs) / game.players)

    anime_ids = get_list[website](name,[watching,completed,planning,paused,dropped])
    song_ids = db.get_ann_song_ids_from_anime_ids(website,[anime_ids,])
    if len(song_ids) <= num: random.shuffle(song_ids)
    else: song_ids = random.sample(song_ids, k=num)

    if song_ids:
        await interaction.followup.send(f"loaded {len(song_ids)} songs")
        game.init_data(song_ids)
        game.players +=1
    else:
        await interaction.followup.send("no songs")
        await terminate(interaction)

async def _amq(interaction: discord.Interaction):
    if not interaction.user.voice:
        await interaction.response.send_message("join a voice channel", ephemeral=True)
        return None
    if interaction.guild.id in games:
        await interaction.response.send_message("game already in progress", ephemeral=True)
        return None
    vc = get(bot.voice_clients, guild__id=interaction.guild.id)
    if vc and vc.channel != interaction.user.voice.channel:await vc.disconnect()
    return await interaction.user.voice.channel.connect()

async def song_autocomplete(interaction, current: str):
    suggestions = []
    current =current.lower()
    for id, names in song_dict.items():
        if current in names[0].lower():
            label = f"{id} : {names[0]} by {names[1]}"
            suggestions.append(app_commands.Choice(name=label[:100], value=id))
        if len(suggestions) >= 25:
            break
    return suggestions

@amq_group.command(name="split-info", description="get full artist info of a song")
@app_commands.describe(name="song name")
@app_commands.autocomplete(name=song_autocomplete)
async def amq_splitinfo(interaction: discord.Interaction,name: int):
    name, results = db.fetch_artist_tree_for_song(name)
    data_map = {id: (name, alt_ids or [], member_ids or []) for id, name, alt_ids, member_ids in results}
    lines = []
    stack = [(name, 0)]

    while stack:
        current_id, level = stack.pop()
        if current_id not in data_map:
            continue
        name, alt_ids, member_ids = data_map[current_id]
        lines.append("  \>  " * level + name)
        for member_id in reversed(member_ids):  # reverse to keep original order
            stack.append((member_id, level + 1))
    await interaction.response.send_message("\n".join(lines))

@amq_group.command(name="help",description="list all available commands")
async def amq_help(interaction):
    lines = ["**Slash Commands**"]
    for cmd in amq_group.commands:
        lines.append(f"`/amq {cmd.name}`   {cmd.description}")
    lines.append("\n**Prefix Commands**")
    for cmd in bot.commands:
        if not cmd.hidden:
            lines.append(f"`!{cmd.name}`   {cmd.help or 'No description'}")
    await interaction.response.send_message("\n".join(lines), ephemeral=True)

@bot.command(help="skip current song")
async def s(ctx):
    lock = guild_locks.setdefault(ctx.guild.id, asyncio.Lock())
    if lock.locked():
        return
    async with lock:
        vc = get(bot.voice_clients, guild__id=ctx.guild.id)
        if ctx.guild.id in games and vc and games[ctx.guild.id].current:
            await ctx.send(f"{games[ctx.guild.id].count}: {games[ctx.guild.id].get_ans()}")
            if not await next(vc, ctx.guild.id, False):
                await ctx.send(f"{games[ctx.guild.id].score}/{games[ctx.guild.id].count}")
                print(f"{games[ctx.guild.id].error} dead links")
                await terminate(ctx)

@bot.command(help="end current game")
async def q(ctx):
    lock = guild_locks.setdefault(ctx.guild.id, asyncio.Lock())
    if lock.locked():
        return
    async with lock:
        if ctx.guild.id not in games:
            return False
        await ctx.send("quitting")
        await terminate(ctx)

@bot.event
async def on_message(message):
    if not message.content or not message.guild or message.author.id == bot.user.id:
        return
    elif message.content.startswith("!"):
        await bot.process_commands(message)
        return
    elif message.guild.id in games:
        lock = guild_locks.setdefault(message.guild.id, asyncio.Lock())
        async with lock:
            state = games[message.guild.id].check(message.content)
            if state == 1:
                await message.channel.send(f"{games[message.guild.id].count}: ✅ {games[message.guild.id].get_ans()}")
                vc = get(bot.voice_clients, guild__id=message.guild.id)
                if vc:
                    if not await next(vc, message.guild.id):
                        await message.channel.send(f"{games[message.guild.id].score}/{games[message.guild.id].count}, {games[message.guild.id].error} dead links")
                        await terminate(message)
            elif state ==2:
                await message.channel.send(f"✅ {message.content}")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        pass
    else:
        raise error

if __name__ == "__main__":
    load_dotenv()
    bot.run(API_TOKEN)