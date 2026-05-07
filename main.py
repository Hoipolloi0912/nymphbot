import discord,asyncio,os,psycopg,random
from discord.ext import commands
from discord import app_commands
from discord.utils import get
from collections import defaultdict
from dotenv import load_dotenv
from listApi import get_list
from cache_autofill import get_anime_dict, get_artist_dict, get_song_dict
import db
import random
import time
import aiohttp
from lobby import *

load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")
GUILD_IDS = [discord.Object(id=int(gid.strip())) for gid in os.getenv("GUILD_IDS", "").split(",")]
HEADER = "https://naedist.animemusicquiz.com/"
DB_URL=os.getenv('DB_URL')

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents, help_command=None)
lobbies = {}
guild_locks = defaultdict(asyncio.Lock)
amq_group = app_commands.Group(name="amq", description="play anime music quiz")
anime_dict = get_anime_dict()
artist_dict = get_artist_dict()
song_dict = get_song_dict()

@bot.event
async def on_ready():
    await bot.change_presence(activity=discord.Game('anime music quiz'))
    for guild in GUILD_IDS:
        bot.tree.add_command(amq_group, guild=guild)
        await bot.tree.sync(guild=guild)
    print(f"{bot.user} at your service!")

@amq_group.command(name="start")
async def amq_init(interaction: discord.Interaction):
    if not interaction.user.voice:
        await interaction.response.send_message("join a voice channel", ephemeral=True)
        return
    if interaction.guild.id not in lobbies:
        lobby = Lobby(interaction.guild, interaction.user.voice.channel, interaction.user.id)
        lobbies[interaction.guild.id] = lobby
    view = LobbyView(lobbies[interaction.guild.id])
    embed = lobbies[interaction.guild.id].create_embed()
    await interaction.response.send_message(embed=embed,view=view)
    lobbies[interaction.guild.id].message = await interaction.original_response()

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
    await interaction.response.defer(thinking=True,ephemeral= True)
    anime_ids = get_list[website](name,[watching,completed,planning,paused,dropped])
    song_ids = db.get_amq_song_ids_from_anime_ids(website, anime_ids)
    db.upsert_user_song_list(interaction.user.id, song_ids)
    await interaction.followup.send(f"adding {len(song_ids)} songs to your list", ephemeral=True)

@amq_group.command(name="clear", description="clear your list")
async def user_update(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True,ephemeral= True)
    db.deactivate_songs(interaction.user.id,)
    await interaction.followup.send(f"cleared list", ephemeral=True)

@amq_group.command(name="test",description="check current download speed")
async def amq_test(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)
    MAX_DURATION = 10
    MAX_MB = 100
    CHUNK_SIZE = 64*1024
    links = db.get_random_links(100)
    downloaded = 0
    max_bytes = MAX_MB *1024 *1024
    start_time = time.time()
    timeout = aiohttp.ClientTimeout(total=None)
    count = 0

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                for link in links:
                    if time.time() - start_time >= MAX_DURATION or downloaded >= max_bytes:break
                    url = f"{HEADER}{link}?nocache={random.randint(1,999999)}"
                    async with session.get(url) as resp:
                        if resp.status != 200:continue
                        async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                            if time.time() - start_time >= MAX_DURATION or downloaded >= max_bytes:break
                            downloaded += len(chunk)
                    count +=1

        speed_bps = downloaded / (time.time() - start_time)
        speed_mbps = speed_bps / (1024 * 1024)

        await interaction.followup.send(f"Downloaded: {count} files at {speed_mbps:.2f} MB/s")

    except Exception as e:
        await interaction.followup.send(f"Speed test failed")
        print(e)

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
        if not interaction.user.voice:
            await interaction.response.send_message("join a voice channel", ephemeral=True)
            return None
        if interaction.guild.id in games:
            await interaction.response.send_message("game already in progress", ephemeral=True)
            return None
        await interaction.response.send_message("starting practice mode")

        games[interaction.guild.id] = game["Train"](interaction.user.id,interaction.guild.id,interaction.user.voice.channel)
        await games[interaction.guild.id].start()

def load_autocomplete(data, label_func, search_func=None):
    async def autocomplete(interaction: discord.Interaction,current: str):
        suggestions = []
        current = current.lower()
        for id, value in data.items():
            target = (search_func(value) if search_func else str(value))
            if current in target.lower():
                label = label_func(id, value)
                suggestions.append(app_commands.Choice(name=label[:100],value=id))
            if len(suggestions) >= 25: break
        return suggestions
    return autocomplete

anime_autocomplete = load_autocomplete(anime_dict,lambda id, name: f"{id} : {name}")
artist_autocomplete = load_autocomplete(artist_dict,lambda id, name: f"{id} : {name}")
song_autocomplete = load_autocomplete(song_dict,lambda id, names: f"{id} : {names[0]} by {names[1]}",lambda names: names[0])

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
        if ctx.guild.id in games and games[ctx.guild.id].current:
            await ctx.send(f"{games[ctx.guild.id].count}: `{games[ctx.guild.id].get_ans()}`")
            if not await games[ctx.guild.id].next(False):
                await ctx.send(f"{games[ctx.guild.id].score}/{games[ctx.guild.id].count}")
                await terminate(ctx.guild.id)

@bot.command(help="end current game")
async def q(ctx):
    lock = guild_locks.setdefault(ctx.guild.id, asyncio.Lock())
    if lock.locked():
        return
    async with lock:
        if ctx.guild.id not in games:
            return False
        await ctx.send("quitting")
        await terminate(ctx.guild.id)

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
                await message.channel.send(f"{games[message.guild.id].count}: ✅ `{games[message.guild.id].get_ans()}`")
                vc = get(bot.voice_clients, guild__id=message.guild.id)
                if vc:
                    if not await games[message.guild.id].next(True):
                        await message.channel.send(f"{games[message.guild.id].score}/{games[message.guild.id].count}")
                        await terminate(message.guild.id)
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