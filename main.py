import discord,asyncio,requests,os,psycopg,subprocess,random,json
from discord.ext import commands
from discord import app_commands
from discord.utils import get
from collections import defaultdict
from dotenv import load_dotenv
from amq import gamemode
from listApi import get_list
from cache_autofill import get_anime_dict, get_artist_dict, get_song_dict

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
GUILD_IDS = [discord.Object(id=int(gid.strip())) for gid in os.getenv("GUILD_IDS", "").split(",")]
HEADER = "https://naedist.animemusicquiz.com/"
DB_URL=os.getenv('DB_URL')

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
    
def get_stream_duration(url: str) -> int:
    """Return duration of audio stream in seconds"""
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'json',
        url
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        data = json.loads(result.stdout)
        return int(float(data['format']['duration']))
    except Exception as e:
        print("Failed to get duration:", e)
        return 0

@bot.event
async def on_ready():
    await bot.change_presence(activity=discord.Game('anime music quiz'))
    for guild in GUILD_IDS:
        bot.tree.add_command(amq_group, guild=guild)
        await bot.tree.sync(guild=guild)
        break
    print(f"{bot.user} at your service!")

async def next(vc, gid):
    if vc.is_playing():vc.stop()

    while True:
        path = None
        while True:
            if not games[gid].next():
                print("no link")
                return False
            path = games[gid].getlink()
            if path:
                break
            games[gid].error += 1

        link = HEADER + path

        try:
            response = requests.head(link, allow_redirects=False, timeout=2)
            response.status_code == 200
        except Exception:
            print(Exception)

        if response.status_code == 200:
            break
        print("404 not found")

    duration = get_stream_duration(link)
    start_time = start_time = random.randint(0, max(duration - 25, 0))
    source = discord.FFmpegPCMAudio(link, before_options=f'-ss {start_time} -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5',
                                        options='-vn -af "loudnorm=I=-20:TP=-1.5:LRA=11"')
    vc.play(source)
    return True

async def terminate(interaction):
    vc = get(bot.voice_clients, guild__id=interaction.guild.id)
    if vc:
        await vc.disconnect()
    if interaction.guild.id in games:
        del games[interaction.guild.id]

@amq_group.command(name="anime-list", description="play songs from your anime list")
@app_commands.describe(name="list username. separate multiple entries with comma",
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
    vc = await _amq(interaction)
    if not vc:return

    await interaction.response.send_message(f"starting game of [{name}]. guess the {mode}")
    
    names = [n.strip() for n in name.split(",") if n.strip()]
    players = len(names)
    query,params = _query_with_list_name(names,website,num,[watching,completed,planning,paused,dropped])

    with get_cursor() as curr:
        curr.execute(query,params)
        song_ids = curr.fetchall()
        if song_ids:
            await interaction.followup.send(f"loaded {len(song_ids)} songs")
            games[interaction.guild.id] = gamemode[mode](song_ids,curr,players)
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
    vc = await _amq(interaction)
    if not vc:return

    await interaction.response.send_message(f"starting game of [{anime_dict[str(name)][1] or anime_dict[str(name)][0]}]. guess the songartist")
    params = (name,num)
    with get_cursor() as curr:
        curr.execute(query_with_anime,params)
        song_ids = curr.fetchall()
        if song_ids:
            await interaction.followup.send(f"loaded {len(song_ids)} songs")
            games[interaction.guild.id] = gamemode["sa"](song_ids,curr,1,False)
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
    vc = await _amq(interaction)
    if not vc:return

    await interaction.response.send_message(f"starting game of [{artist_dict[str(name)]}]. guess the song name")
    params = ([name],num)
    with get_cursor() as curr:
        curr.execute(query_with_artist,params)
        song_ids = curr.fetchall()
        if song_ids:
            await interaction.followup.send(f"loaded {len(song_ids)} songs")
            games[interaction.guild.id] = gamemode["sa"](song_ids,curr,1,True)
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
    names = [n.strip() for n in name.split(",") if n.strip()]
    query,params = _query_with_list_name(names,website,num,[watching,completed,planning,paused,dropped])

    with get_cursor() as curr:
        curr.execute(query,params)
        song_ids = curr.fetchall()
        if song_ids:
            await interaction.followup.send(f"loaded {len(song_ids)} songs")
            game.init_data(curr,song_ids)
            game.players +=1
        else:
            await interaction.followup.send("no songs")
            await terminate(interaction)

@amq_group.command(name="join-anime-name", description="join the current game with songs from an anime")
@app_commands.describe(name="anime's name")
@app_commands.autocomplete(name=anime_autocomplete)
async def amq_animejoin(interaction: discord.Interaction,name: int,):
    if interaction.guild.id not in games:
        await interaction.response.send_message("no game in progress", ephemeral=True)
        return
    if not interaction.user.voice:
        await interaction.response.send_message("join a voice channel", ephemeral=True)
        return

    await interaction.response.send_message(f"adding list [{anime_dict[str(name)][1] or anime_dict[str(name)][0]}]")
    game = games[interaction.guild.id]
    num = int(len(game.songs) / game.players)
    params = (name,num)
    with get_cursor() as curr:
        curr.execute(query_with_anime,params)
        song_ids = curr.fetchall()
        if song_ids:
            await interaction.followup.send(f"loaded {len(song_ids)} songs")
            game.init_data(curr,song_ids)
            game.players+=1
        else:
            await interaction.followup.send("no songs")
            await terminate(interaction)

@amq_group.command(name="join-artist-name", description="join the current game with songs of an artist")
@app_commands.describe(name="artist's name")
@app_commands.autocomplete(name=artist_autocomplete)
async def amq_artistjoin(interaction: discord.Interaction,name: int,):
    if interaction.guild.id not in games:
        await interaction.response.send_message("no game in progress", ephemeral=True)
        return
    if not interaction.user.voice:
        await interaction.response.send_message("join a voice channel", ephemeral=True)
        return

    await interaction.response.send_message(f"adding list [{artist_dict[str(name)]}]")
    game = games[interaction.guild.id]
    num = int(len(game.songs) / game.players)
    params = ([name],num)
    with get_cursor() as curr:
        curr.execute(query_with_artist,params)
        song_ids = curr.fetchall()
        if song_ids:
            await interaction.followup.send(f"loaded {len(song_ids)} songs")
            game.init_data(curr,song_ids)
            game.players+=1
        else:
            await interaction.followup.send("no songs")
            await terminate(interaction)

async def _amq(interaction: discord.Interaction):
    lock = guild_locks.setdefault(interaction.guild.id, asyncio.Lock())
    if lock.locked():
        await interaction.response.send_message("wait.",ephemeral=True)
        return False
    async with lock:
        if not interaction.user.voice:
            await interaction.response.send_message("join a voice channel", ephemeral=True)
            return
        if interaction.guild.id in games:
            await interaction.response.send_message("game in progress. try /join", ephemeral=True)
            return
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
    cur = get_cursor()
    cur.execute("""
        select artist_id from song where amq_song_id = %s
                """,(name,))
    name = cur.fetchone()[0]
    cur.execute("""
        WITH RECURSIVE artist_tree AS (
            SELECT * FROM artist WHERE id = ANY(%s)
            UNION
            SELECT a.* FROM artist a
            JOIN artist_tree at ON a.id = ANY(at.member_id)
        )
        SELECT * FROM artist_tree;
    """, ([name],))
    results = cur.fetchall()
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

def _query_with_list_name(names,website,num,flags):
    ids = set()
    for name in names:
        ids.update(get_list[website](name,flags))
    ids = list(ids)
    placeholders = ','.join(['%s'] * len(ids))
    query = f"""
    select * from (
        select distinct on (a.amq_song_id) a.ann_song_id 
        from anison a 
        join anime b on a.anime_id = b.ann_id
        join song c on a.amq_song_id = c.amq_song_id
        where b.{website}_id in ({placeholders}) and a.link IS NOT NULL and c.dub IS FALSE and c.rebroad IS FALSE
        order by a.amq_song_id, random()
    )as sub
    order by random()
    limit %s;
    """
    params = ids + [num]
    return query,params

query_with_anime = f"""
    select * from (
        select distinct on (a.amq_song_id) a.ann_song_id 
        from anison a 
        join anime b on a.anime_id = b.ann_id
        join song c on a.amq_song_id = c.amq_song_id
        where b.ann_id = %s and a.link IS NOT NULL and c.dub IS FALSE and c.rebroad IS FALSE
        order by a.amq_song_id, random()
    )as sub
    order by random()
    limit %s;
    """

query_with_artist = f"""
    WITH RECURSIVE containing_groups AS (
    -- Base: input artist ID(s)
    SELECT id, name, member_id
    FROM artist
    WHERE id = ANY(%s)

    UNION

    -- Recursively find all groups that contain them
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
        AND a.link IS NOT NULL
        AND c.dub IS FALSE
        AND c.rebroad IS FALSE
    ORDER BY a.amq_song_id, random()
    )
    SELECT *
    FROM distinct_songs
    ORDER BY random()
    LIMIT %s;
    """

@bot.command(help="skip current song")
async def s(ctx):
    lock = guild_locks.setdefault(ctx.guild.id, asyncio.Lock())
    if lock.locked():
        return
    async with lock:
        vc = get(bot.voice_clients, guild__id=ctx.guild.id)
        if not(ctx.guild.id not in games or not vc):
            await ctx.send(f"{games[ctx.guild.id].count}: {games[ctx.guild.id].get_ans()}")
            if not await next(vc, ctx.guild.id):
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
    bot.run(API_TOKEN)