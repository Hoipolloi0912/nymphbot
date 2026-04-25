import discord
from amq import game

games = {}
MODES = ["Song/Artist","Anime"]

async def terminate(guild_id):
    if guild_id in games:
        await games[guild_id].end()
        del games[guild_id]

class Lobby:
    def __init__(self,guild,vc,host_id):
        self.guild = guild
        self.vc = vc
        self.players = {host_id}
        self.mode = 0
        self.rounds = 20
        self.message = None

    def add_player(self, user_id):
        self.players.add(user_id)

    def remove_player(self, user_id):    
        self.players.discard(user_id)

    def toggle_mode(self):
        self.mode = (self.mode + 1) % 2
    
    async def start(self):
        games[self.guild.id] = game[MODES[self.mode]](self)
        if not await games[self.guild.id].start():
            await terminate(self.guild.id)

    def create_embed(self):
        embed = discord.Embed(title="🎵 AMQ Lobby", color=discord.Color.blurple())
        players = sorted(f"<@{user_id}>" for user_id in self.players)
        embed.add_field(name="Mode",value=f"`{MODES[self.mode]}`")
        embed.add_field(name=f"Players ({len(self.players)})", value="\n".join(sorted(players)), inline=False)
        return embed
    
class LobbyView(discord.ui.View):
    def __init__(self, lobby):
        super().__init__(timeout=None)
        self.lobby = lobby

    async def update_message(self):
        embed = self.lobby.create_embed()
        await self.lobby.message.edit(embed=embed,view=self)

    @discord.ui.button(label="➕", style=discord.ButtonStyle.green)
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.lobby.add_player(interaction.user.id)
        await interaction.response.defer()
        await self.update_message()

    @discord.ui.button(label="➖", style=discord.ButtonStyle.red,)
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.lobby.remove_player(interaction.user.id)
        await interaction.response.defer()
        await self.update_message()

    @discord.ui.button(label="🔄", style=discord.ButtonStyle.gray,)
    async def toggle_mode(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.lobby.toggle_mode()
        await interaction.response.defer()
        await self.update_message()

    @discord.ui.button(label="▶️", style=discord.ButtonStyle.primary,row=2)
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("game starting...")
        await self.lobby.start()

    @discord.ui.button(label="⏩", style=discord.ButtonStyle.primary,row=2)
    async def start_100(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.lobby.rounds = 100
        await interaction.response.send_message("game starting...")
        await self.lobby.start()

    @discord.ui.button(label="🗑️", style=discord.ButtonStyle.red,row=2)
    async def reset(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.voice:
            self.lobby.vc = interaction.user.voice.channel
        self.lobby.players = {interaction.user.id}
        self.lobby.mode = 0
        self.lobby.rounds = 20
        await interaction.response.defer()
        await self.update_message()