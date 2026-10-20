import discord
from amq import game
import db
import asyncio

games = {}
MODES = ["Song/Artist","Anime"]

async def terminate(guild_id):
    if guild_id in games:
        await games[guild_id].end()
        del games[guild_id]

class Lobby:
    def __init__(self, guild, vc, text_channel, host_id):
        self.guild = guild
        self.vc = vc
        self.text_channel = text_channel
        self.players = set()
        self.mode = 0
        self.rounds = 0
        self.message = None
        self.host_id = host_id

    @classmethod
    async def create(cls, guild, vc, text_channel, host_id):
        lobby = cls(guild, vc, text_channel, host_id)
        needs_list = await lobby.add_player(host_id)
        return lobby, needs_list

    async def add_player(self, user_id):
        needs_list = await asyncio.to_thread(db.add_user, user_id)
        self.players.add(user_id)
        return needs_list

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
    
class KickPlayerView(discord.ui.View):
    def __init__(self, lobby, lobby_view):
        super().__init__(timeout=60)
        self.lobby = lobby
        self.lobby_view = lobby_view

    @discord.ui.select(
        cls=discord.ui.UserSelect,
        placeholder="Select a player to kick"
    )
    async def select_player(
        self,
        interaction: discord.Interaction,
        select: discord.ui.UserSelect
    ):
        if interaction.user.id != self.lobby.host_id:
            await interaction.response.send_message(
                "Only the lobby owner can kick players.",
                ephemeral=True
            )
            return

        player = select.values[0]

        if player.id == self.lobby.host_id:
            await interaction.response.send_message(
                "You cannot kick yourself.",
                ephemeral=True
            )
            return

        if player.id not in self.lobby.players:
            await interaction.response.send_message(
                "That user isn't in the lobby.",
                ephemeral=True
            )
            return

        self.lobby.remove_player(player.id)

        await interaction.response.edit_message(
            content=f"Kicked {player.mention} from the lobby.",
            view=None
        )

        await self.lobby_view.update_message()
        self.stop()

class LobbyView(discord.ui.View):
    def __init__(self, lobby):
        super().__init__(timeout=None)
        self.lobby = lobby

    async def update_message(self):
        embed = self.lobby.create_embed()
        await self.lobby.message.edit(embed=embed,view=self)

    @discord.ui.button(label="➕", style=discord.ButtonStyle.green)
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        needs_list = await self.lobby.add_player(interaction.user.id)

        if needs_list:
            await interaction.response.send_message("joined without anime list. you can use `/amq anime-list` to link your MAL or AniList.",
                                                    ephemeral=True, view=ListReminderView(interaction.user.id))
        else:
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
        self.lobby.rounds = 10
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
        await interaction.response.defer()
        await self.update_message()

    @discord.ui.button(label="Kick", style=discord.ButtonStyle.red, row=2)
    async def kick(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.lobby.host_id:
            await interaction.response.send_message(
                "Only the lobby owner can kick players.",
                ephemeral=True
            )
            return

        if len(self.lobby.players) <= 1:
            await interaction.response.send_message(
                "There are no players to kick.",
                ephemeral=True
            )
            return

        await interaction.response.send_message(
            "Select a player to kick:",
            view=KickPlayerView(self.lobby, self),
            ephemeral=True
        )

class ListReminderView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=180)
        self.user_id = user_id

    @discord.ui.button(
        label="Don't show again",
        style=discord.ButtonStyle.secondary
    )
    async def disable(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "This reminder isn't for you.",
                ephemeral=True
            )
            return

        await asyncio.to_thread(db.mute_tips, self.user_id)

        await interaction.response.edit_message(
            content="You won't see this reminder again.",
            view=None
        )
        self.stop()