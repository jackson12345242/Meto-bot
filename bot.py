"""
Litecoin Balance Tracker - Discord Bot
-------------------------------------
Watches Litecoin addresses. DMs the owner whenever a balance changes (any
amount, confirmed or pending), and offers /balance, /wallet, and /imlimited
slash commands.

Config comes from environment variables:
    DISCORD_TOKEN        - the bot's token
    DISCORD_USER_ID       - your Discord user ID (numeric), who gets DMed
    LTC_ADDRESSES          - comma-separated list of Litecoin addresses
    POLL_SECONDS            - how often to check, default 8
    PREFIX                  - command prefix, default "?"

Balances persist in balances.json (created automatically) so restarts don't
cause false "change" notifications.
"""

import asyncio
import json
import os
import time
from datetime import datetime, timezone

import aiohttp
import discord
from discord.ext import commands, tasks

BALANCES_PATH = "balances.json"

COIN_META = {
    "LTC": {"icon": "🪙", "network": "Litecoin", "coingecko_id": "litecoin"},
}

PRICE_CACHE = {"data": {}, "ts": 0.0}

# Minimum USD value a balance change must cross before the owner gets DMed.
# Set to 0 so every detected change notifies, no matter how small.
MIN_NOTIFY_USD = 0

# Shared embed color (white) used across all commands/notifications.
EMBED_COLOR = discord.Color(0xFFFFFF)


def get_setting(env_var, default=None, required=True):
    value = os.environ.get(env_var, default)
    if required and value is None:
        raise SystemExit(f"Missing required setting: set {env_var} env var")
    return value


DISCORD_TOKEN = get_setting("DISCORD_TOKEN")
DISCORD_USER_ID = int(get_setting("DISCORD_USER_ID"))
LTC_ADDRESSES = [a.strip() for a in get_setting("LTC_ADDRESSES", default="", required=False).split(",") if a.strip()]
POLL_SECONDS = int(get_setting("POLL_SECONDS", default=8, required=False))
PREFIX = get_setting("PREFIX", default="?", required=False)


def load_balances():
    if not os.path.exists(BALANCES_PATH):
        return {}
    with open(BALANCES_PATH, "r") as f:
        return json.load(f)


def save_balances(data):
    with open(BALANCES_PATH, "w") as f:
        json.dump(data, f, indent=2)


balances = load_balances()

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix=PREFIX, intents=intents)


# ---------------------------------------------------------------------------
# Balance / price fetch helpers
# ---------------------------------------------------------------------------

async def get_ltc_balance_only(session: aiohttp.ClientSession, address: str):
    """Fast, lightweight balance-only check (no tx history) - used as a fallback."""
    url = f"https://api.blockcypher.com/v1/ltc/main/addrs/{address}/balance"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            return data.get("balance", 0) / 1e8
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return None


async def get_ltc_info(session: aiohttp.ClientSession, address: str):
    """Returns dict with balance (LTC float) and unconfirmed_txrefs (pending txs).
    Falls back to a fast balance-only check if the full endpoint is slow/fails,
    so a slow pending-tx lookup never blocks the regular balance update."""
    url = f"https://api.blockcypher.com/v1/ltc/main/addrs/{address}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return {
                    "balance": data.get("balance", 0) / 1e8,
                    "unconfirmed_txrefs": data.get("unconfirmed_txrefs", []),
                }
            print(f"[warn] LTC info fetch failed for {address}: HTTP {resp.status}, falling back")
    except (aiohttp.ClientError, asyncio.TimeoutError):
        print(f"[warn] LTC info fetch timed out for {address}, falling back to balance-only")

    fallback_balance = await get_ltc_balance_only(session, address)
    if fallback_balance is None:
        return None
    return {"balance": fallback_balance, "unconfirmed_txrefs": []}


async def get_usd_prices(session: aiohttp.ClientSession):
    """Returns {'LTC': price}, cached for 60 seconds."""
    now = time.monotonic()
    if PRICE_CACHE["data"] and now - PRICE_CACHE["ts"] < 60:
        return PRICE_CACHE["data"]

    url = "https://api.coingecko.com/api/v3/simple/price?ids=litecoin&vs_currencies=usd"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                prices = {
                    "LTC": data.get("litecoin", {}).get("usd", 0),
                }
                PRICE_CACHE["data"] = prices
                PRICE_CACHE["ts"] = now
                return prices
    except (aiohttp.ClientError, asyncio.TimeoutError):
        pass

    return PRICE_CACHE["data"] or {"LTC": 0}


# ---------------------------------------------------------------------------
# Background polling loop
# ---------------------------------------------------------------------------

@tasks.loop(seconds=POLL_SECONDS)
async def poll_balances():
    owner = bot.get_user(DISCORD_USER_ID) or await bot.fetch_user(DISCORD_USER_ID)

    changed = False
    async with aiohttp.ClientSession() as session:
        ltc_results = await asyncio.gather(
            *(get_ltc_info(session, address) for address in LTC_ADDRESSES)
        )

        prices = await get_usd_prices(session)

        for address, info in zip(LTC_ADDRESSES, ltc_results):
            if info is None:
                continue
            new_confirmed = info["balance"]
            key = f"ltc:{address}"

            # LTC entries are stored as {"confirmed": float, "pending": {txid: value}}.
            # Older balances.json files stored a plain float for LTC - upgrade in place.
            stored = balances.get(key)
            if isinstance(stored, dict):
                old_confirmed = stored.get("confirmed")
                old_pending = stored.get("pending", {})
            else:
                old_confirmed = stored
                old_pending = {}

            current_pending = {}
            for tx in info.get("unconfirmed_txrefs", []):
                txid = tx.get("tx_hash")
                if not txid:
                    continue
                is_incoming = tx.get("tx_output_n", -1) != -1
                current_pending[txid] = {
                    "value": tx.get("value", 0) / 1e8,
                    "incoming": is_incoming,
                }

            # New pending tx we haven't alerted on yet -> "seen in mempool" notice.
            for txid, tx in current_pending.items():
                if txid not in old_pending:
                    pending_usd = tx["value"] * prices.get("LTC", 0)
                    if pending_usd >= MIN_NOTIFY_USD:
                        await notify_ltc_pending(session, owner, address, tx["value"], tx["incoming"])

            # Confirmed balance actually moved -> the real "money arrived/left" notice
            if old_confirmed is not None and abs(new_confirmed - old_confirmed) > 1e-8:
                diff_usd = abs(new_confirmed - old_confirmed) * prices.get("LTC", 0)
                if diff_usd >= MIN_NOTIFY_USD:
                    await notify(session, owner, address, old_confirmed, new_confirmed, "LTC")

            if old_confirmed != new_confirmed or old_pending != current_pending:
                balances[key] = {"confirmed": new_confirmed, "pending": current_pending}
                changed = True

    if changed:
        try:
            save_balances(balances)
        except Exception as e:
            print(f"[error] Failed to save balances: {e}")


async def notify_ltc_pending(session, owner, address, amount, incoming):
    direction = "INCOMING" if incoming else "OUTGOING"
    short_addr = f"{address[:6]}...{address[-4:]}"
    meta = COIN_META["LTC"]

    prices = await get_usd_prices(session)
    amount_usd = amount * prices.get("LTC", 0)

    embed = discord.Embed(
        title=f"⏳ LTC — {direction} (PENDING)",
        description="Seen in the mempool, waiting on confirmations.",
        color=EMBED_COLOR,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Network", value=meta["network"], inline=True)
    embed.add_field(name="Address", value=f"`{short_addr}`", inline=True)
    embed.add_field(name="Amount", value=f"{amount:.6f} LTC\n${amount_usd:,.2f}", inline=False)

    try:
        await owner.send(embed=embed)
    except discord.Forbidden:
        print("[warn] Could not DM owner — check shared server / DM privacy settings.")


async def notify(session, owner, address, old_balance, new_balance, unit):
    diff = new_balance - old_balance
    direction = "RECEIVED" if diff > 0 else "SENT"
    short_addr = f"{address[:6]}...{address[-4:]}"
    meta = COIN_META[unit]

    prices = await get_usd_prices(session)
    price = prices.get(unit, 0)
    diff_usd = abs(diff) * price
    new_balance_usd = new_balance * price

    embed = discord.Embed(
        title=f"{meta['icon']} {unit} — {direction} (CONFIRMED)",
        color=EMBED_COLOR,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Network", value=meta["network"], inline=True)
    embed.add_field(name="Address", value=f"`{short_addr}`", inline=True)
    embed.add_field(name="Amount", value=f"{abs(diff):.6f} {unit}\n${diff_usd:,.2f}", inline=False)
    embed.add_field(name="Balance Now", value=f"{new_balance:.6f} {unit}\n${new_balance_usd:,.2f}", inline=False)

    try:
        await owner.send(embed=embed)
    except discord.Forbidden:
        print("[warn] Could not DM owner — check shared server / DM privacy settings.")


@poll_balances.before_loop
async def before_poll():
    await bot.wait_until_ready()


@poll_balances.error
async def poll_balances_error(error):
    print(f"[error] poll_balances loop crashed: {error}")
    if not poll_balances.is_running():
        poll_balances.restart()


# ---------------------------------------------------------------------------
# Slash commands
# ---------------------------------------------------------------------------

@bot.tree.command(name="balance", description="Show current wallet balances")
@discord.app_commands.allowed_installs(guilds=True, users=True)
@discord.app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
async def balance_cmd(interaction: discord.Interaction):
    await interaction.response.defer()

    async with aiohttp.ClientSession() as session:
        prices = await get_usd_prices(session)

    embed = discord.Embed(title="💰 Balances", color=EMBED_COLOR)
    total_usd = 0.0

    for address in LTC_ADDRESSES:
        entry = balances.get(f"ltc:{address}")
        if entry is None:
            continue
        confirmed = entry.get("confirmed", 0) if isinstance(entry, dict) else entry
        pending_map = entry.get("pending", {}) if isinstance(entry, dict) else {}
        pending_total = sum(
            tx["value"] if tx["incoming"] else -tx["value"] for tx in pending_map.values()
        )
        usd = confirmed * prices.get("LTC", 0)
        total_usd += usd
        meta = COIN_META["LTC"]
        value_lines = f"{confirmed:.6f} LTC\n${usd:,.2f}"
        if pending_map:
            sign = "+" if pending_total >= 0 else ""
            value_lines += f"\n⏳ Pending: {sign}{pending_total:.6f} LTC"
        embed.add_field(
            name=f"{meta['icon']} LTC — {meta['network']}",
            value=value_lines,
            inline=False,
        )

    if not embed.fields:
        embed.description = "No balances tracked yet — waiting on the first poll."
    else:
        embed.add_field(name="Estimated total", value=f"**${total_usd:,.2f}**", inline=False)

    await interaction.followup.send(embed=embed)


class WalletView(discord.ui.View):
    def __init__(self, ltc_address: str):
        super().__init__(timeout=None)
        self.ltc_address = ltc_address
        if not ltc_address:
            self.ltc_button.disabled = True

    @discord.ui.button(label="LTC", style=discord.ButtonStyle.secondary, emoji="🪙")
    async def ltc_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(self.ltc_address, ephemeral=True)


@bot.tree.command(name="wallet", description="Show wallet address to send crypto")
@discord.app_commands.allowed_installs(guilds=True, users=True)
@discord.app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
async def wallet_cmd(interaction: discord.Interaction):
    ltc_address = LTC_ADDRESSES[0] if LTC_ADDRESSES else None

    if not ltc_address:
        await interaction.response.send_message("No wallet address is configured yet.", ephemeral=True)
        return

    embed = discord.Embed(
        title="💰 Wallet",
        description="Tap the button below to reveal the address to send to.",
        color=EMBED_COLOR,
    )
    meta = COIN_META["LTC"]
    embed.add_field(name=f"{meta['icon']} LTC — {meta['network']}", value="Tap **LTC** below", inline=False)

    view = WalletView(ltc_address)
    await interaction.response.send_message(embed=embed, view=view)


@bot.tree.command(name="imlimited", description="Send a message in a clean embed")
@discord.app_commands.describe(message="The message to display")
@discord.app_commands.allowed_installs(guilds=True, users=True)
@discord.app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
async def imlimited_cmd(interaction: discord.Interaction, message: str):
    embed = discord.Embed(
        description=message,
        color=EMBED_COLOR,
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_author(
        name=interaction.user.display_name,
        icon_url=interaction.user.display_avatar.url,
    )
    await interaction.response.send_message(embed=embed)


# ---------------------------------------------------------------------------
# Prefix commands (fallbacks)
# ---------------------------------------------------------------------------

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (id: {bot.user.id})")
    if not poll_balances.is_running():
        poll_balances.start()
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} slash command(s)")
    except Exception as e:
        print(f"[warn] Slash command sync failed: {e}")


@bot.command(name="balances")
async def balances_cmd(ctx):
    """?balances - show current known balances"""
    if not balances:
        await ctx.send("No balances tracked yet — waiting on the first poll.")
        return

    lines = []
    for key, entry in balances.items():
        chain, address = key.split(":", 1)
        unit = "LTC"
        short_addr = f"{address[:6]}...{address[-4:]}"
        if isinstance(entry, dict):
            confirmed = entry.get("confirmed", 0)
            pending_map = entry.get("pending", {})
            pending_total = sum(
                tx["value"] if tx["incoming"] else -tx["value"] for tx in pending_map.values()
            )
            line = f"`{short_addr}` ({unit}): {confirmed:.6f}"
            if pending_map:
                sign = "+" if pending_total >= 0 else ""
                line += f" (⏳ {sign}{pending_total:.6f} pending)"
            lines.append(line)
        else:
            lines.append(f"`{short_addr}` ({unit}): {entry:.6f}")

    embed = discord.Embed(
        title="Tracked Balances",
        description="\n".join(lines),
        color=EMBED_COLOR,
    )
    await ctx.send(embed=embed)


@bot.command(name="checknow")
async def checknow_cmd(ctx):
    """?checknow - force an immediate balance check"""
    await ctx.send("Checking now...")
    await poll_balances()
    await ctx.send("Done.")


if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
