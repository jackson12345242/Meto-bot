"""
Litecoin + USDT (BEP20) Balance Tracker - Discord Bot
------------------------------------------------------
Watches Litecoin addresses AND USDT-on-BSC (BEP20) addresses. DMs the owner
whenever a balance changes (any amount, confirmed or pending for LTC;
confirmed only for USDT BEP20 - see note below), and offers /balance,
/wallet, and /imlimited slash commands.

Config comes from environment variables:
    DISCORD_TOKEN         - the bot's token
    DISCORD_USER_ID       - your Discord user ID (numeric), who gets DMed
                            (this is also the ONLY user allowed to run the
                            owner-only slash commands - see owner_only() below.
                            /checknow is the one exception -- anyone can run it.)
    LTC_ADDRESSES          - comma-separated list of Litecoin addresses
    BEP20_ADDRESSES        - comma-separated list of BSC (BEP20) addresses to
                             watch for USDT balance
    POLL_SECONDS            - how often to check, default 8
    PREFIX                  - command prefix, default "?"
    BLOCKCYPHER_TOKEN       - optional, free token from blockcypher.com, used
                              for LTC lookups.
                              Without one you share a 200 req/hour pool with
                              everyone else on your IP and will get 429s.
                              With one you get your own 3 req/sec allowance.
    BSCSCAN_TOKEN           - optional (but strongly recommended) free API key
                              from bscscan.com, used for USDT BEP20 lookups.
                              Without one you're limited to ~1-2 req/sec shared
                              across your IP and will get 429s quickly with more
                              than a couple addresses.

Balances persist in balances.json (created automatically) so restarts don't
cause false "change" notifications.

NOTE on USDT BEP20: BscScan's free tokenbalance endpoint only reports the
current confirmed on-chain balance, not pending/mempool transactions the way
BlockCypher does for LTC. So USDT BEP20 only gets a "confirmed balance
changed" notification - there's no pending/mempool notice for it.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

import aiohttp
import discord
from discord.ext import commands, tasks

# discord.py logs every gateway reconnect/resume at INFO level, which on a
# long-running bot adds up to dozens of lines a day that all say the same
# thing and drown out anything actually worth seeing. Reconnects/resumes on
# their own aren't errors -- Discord gateway connections drop and resume
# periodically as a matter of course -- so this just quiets that specific
# noise down to WARNING+ (actual connection problems still show up).
logging.getLogger("discord.gateway").setLevel(logging.WARNING)
logging.getLogger("discord.client").setLevel(logging.WARNING)

BALANCES_PATH = "balances.json"

# USDT (BEP20 / BSC) token contract address - fixed, well-known constant.
USDT_BEP20_CONTRACT = "0x55d398326f99059fF775485246999027B3197955"
USDT_BEP20_DECIMALS = 18

COIN_META = {
    "LTC": {"icon": "🪙", "network": "Litecoin", "coingecko_id": "litecoin"},
    "USDT_BEP20": {"icon": "💵", "network": "USDT (BEP20 / BSC)", "coingecko_id": "tether"},
}

PRICE_CACHE = {"data": {}, "ts": 0.0}

# Minimum USD value a balance change must cross before the owner gets DMed.
# Set to 0 so every detected change notifies, no matter how small.
MIN_NOTIFY_USD = 0

# Shared embed color (white) used across all commands/notifications.
EMBED_COLOR = discord.Color(0xFFFFFF)

# If BlockCypher starts rate-limiting us (HTTP 429), back off from hitting it
# again for this many seconds instead of retrying every single poll cycle -
# that's what was spamming the Railway logs.
RATE_LIMIT_BACKOFF_SECONDS = 60
_blockcypher_backoff_until = 0.0
_last_429_logged = 0.0

# Same idea, but for BscScan (used for USDT BEP20 lookups).
_bscscan_backoff_until = 0.0
_last_bscscan_429_logged = 0.0

# /checknow is intentionally open to anyone (not owner_only), but since it
# forces an immediate poll of the underlying APIs on demand, this cooldown
# stops it from being spammed into a 429 by repeated rapid calls from any
# caller.
CHECKNOW_COOLDOWN_SECONDS = 15
_last_manual_checknow = 0.0


def get_setting(env_var, default=None, required=True):
    value = os.environ.get(env_var, default)
    if required and value is None:
        raise SystemExit(f"Missing required setting: set {env_var} env var")
    return value


DISCORD_TOKEN = get_setting("DISCORD_TOKEN")
DISCORD_USER_ID = int(get_setting("DISCORD_USER_ID"))
LTC_ADDRESSES = [a.strip() for a in get_setting("LTC_ADDRESSES", default="", required=False).split(",") if a.strip()]
BEP20_ADDRESSES = [a.strip() for a in get_setting("BEP20_ADDRESSES", default="", required=False).split(",") if a.strip()]
POLL_SECONDS = int(get_setting("POLL_SECONDS", default=8, required=False))
PREFIX = get_setting("PREFIX", default="?", required=False)
BLOCKCYPHER_TOKEN = get_setting("BLOCKCYPHER_TOKEN", default=None, required=False)
BSCSCAN_TOKEN = get_setting("BSCSCAN_TOKEN", default=None, required=False)


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
# Access control - only DISCORD_USER_ID may run owner-only slash commands
# ---------------------------------------------------------------------------

def owner_only():
    """App-command check that rejects everyone except DISCORD_USER_ID.

    Since User Install lets anyone add this bot to their own account and DM
    it, this check is what actually keeps these commands private to you -
    Discord itself has no allowlist for installs. NOT applied to /checknow,
    which is deliberately open to anyone.
    """
    async def predicate(interaction: discord.Interaction) -> bool:
        if interaction.user.id != DISCORD_USER_ID:
            await interaction.response.send_message(
                "You're not authorized to use this bot.", ephemeral=True
            )
            return False
        return True
    return discord.app_commands.check(predicate)


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: discord.app_commands.AppCommandError):
    # CheckFailure is already handled (owner_only sends its own message).
    # Anything else, log it so it doesn't fail silently.
    if isinstance(error, discord.app_commands.CheckFailure):
        return
    print(f"[error] app command error: {error}")


# ---------------------------------------------------------------------------
# Balance / price fetch helpers
# ---------------------------------------------------------------------------

def _with_bc_token(url: str) -> str:
    if not BLOCKCYPHER_TOKEN:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}token={BLOCKCYPHER_TOKEN}"


def _note_blockcypher_429():
    """Record a rate-limit hit and log it at most once per backoff window,
    instead of once per address per poll (that's what was spamming logs)."""
    global _blockcypher_backoff_until, _last_429_logged
    now = time.monotonic()
    _blockcypher_backoff_until = now + RATE_LIMIT_BACKOFF_SECONDS
    if now - _last_429_logged > RATE_LIMIT_BACKOFF_SECONDS:
        print(
            f"[warn] BlockCypher rate limit hit (HTTP 429) - pausing BlockCypher "
            f"calls for {RATE_LIMIT_BACKOFF_SECONDS}s. "
            f"{'Add a BLOCKCYPHER_TOKEN env var for a higher limit.' if not BLOCKCYPHER_TOKEN else ''}"
        )
        _last_429_logged = now


def _blockcypher_available() -> bool:
    return time.monotonic() >= _blockcypher_backoff_until


def _note_bscscan_429():
    global _bscscan_backoff_until, _last_bscscan_429_logged
    now = time.monotonic()
    _bscscan_backoff_until = now + RATE_LIMIT_BACKOFF_SECONDS
    if now - _last_bscscan_429_logged > RATE_LIMIT_BACKOFF_SECONDS:
        print(
            f"[warn] BscScan rate limit hit (HTTP 429) - pausing BscScan "
            f"calls for {RATE_LIMIT_BACKOFF_SECONDS}s. "
            f"{'Add a BSCSCAN_TOKEN env var for a higher limit.' if not BSCSCAN_TOKEN else ''}"
        )
        _last_bscscan_429_logged = now


def _bscscan_available() -> bool:
    return time.monotonic() >= _bscscan_backoff_until


async def get_ltc_balance_only(session: aiohttp.ClientSession, address: str):
    """Fast, lightweight balance-only check (no tx history) - used as a fallback."""
    url = _with_bc_token(f"https://api.blockcypher.com/v1/ltc/main/addrs/{address}/balance")
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status == 429:
                _note_blockcypher_429()
                return None
            if resp.status != 200:
                return None
            data = await resp.json()
            return data.get("balance", 0) / 1e8
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return None


async def get_ltc_info(session: aiohttp.ClientSession, address: str):
    """Returns dict with balance (LTC float) and unconfirmed_txrefs (pending txs).
    Skips BlockCypher entirely while we're in a rate-limit backoff window, and
    only falls back to the lighter balance-only endpoint on non-429 failures
    (retrying immediately after a 429 just earns another 429)."""
    if not _blockcypher_available():
        return None

    url = _with_bc_token(f"https://api.blockcypher.com/v1/ltc/main/addrs/{address}")
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return {
                    "balance": data.get("balance", 0) / 1e8,
                    "unconfirmed_txrefs": data.get("unconfirmed_txrefs", []),
                }
            if resp.status == 429:
                _note_blockcypher_429()
                return None
            print(f"[warn] LTC info fetch failed for {address}: HTTP {resp.status}, falling back")
    except (aiohttp.ClientError, asyncio.TimeoutError):
        print(f"[warn] LTC info fetch timed out for {address}, falling back to balance-only")

    fallback_balance = await get_ltc_balance_only(session, address)
    if fallback_balance is None:
        return None
    return {"balance": fallback_balance, "unconfirmed_txrefs": []}


async def get_usdt_bep20_balance(session: aiohttp.ClientSession, address: str):
    """Returns confirmed USDT balance (float) for a BEP20 address, or None on
    failure/rate-limit. Uses BscScan's tokenbalance endpoint, which returns the
    raw integer token amount in the token's smallest unit as a string."""
    if not _bscscan_available():
        return None

    params = (
        f"module=account&action=tokenbalance"
        f"&contractaddress={USDT_BEP20_CONTRACT}"
        f"&address={address}&tag=latest"
    )
    if BSCSCAN_TOKEN:
        params += f"&apikey={BSCSCAN_TOKEN}"
    url = f"https://api.bscscan.com/api?{params}"

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status == 429:
                _note_bscscan_429()
                return None
            if resp.status != 200:
                print(f"[warn] USDT BEP20 fetch failed for {address}: HTTP {resp.status}")
                return None
            data = await resp.json()
            # BscScan wraps rate-limit / error conditions in a 200 response
            # with status "0" and a message, rather than an HTTP error code.
            if data.get("status") == "0":
                message = data.get("message", "")
                result = data.get("result", "")
                if "rate limit" in message.lower() or "rate limit" in str(result).lower():
                    _note_bscscan_429()
                else:
                    print(f"[warn] USDT BEP20 fetch error for {address}: {message} {result}")
                return None
            raw = data.get("result")
            return int(raw) / (10 ** USDT_BEP20_DECIMALS)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        print(f"[warn] USDT BEP20 fetch timed out for {address}")
        return None
    except (ValueError, TypeError):
        print(f"[warn] USDT BEP20 fetch returned unparseable result for {address}")
        return None


async def get_usd_prices(session: aiohttp.ClientSession):
    """Returns {'LTC': price, 'USDT_BEP20': price}, cached for 60 seconds."""
    now = time.monotonic()
    if PRICE_CACHE["data"] and now - PRICE_CACHE["ts"] < 60:
        return PRICE_CACHE["data"]

    url = "https://api.coingecko.com/api/v3/simple/price?ids=litecoin,tether&vs_currencies=usd"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                prices = {
                    "LTC": data.get("litecoin", {}).get("usd", 0),
                    "USDT_BEP20": data.get("tether", {}).get("usd", 1),
                }
                PRICE_CACHE["data"] = prices
                PRICE_CACHE["ts"] = now
                return prices
    except (aiohttp.ClientError, asyncio.TimeoutError):
        pass

    return PRICE_CACHE["data"] or {"LTC": 0, "USDT_BEP20": 1}


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
        usdt_results = await asyncio.gather(
            *(get_usdt_bep20_balance(session, address) for address in BEP20_ADDRESSES)
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

        for address, new_balance in zip(BEP20_ADDRESSES, usdt_results):
            if new_balance is None:
                continue
            key = f"usdt_bep20:{address}"

            # USDT BEP20 has no pending/mempool concept surfaced by the free
            # BscScan endpoint, so it's stored simply as a float, same shape
            # as the old plain-float LTC entries used to be.
            old_balance = balances.get(key)

            if old_balance is not None and abs(new_balance - old_balance) > 1e-8:
                diff_usd = abs(new_balance - old_balance) * prices.get("USDT_BEP20", 1)
                if diff_usd >= MIN_NOTIFY_USD:
                    await notify(session, owner, address, old_balance, new_balance, "USDT_BEP20")

            if old_balance != new_balance:
                balances[key] = new_balance
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
    label = "USDT" if unit == "USDT_BEP20" else unit

    prices = await get_usd_prices(session)
    price = prices.get(unit, 0)
    diff_usd = abs(diff) * price
    new_balance_usd = new_balance * price

    embed = discord.Embed(
        title=f"{meta['icon']} {label} — {direction} (CONFIRMED)",
        color=EMBED_COLOR,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Network", value=meta["network"], inline=True)
    embed.add_field(name="Address", value=f"`{short_addr}`", inline=True)
    embed.add_field(name="Amount", value=f"{abs(diff):.6f} {label}\n${diff_usd:,.2f}", inline=False)
    embed.add_field(name="Balance Now", value=f"{new_balance:.6f} {label}\n${new_balance_usd:,.2f}", inline=False)

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
@owner_only()
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

    for address in BEP20_ADDRESSES:
        confirmed = balances.get(f"usdt_bep20:{address}")
        if confirmed is None:
            continue
        usd = confirmed * prices.get("USDT_BEP20", 1)
        total_usd += usd
        meta = COIN_META["USDT_BEP20"]
        embed.add_field(
            name=f"{meta['icon']} USDT — {meta['network']}",
            value=f"{confirmed:.6f} USDT\n${usd:,.2f}",
            inline=False,
        )

    if not embed.fields:
        embed.description = "No balances tracked yet — waiting on the first poll."
    else:
        embed.add_field(name="Estimated total", value=f"**${total_usd:,.2f}**", inline=False)

    await interaction.followup.send(embed=embed)


class WalletView(discord.ui.View):
    def __init__(self, ltc_address: str, usdt_bep20_address: str):
        super().__init__(timeout=None)
        self.ltc_address = ltc_address
        self.usdt_bep20_address = usdt_bep20_address
        if not ltc_address:
            self.ltc_button.disabled = True
        if not usdt_bep20_address:
            self.usdt_button.disabled = True

    @discord.ui.button(label="LTC", style=discord.ButtonStyle.secondary, emoji="🪙")
    async def ltc_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(self.ltc_address, ephemeral=True)

    @discord.ui.button(label="USDT (BEP20)", style=discord.ButtonStyle.secondary, emoji="💵")
    async def usdt_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(self.usdt_bep20_address, ephemeral=True)


@bot.tree.command(name="wallet", description="Show wallet address to send crypto")
@owner_only()
@discord.app_commands.allowed_installs(guilds=True, users=True)
@discord.app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
async def wallet_cmd(interaction: discord.Interaction):
    ltc_address = LTC_ADDRESSES[0] if LTC_ADDRESSES else None
    usdt_bep20_address = BEP20_ADDRESSES[0] if BEP20_ADDRESSES else None

    if not ltc_address and not usdt_bep20_address:
        await interaction.response.send_message("No wallet address is configured yet.", ephemeral=True)
        return

    embed = discord.Embed(
        title="💰 Wallet",
        description="Tap a button below to reveal the address to send to.",
        color=EMBED_COLOR,
    )
    if ltc_address:
        meta = COIN_META["LTC"]
        embed.add_field(name=f"{meta['icon']} LTC — {meta['network']}", value="Tap **LTC** below", inline=False)
    if usdt_bep20_address:
        meta = COIN_META["USDT_BEP20"]
        embed.add_field(name=f"{meta['icon']} USDT — {meta['network']}", value="Tap **USDT (BEP20)** below", inline=False)

    view = WalletView(ltc_address, usdt_bep20_address)
    await interaction.response.send_message(embed=embed, view=view)


@bot.tree.command(name="imlimited", description="Send a message in a clean embed")
@discord.app_commands.describe(message="The message to display")
@owner_only()
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


@bot.tree.command(name="checknow", description="Force an immediate balance check")
@discord.app_commands.allowed_installs(guilds=True, users=True)
@discord.app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
async def checknow_slash(interaction: discord.Interaction):
    """
    Deliberately NOT owner_only() -- anyone can run this, per request.
    The cooldown below isn't an access restriction, just abuse protection:
    since this forces an on-demand poll of the underlying APIs, letting it be
    spammed with no limit at all would be an easy way for anyone to burn
    through those rate limits for everyone (owner included).
    """
    global _last_manual_checknow
    now = time.monotonic()
    remaining = CHECKNOW_COOLDOWN_SECONDS - (now - _last_manual_checknow)
    if remaining > 0:
        await interaction.response.send_message(
            f"Just checked recently — try again in {remaining:.0f}s.", ephemeral=True
        )
        return
    _last_manual_checknow = now

    await interaction.response.defer()
    await poll_balances()
    await interaction.followup.send("Balances checked.")


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
    if ctx.author.id != DISCORD_USER_ID:
        await ctx.send("You're not authorized to use this bot.")
        return

    if not balances:
        await ctx.send("No balances tracked yet — waiting on the first poll.")
        return

    lines = []
    for key, entry in balances.items():
        chain, address = key.split(":", 1)
        short_addr = f"{address[:6]}...{address[-4:]}"
        if chain == "ltc":
            if isinstance(entry, dict):
                confirmed = entry.get("confirmed", 0)
                pending_map = entry.get("pending", {})
                pending_total = sum(
                    tx["value"] if tx["incoming"] else -tx["value"] for tx in pending_map.values()
                )
                line = f"`{short_addr}` (LTC): {confirmed:.6f}"
                if pending_map:
                    sign = "+" if pending_total >= 0 else ""
                    line += f" (⏳ {sign}{pending_total:.6f} pending)"
                lines.append(line)
            else:
                lines.append(f"`{short_addr}` (LTC): {entry:.6f}")
        elif chain == "usdt_bep20":
            lines.append(f"`{short_addr}` (USDT BEP20): {entry:.6f}")
        else:
            lines.append(f"`{short_addr}` ({chain}): {entry}")

    embed = discord.Embed(
        title="Tracked Balances",
        description="\n".join(lines),
        color=EMBED_COLOR,
    )
    await ctx.send(embed=embed)


@bot.command(name="checknow")
async def checknow_cmd(ctx):
    """?checknow - force an immediate balance check (owner-only prefix version;
    see /checknow above for the public slash version)"""
    if ctx.author.id != DISCORD_USER_ID:
        await ctx.send("You're not authorized to use this bot.")
        return

    await ctx.send("Checking now...")
    await poll_balances()
    await ctx.send("Done.")


if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
