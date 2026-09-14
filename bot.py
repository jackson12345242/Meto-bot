"""
Crypto Balance Tracker - Discord Bot
-------------------------------------
Watches Litecoin and BEP20 USDT addresses. DMs the owner whenever a balance
changes by at least MIN_NOTIFY_USD, and offers /balance, /wallet, and
/imlimited slash commands.

Config comes from environment variables:
    DISCORD_TOKEN        - the bot's token
    DISCORD_USER_ID       - your Discord user ID (numeric), who gets DMed
                            (this is also the ONLY user allowed to run the
                            owner-only commands - see owner_only() below.
                            /wallet is open to everyone.)
                            Defaults to 1318513875372605481 if not set.
    LTC_ADDRESSES          - comma-separated list of Litecoin addresses
    BSC_USDT_ADDRESSES     - comma-separated list of BEP20 USDT addresses
    POLL_SECONDS            - how often to check, default 45
    PREFIX                  - command prefix, default "?"

Balances persist in balances.json (created automatically) so restarts don't
cause false "change" notifications.

Access control: only the Discord user with ID DISCORD_USER_ID can run
/balance, /imlimited, and the ?balances / ?checknow prefix commands.
/wallet is open to everyone so anyone can view the addresses to send to.
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

# BEP20 (Binance-Peg) USDT contract address on BNB Smart Chain
USDT_BEP20_CONTRACT = "0x55d398326f99059fF775485246999027B3197955"

# Free public BSC RPC endpoints (no API key needed) - tried in order
BSC_RPC_ENDPOINTS = [
    "https://bsc-dataseed.binance.org/",
    "https://bsc-dataseed1.defibit.io/",
    "https://bsc-dataseed1.ninicoin.io/",
]

COIN_META = {
    "LTC": {"icon": "🪙", "network": "Litecoin", "coingecko_id": "litecoin"},
    "USDT": {"icon": "💵", "network": "BNB Smart Chain (BEP20)", "coingecko_id": "tether"},
}

PRICE_CACHE = {"data": {}, "ts": 0.0}

# Minimum USD value a balance change must cross before the owner gets DMed.
MIN_NOTIFY_USD = 1.00

# Shared embed color (dark brown) used across all commands/notifications.
EMBED_COLOR = discord.Color(0x1B1716)

# --- Blockcypher rate-limit backoff -----------------------------------
# blockcypher's free tier returns HTTP 429 once you exceed its per-second/
# per-hour request quota. Previously every poll (every POLL_SECONDS) would
# immediately retry the same address and log a fresh warning, which just
# re-triggers the rate limit and floods the console with identical lines.
# Instead: once an address 429s, back off on that address for a while
# (honoring the Retry-After header if the API sends one) and only log once
# when the backoff starts, not on every subsequent poll.
DEFAULT_429_BACKOFF = 90  # seconds, used when no Retry-After header is sent
MAX_429_BACKOFF = 600  # cap so we don't back off forever
RATE_LIMIT_UNTIL = {}  # address -> monotonic timestamp when it's OK to retry
RATE_LIMIT_STREAK = {}  # address -> consecutive 429 count, for backoff growth


def get_setting(env_var, default=None, required=True):
    value = os.environ.get(env_var, default)
    if required and value is None:
        raise SystemExit(f"Missing required setting: set {env_var} env var")
    return value


DISCORD_TOKEN = get_setting("DISCORD_TOKEN")
# Hardcoded default owner/authorized user ID. This is the only account that
# can run the owner-only commands (/balance, /imlimited, ?balances,
# ?checknow). /wallet stays open to everyone regardless of this value.
DISCORD_USER_ID = int(get_setting("DISCORD_USER_ID", default="1318513875372605481", required=False))
LTC_ADDRESSES = [a.strip() for a in get_setting("LTC_ADDRESSES", default="", required=False).split(",") if a.strip()]
BSC_USDT_ADDRESSES = [a.strip() for a in get_setting("BSC_USDT_ADDRESSES", default="", required=False).split(",") if a.strip()]
POLL_SECONDS = int(get_setting("POLL_SECONDS", default=45, required=False))
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
# Access control - only DISCORD_USER_ID may run owner-restricted commands
# ---------------------------------------------------------------------------

def owner_only():
    """App-command check that rejects everyone except DISCORD_USER_ID.

    Since User Install lets anyone add this bot to their own account and DM
    it, this check is what actually keeps these commands private - Discord
    itself has no allowlist for installs.
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

async def get_ltc_balance_only(session: aiohttp.ClientSession, address: str):
    """Fast, lightweight balance-only check (no tx history) - used as a fallback."""
    url = f"https://api.blockcypher.com/v1/ltc/main/addrs/{address}/balance"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status == 429:
                _enter_backoff(address, resp.headers.get("Retry-After"))
                return None
            if resp.status != 200:
                return None
            data = await resp.json()
            return data.get("balance", 0) / 1e8
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return None


def _enter_backoff(address: str, retry_after_header):
    """Record that `address` is rate-limited and shouldn't be retried until
    the backoff window passes. Logs once per backoff entry instead of once
    per poll."""
    streak = RATE_LIMIT_STREAK.get(address, 0) + 1
    RATE_LIMIT_STREAK[address] = streak

    if retry_after_header is not None:
        try:
            delay = float(retry_after_header)
        except (TypeError, ValueError):
            delay = DEFAULT_429_BACKOFF
    else:
        # exponential-ish growth on repeated 429s, capped
        delay = min(DEFAULT_429_BACKOFF * streak, MAX_429_BACKOFF)

    was_already_in_backoff = RATE_LIMIT_UNTIL.get(address, 0) > time.monotonic()
    RATE_LIMIT_UNTIL[address] = time.monotonic() + delay

    if not was_already_in_backoff:
        print(f"[warn] LTC {address}: rate limited (429), backing off {delay:.0f}s")


def _in_backoff(address: str) -> bool:
    return RATE_LIMIT_UNTIL.get(address, 0) > time.monotonic()


async def get_ltc_info(session: aiohttp.ClientSession, address: str):
    """Returns dict with balance (LTC float) and unconfirmed_txrefs (pending txs).
    Falls back to a fast balance-only check if the full endpoint is slow/fails,
    so a slow pending-tx lookup never blocks the regular balance update.
    Skips the network call entirely while the address is in a 429 backoff
    window, and clears the backoff/streak once a request succeeds again."""
    if _in_backoff(address):
        return None

    url = f"https://api.blockcypher.com/v1/ltc/main/addrs/{address}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status == 200:
                data = await resp.json()
                RATE_LIMIT_STREAK.pop(address, None)
                return {
                    "balance": data.get("balance", 0) / 1e8,
                    "unconfirmed_txrefs": data.get("unconfirmed_txrefs", []),
                }
            if resp.status == 429:
                _enter_backoff(address, resp.headers.get("Retry-After"))
                return None
            print(f"[warn] LTC info fetch failed for {address}: HTTP {resp.status}, falling back")
    except (aiohttp.ClientError, asyncio.TimeoutError):
        print(f"[warn] LTC info fetch timed out for {address}, falling back to balance-only")

    fallback_balance = await get_ltc_balance_only(session, address)
    if fallback_balance is None:
        return None
    RATE_LIMIT_STREAK.pop(address, None)
    return {"balance": fallback_balance, "unconfirmed_txrefs": []}


async def get_usdt_bep20_balance(session: aiohttp.ClientSession, address: str):
    """Returns balance in USDT (float), or None on failure. Races all free public
    BSC RPC endpoints concurrently and returns whichever responds first."""
    padded_address = address.lower().replace("0x", "").rjust(64, "0")
    call_data = "0x70a08231" + padded_address

    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": USDT_BEP20_CONTRACT, "data": call_data}, "latest"],
        "id": 1,
    }

    async def try_endpoint(rpc_url):
        async with session.post(rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            result = data.get("result")
            if not result or result == "0x":
                return None
            return int(result, 16) / 1e18

    tasks_list = [asyncio.create_task(try_endpoint(url)) for url in BSC_RPC_ENDPOINTS]
    try:
        for coro in asyncio.as_completed(tasks_list, timeout=8):
            try:
                result = await coro
                if result is not None:
                    for t in tasks_list:
                        t.cancel()
                    return result
            except Exception:
                continue
    except asyncio.TimeoutError:
        pass
    finally:
        for t in tasks_list:
            if not t.done():
                t.cancel()

    print(f"[warn] USDT balance fetch failed for {address}: all RPC endpoints failed")
    return None


async def get_usd_prices(session: aiohttp.ClientSession):
    """Returns {'LTC': price, 'USDT': price}, cached for 60 seconds."""
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
                    "USDT": data.get("tether", {}).get("usd", 1),
                }
                PRICE_CACHE["data"] = prices
                PRICE_CACHE["ts"] = now
                return prices
    except (aiohttp.ClientError, asyncio.TimeoutError):
        pass

    return PRICE_CACHE["data"] or {"LTC": 0, "USDT": 1}


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
            *(get_usdt_bep20_balance(session, address) for address in BSC_USDT_ADDRESSES)
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

            # New pending tx we haven't alerted on yet -> "seen in mempool" notice,
            # but only if it clears the same $ threshold as confirmed transfers.
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

        for address, new_balance in zip(BSC_USDT_ADDRESSES, usdt_results):
            if new_balance is None:
                continue
            key = f"usdt_bep20:{address}"
            old_balance = balances.get(key)
            if old_balance is not None and abs(new_balance - old_balance) > 1e-6:
                diff_usd = abs(new_balance - old_balance) * prices.get("USDT", 1)
                if diff_usd >= MIN_NOTIFY_USD:
                    await notify(session, owner, address, old_balance, new_balance, "USDT")
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

    prices = await get_usd_prices(session)
    price = prices.get(unit, 0)
    diff_usd = abs(diff) * price
    new_balance_usd = new_balance * price

    title_suffix = " (CONFIRMED)" if unit == "LTC" else ""
    embed = discord.Embed(
        title=f"{meta['icon']} {unit} — {direction}{title_suffix}",
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

    for address in BSC_USDT_ADDRESSES:
        bal = balances.get(f"usdt_bep20:{address}")
        if bal is None:
            continue
        usd = bal * prices.get("USDT", 1)
        total_usd += usd
        meta = COIN_META["USDT"]
        embed.add_field(
            name=f"{meta['icon']} USDT — {meta['network']}",
            value=f"{bal:.6f} USDT\n${usd:,.2f}",
            inline=False,
        )

    if not embed.fields:
        embed.description = "No balances tracked yet — waiting on the first poll."
    else:
        embed.add_field(name="Estimated total", value=f"**${total_usd:,.2f}**", inline=False)

    await interaction.followup.send(embed=embed)


class WalletView(discord.ui.View):
    def __init__(self, ltc_address: str, usdt_address: str):
        super().__init__(timeout=None)
        self.ltc_address = ltc_address
        self.usdt_address = usdt_address
        if not ltc_address:
            self.ltc_button.disabled = True
        if not usdt_address:
            self.usdt_button.disabled = True

    # Open to everyone - no owner check, so anyone can tap and reveal the
    # address to send to.
    @discord.ui.button(label="LTC", style=discord.ButtonStyle.secondary, emoji="🪙")
    async def ltc_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(self.ltc_address, ephemeral=True)

    @discord.ui.button(label="USDT (BEP20)", style=discord.ButtonStyle.secondary, emoji="💵")
    async def usdt_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(self.usdt_address, ephemeral=True)


# Open to everyone - no @owner_only() here, so anyone can run /wallet and
# see the buttons that reveal the addresses.
@bot.tree.command(name="wallet", description="Show wallet addresses to send crypto")
@discord.app_commands.allowed_installs(guilds=True, users=True)
@discord.app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
async def wallet_cmd(interaction: discord.Interaction):
    ltc_address = LTC_ADDRESSES[0] if LTC_ADDRESSES else None
    usdt_address = BSC_USDT_ADDRESSES[0] if BSC_USDT_ADDRESSES else None

    if not ltc_address and not usdt_address:
        await interaction.response.send_message("No wallet addresses are configured yet.", ephemeral=True)
        return

    embed = discord.Embed(
        title="💰 Wallet",
        description="Tap a coin below to reveal the address to send to.",
        color=EMBED_COLOR,
    )
    if ltc_address:
        meta = COIN_META["LTC"]
        embed.add_field(name=f"{meta['icon']} LTC — {meta['network']}", value="Tap **LTC** below", inline=False)
    if usdt_address:
        meta = COIN_META["USDT"]
        embed.add_field(name=f"{meta['icon']} USDT — {meta['network']}", value="Tap **USDT (BEP20)** below", inline=False)

    view = WalletView(ltc_address, usdt_address)
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
        unit = "LTC" if chain == "ltc" else "USDT"
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
    if ctx.author.id != DISCORD_USER_ID:
        await ctx.send("You're not authorized to use this bot.")
        return

    await ctx.send("Checking now...")
    await poll_balances()
    await ctx.send("Done.")


if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
