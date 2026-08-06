"""
opensea_mint_watcher.py
------------------------
Poll OpenSea API v2 mỗi 2 phút để phát hiện mint mới (event_type=transfer,
from_address=0x0) cho danh sách ví theo dõi, rồi đẩy alert về Telegram.

Cài đặt:
    pip install requests python-dotenv

Chạy:
    python opensea_mint_watcher.py
    (khuyến nghị chạy nền bằng pm2: pm2 start opensea_mint_watcher.py --interpreter python3 --name mint-watcher)

.env cần có:
    OPENSEA_API_KEY=xxxx
    WATCH_WALLETS=0xabc...,0xdef...        # phân cách bởi dấu phẩy
    CHAIN=arbitrum                          # slug chain OpenSea dùng (vd: ethereum, arbitrum, base...)
    TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
    TELEGRAM_CHAT_ID=-1001234567890
    POLL_INTERVAL_SECONDS=120
    STATE_FILE=mint_watcher_state.json      # optional, mặc định như dòng này
"""

import os
import json
import time
import logging
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
WATCH_WALLETS = [w.strip() for w in os.environ["WATCH_WALLETS"].split(",") if w.strip()]
CHAIN = os.environ.get("CHAIN", "ethereum")
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "120"))
STATE_FILE = os.environ.get("STATE_FILE", "mint_watcher_state.json")

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
OPENSEA_BASE_URL = "https://api.opensea.io/api/v2"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("mint_watcher")


# ---------- State (chống bắn trùng alert khi restart) ----------

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            log.warning("State file lỗi, khởi tạo lại state rỗng.")
    return {"last_seen_ts": {}, "seen_event_ids": []}


def save_state(state: dict):
    # giới hạn seen_event_ids để không phình file vô hạn
    state["seen_event_ids"] = state["seen_event_ids"][-2000:]
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


# ---------- Telegram ----------

def telegram_api(method: str, payload: dict | None = None) -> dict | None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        resp = requests.post(
            url,
            json=payload or {},
            timeout=10,
        )
        if not resp.ok:
            log.error("Telegram API %s failed: %s", method, resp.text)
            return None
        data = resp.json()
        if not data.get("ok"):
            log.error("Telegram API %s returned an error: %s", method, data)
            return None
        return data
    except requests.RequestException as e:
        log.error("Telegram API %s error: %s", method, e)
    except ValueError as e:
        log.error("Telegram API %s returned invalid JSON: %s", method, e)
    return None


def send_telegram(text: str) -> bool:
    return telegram_api(
        "sendMessage",
        {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
    ) is not None


def announce_telegram_connection() -> bool:
    """Verify the bot token and notify Telegram once the watcher is online."""
    bot_info = telegram_api("getMe")
    if not bot_info:
        return False

    bot = bot_info.get("result", {})
    bot_name = bot.get("username") or bot.get("first_name") or "unknown"
    message = (
        "\U0001f7e2 <b>Bot \u0111\u00e3 k\u1ebft n\u1ed1i th\u00e0nh c\u00f4ng</b>\n"
        f"Bot: @{bot_name}\n"
        f"Chain: {CHAIN}\n"
        f"\u0110ang theo d\u00f5i: {len(WATCH_WALLETS)} v\u00ed"
    )
    connected = send_telegram(message)
    if connected:
        log.info("Telegram bot connected: @%s", bot_name)
    return connected


# ---------- OpenSea ----------

def fetch_account_mint_events(wallet: str, after_ts: int | None) -> list[dict]:
    """Gọi OpenSea API v2 lấy event transfer của 1 ví, lọc mint (from = 0x0)."""
    url = f"{OPENSEA_BASE_URL}/events/accounts/{wallet}"
    params = {
        "chain": CHAIN,
        "event_type": "transfer",
        "limit": 50,
    }
    if after_ts:
        params["after"] = after_ts

    headers = {"accept": "application/json", "x-api-key": OPENSEA_API_KEY}

    resp = requests.get(url, params=params, headers=headers, timeout=15)
    if resp.status_code == 429:
        log.warning("Bị rate limit (429), sẽ thử lại ở vòng poll sau.")
        return []
    resp.raise_for_status()
    data = resp.json()

    events = data.get("asset_events", [])
    mints = [
        e for e in events
        if e.get("from_address", "").lower() == ZERO_ADDRESS
        and e.get("to_address", "").lower() == wallet.lower()
    ]
    return mints


def format_mint_message(wallet: str, event: dict) -> str:
    nft = event.get("nft") or {}
    collection = nft.get("collection") or event.get("asset", {}).get("collection", "N/A")
    identifier = nft.get("identifier", "N/A")
    name = nft.get("name") or f"#{identifier}"
    contract = nft.get("contract", "N/A")
    tx_hash = event.get("transaction")
    opensea_url = nft.get("opensea_url", "")

    lines = [
        "🟢 <b>New Mint Detected</b>",
        f"Wallet: <code>{wallet}</code>",
        f"Collection: {collection}",
        f"Item: {name}",
        f"Contract: <code>{contract}</code>",
    ]
    if tx_hash:
        lines.append(f"Tx: <code>{tx_hash}</code>")
    if opensea_url:
        lines.append(f"OpenSea: {opensea_url}")
    return "\n".join(lines)


# ---------- Main loop ----------

def poll_once(state: dict):
    for wallet in WATCH_WALLETS:
        after_ts = state["last_seen_ts"].get(wallet)
        try:
            mints = fetch_account_mint_events(wallet, after_ts)
        except requests.RequestException as e:
            log.error("Lỗi gọi OpenSea API cho %s: %s", wallet, e)
            continue

        if not mints:
            continue

        new_count = 0
        for event in mints:
            event_id = event.get("event_timestamp", "") + event.get("transaction", "")
            if event_id in state["seen_event_ids"]:
                continue
            state["seen_event_ids"].append(event_id)
            send_telegram(format_mint_message(wallet, event))
            new_count += 1

        if new_count:
            log.info("Ví %s: %d mint mới đã gửi Telegram.", wallet, new_count)

        # cập nhật mốc thời gian để lần sau chỉ lấy event mới hơn
        state["last_seen_ts"][wallet] = int(datetime.now(timezone.utc).timestamp())


def main():
    log.info(
        "Bắt đầu theo dõi mint cho %d ví trên chain=%s, poll mỗi %ds",
        len(WATCH_WALLETS), CHAIN, POLL_INTERVAL_SECONDS,
    )
    state = load_state()
    telegram_connected = announce_telegram_connection()

    while True:
        try:
            if not telegram_connected:
                telegram_connected = announce_telegram_connection()
            poll_once(state)
            save_state(state)
        except Exception as e:
            log.exception("Lỗi không mong muốn trong vòng poll: %s", e)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
