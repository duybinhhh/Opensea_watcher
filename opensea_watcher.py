"""
opensea_mint_watcher.py
------------------------
Poll OpenSea API v2 để backfill mint và nghe OpenSea Stream để phát hiện mint
realtime (transfer từ NullAddress vào ví theo dõi), rồi đẩy alert về Telegram.

Cài đặt:
    pip install requests python-dotenv
    pip install websocket-client

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
import threading
from urllib.parse import quote

import requests
from dotenv import load_dotenv

try:
    import websocket
except ImportError:  # pragma: no cover - báo lỗi rõ khi thiếu dependency trên VPS
    websocket = None

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
WATCH_WALLETS = list(
    dict.fromkeys(
        w.strip().lower()
        for w in os.environ["WATCH_WALLETS"].split(",")
        if w.strip()
    )
)
CHAIN = os.environ.get("CHAIN", "ethereum")
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "120"))
REST_LOOKBACK_SECONDS = int(os.environ.get("REST_LOOKBACK_SECONDS", "3600"))
STREAM_BATCH_WINDOW_SECONDS = float(os.environ.get("STREAM_BATCH_WINDOW_SECONDS", "1.5"))
STATE_FILE = os.environ.get("STATE_FILE", "mint_watcher_state.json")

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
OPENSEA_BASE_URL = "https://api.opensea.io/api/v2"
OPENSEA_STREAM_URL = "wss://stream-api.opensea.io/socket/websocket"
OPENSEA_STREAM_TOPIC = "collection:*"

# OpenSea normalizes some Robinhood Chain mint transfers as wallet -> wallet.
# Verify those events against the chain instead of trusting the normalized
# from/to fields. The URL can be overridden for production/archive RPCs.
CHAIN_KEY = CHAIN.strip().lower().replace(" ", "_")
MINT_RPC_URL = os.environ.get("MINT_RPC_URL") or os.environ.get("RPC_URL")
if not MINT_RPC_URL and CHAIN_KEY in {"robinhood", "robinhood_chain", "4663"}:
    MINT_RPC_URL = "https://rpc.mainnet.chain.robinhood.com"

ERC721_TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
ERC1155_TRANSFER_SINGLE_TOPIC = (
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
)
ERC1155_TRANSFER_BATCH_TOPIC = (
    "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("mint_watcher")
STATE_LOCK = threading.Lock()


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
    with STATE_LOCK:
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

def is_rest_mint_event(event: dict, wallet: str, chain: str = CHAIN) -> bool:
    from_address = (event.get("from_address") or "").lower()
    to_address = (event.get("to_address") or "").lower()
    if to_address != wallet.lower():
        return False

    # EVM mint chuẩn: NullAddress -> ví.
    if from_address == ZERO_ADDRESS:
        return True

    # OpenSea hiện trả mint trên Robinhood dưới dạng transfer ví -> chính ví.
    return (
        normalize_chain(chain) == "robinhood"
        and from_address == wallet.lower()
        and transaction_contains_mint(event, wallet)
    )

def fetch_account_mint_events(wallet: str, after_ts: int | None) -> list[dict]:
    """Gọi OpenSea API v2 lấy các event mint của một ví."""
    url = f"{OPENSEA_BASE_URL}/events/accounts/{wallet}"
    params = {
        "chain": CHAIN,
        "event_type": "mint",
        "limit": 200,
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
    # OpenSea có thể trả event_type=transfer ngay cả khi query event_type=mint.
    mints = [e for e in events if is_rest_mint_event(e, wallet)]
    return mints


def event_item_details(event: dict) -> dict:
    stream_payload = event.get("payload")
    if stream_payload:
        item = stream_payload.get("item") or {}
        metadata = item.get("metadata") or {}
        nft_id = str(item.get("nft_id") or "")
        nft_parts = nft_id.split("/")
        identifier = nft_parts[-1] if nft_parts else "N/A"
        contract = nft_parts[-2] if len(nft_parts) >= 3 else "N/A"
        return {
            "collection": (stream_payload.get("collection") or {}).get("slug", "N/A"),
            "identifier": identifier,
            "name": metadata.get("name") or f"#{identifier}",
            "contract": contract,
            "tx_hash": (stream_payload.get("transaction") or {}).get("hash"),
            "opensea_url": item.get("permalink", ""),
        }

    nft = event.get("nft") or {}
    asset = event.get("asset") or {}
    identifier = str(nft.get("identifier") or "N/A")
    return {
        "collection": nft.get("collection") or asset.get("collection", "N/A"),
        "identifier": identifier,
        "name": nft.get("name") or f"#{identifier}",
        "contract": nft.get("contract") or "N/A",
        "tx_hash": event.get("transaction"),
        "opensea_url": nft.get("opensea_url", ""),
    }


def event_quantity(event: dict) -> int:
    payload = event.get("payload") or {}
    raw_quantity = event.get("quantity") or payload.get("quantity") or 1
    try:
        return max(1, int(raw_quantity))
    except (TypeError, ValueError):
        return 1


def format_mint_message(wallet: str, events: list[dict] | dict) -> str:
    if isinstance(events, dict):
        events = [events]
    events = dedupe_mint_events(events)
    details = [event_item_details(event) for event in events]
    first = details[0]
    quantity = sum(event_quantity(event) for event in events)
    identifiers = [detail["identifier"] for detail in details]
    shown_identifiers = identifiers[:20]
    if len(identifiers) > len(shown_identifiers):
        shown_identifiers.append(f"+{len(identifiers) - len(shown_identifiers)} more")

    lines = [
        "🟢 <b>New Mint Detected</b>",
        f"Wallet: <code>{wallet}</code>",
        f"Collection: {first['collection']}",
        f"Quantity: {quantity} NFT(s)",
        f"Items: {', '.join(f'#{identifier}' for identifier in shown_identifiers)}",
        f"Contract: <code>{first['contract']}</code>",
    ]
    tx_hash = first["tx_hash"]
    if tx_hash:
        lines.append(f"Tx: <code>{tx_hash}</code>")
    if first["opensea_url"]:
        label = "OpenSea" if len(details) == 1 else "OpenSea (first item)"
        lines.append(f"{label}: {first['opensea_url']}")
    return "\n".join(lines)


def event_id(event: dict) -> str:
    """Create a stable id for both REST and Stream event shapes."""
    payload = event.get("payload") or {}
    if payload:
        item = payload.get("item") or {}
        transaction = (payload.get("transaction") or {}).get("hash", "")
        timestamp = payload.get("event_timestamp", "")
        nft_id = item.get("nft_id", "")
        return "|".join((str(transaction), str(timestamp), str(nft_id)))

    nft = event.get("nft") or {}
    return "|".join(
        (
            str(event.get("transaction") or ""),
            str(event.get("event_timestamp") or ""),
            str(nft.get("contract") or ""),
            str(nft.get("identifier") or ""),
        )
    )


def mint_batch_id(wallet: str, event: dict) -> str:
    transaction_hash = event_transaction_hash(event)
    contract, _ = event_nft_identity(event)
    if transaction_hash and contract:
        return f"mint-batch|{wallet.lower()}|{transaction_hash}|{contract}"
    return f"mint-batch|{wallet.lower()}|{event_id(event)}"


def mint_item_key(event: dict) -> str:
    transaction_hash = event_transaction_hash(event)
    contract, identifier = event_nft_identity(event)
    if transaction_hash and contract and identifier:
        return f"{transaction_hash}|{contract}|{identifier}"
    return event_id(event)


def dedupe_mint_events(events: list[dict]) -> list[dict]:
    unique_events: dict[str, dict] = {}
    for event in events:
        unique_events.setdefault(mint_item_key(event), event)
    return list(unique_events.values())


def handle_mint_events(wallet: str, events: list[dict], state: dict) -> bool:
    """Send one aggregate alert for a transaction and remember each item."""
    unique_events = dedupe_mint_events(events)
    if not unique_events:
        return False

    batch_id = mint_batch_id(wallet, unique_events[0])
    current_event_ids = [event_id(event) for event in unique_events]
    with STATE_LOCK:
        state.setdefault("seen_event_ids", [])
        if batch_id in state["seen_event_ids"]:
            return False
        if all(current_id in state["seen_event_ids"] for current_id in current_event_ids):
            return False
        if not send_telegram(format_mint_message(wallet, unique_events)):
            return False
        state["seen_event_ids"].extend(current_event_ids)
        state["seen_event_ids"].append(batch_id)
        return True


def handle_mint_event(wallet: str, event: dict, state: dict) -> bool:
    """Backward-compatible wrapper for a single event."""
    return handle_mint_events(wallet, [event], state)


def normalize_chain(value: object) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "_")
    if normalized in {"4663", "robinhood_chain"}:
        return "robinhood"
    return normalized


MINT_RECEIPT_CACHE: dict[str, list[dict]] = {}
MINT_RECEIPT_LOCK = threading.Lock()


def event_transaction_hash(event: dict) -> str:
    payload = event.get("payload") or {}
    if payload:
        return str((payload.get("transaction") or {}).get("hash") or "").lower()
    return str(event.get("transaction") or "").lower()


def event_nft_identity(event: dict) -> tuple[str, str]:
    payload = event.get("payload") or {}
    if payload:
        item = payload.get("item") or {}
        nft_id = str(item.get("nft_id") or "")
        parts = nft_id.split("/")
        if len(parts) >= 3:
            return parts[-2].lower(), parts[-1]
        return str(item.get("contract") or "").lower(), str(item.get("identifier") or "")

    nft = event.get("nft") or {}
    return str(nft.get("contract") or "").lower(), str(nft.get("identifier") or "")


def topic_address(topic: object) -> str:
    value = str(topic or "").lower()
    if len(value) < 40:
        return ""
    return "0x" + value[-40:]


def token_id_matches(value: object, identifier: str) -> bool:
    try:
        topic_value = int(str(value), 16)
        identifier_value = int(identifier, 16) if identifier.lower().startswith("0x") else int(identifier)
    except (TypeError, ValueError):
        return False
    return topic_value == identifier_value


def abi_word(data: str, index: int) -> str:
    words = str(data or "").removeprefix("0x")
    start = index * 64
    end = start + 64
    return words[start:end] if len(words) >= end else ""


def log_contains_mint(log_entry: dict, contract: str, identifier: str, wallet: str) -> bool:
    if str(log_entry.get("address") or "").lower() != contract:
        return False

    topics = [str(topic).lower() for topic in log_entry.get("topics") or []]
    if not topics:
        return False

    topic = topics[0]
    from_address = ""
    to_address = ""
    token_values: list[str] = []

    if topic == ERC721_TRANSFER_TOPIC and len(topics) >= 4:
        from_address = topic_address(topics[1])
        to_address = topic_address(topics[2])
        token_values = [topics[3]]
    elif topic == ERC1155_TRANSFER_SINGLE_TOPIC and len(topics) >= 4:
        from_address = topic_address(topics[2])
        to_address = topic_address(topics[3])
        token_values = [abi_word(log_entry.get("data", ""), 0)]
    elif topic == ERC1155_TRANSFER_BATCH_TOPIC and len(topics) >= 4:
        from_address = topic_address(topics[2])
        to_address = topic_address(topics[3])
        data = str(log_entry.get("data") or "").removeprefix("0x")
        try:
            ids_offset = int(data[0:64], 16) * 2
            ids_length = int(data[ids_offset:ids_offset + 64], 16)
            ids_start = ids_offset + 64
            token_values = [
                data[ids_start + (index * 64):ids_start + ((index + 1) * 64)]
                for index in range(ids_length)
            ]
        except (TypeError, ValueError):
            token_values = []

    return (
        from_address == ZERO_ADDRESS
        and to_address == wallet.lower()
        and any(token_id_matches(value, identifier) for value in token_values)
    )


def transaction_receipt_logs(transaction_hash: str) -> list[dict] | None:
    if not MINT_RPC_URL or not transaction_hash:
        return None

    with MINT_RECEIPT_LOCK:
        if transaction_hash in MINT_RECEIPT_CACHE:
            return MINT_RECEIPT_CACHE[transaction_hash]

    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getTransactionReceipt",
        "params": [transaction_hash],
    }
    try:
        response = requests.post(MINT_RPC_URL, json=body, timeout=10)
        response.raise_for_status()
        result = response.json().get("result") or {}
        logs = result.get("logs")
        if not isinstance(logs, list):
            return None
    except (requests.RequestException, ValueError) as exc:
        log.warning("Không xác minh được receipt %s: %s", transaction_hash, exc)
        return None

    with MINT_RECEIPT_LOCK:
        MINT_RECEIPT_CACHE[transaction_hash] = logs
        if len(MINT_RECEIPT_CACHE) > 2000:
            oldest_hash = next(iter(MINT_RECEIPT_CACHE))
            del MINT_RECEIPT_CACHE[oldest_hash]
    return logs


def transaction_contains_mint(event: dict, wallet: str) -> bool:
    contract, identifier = event_nft_identity(event)
    transaction_hash = event_transaction_hash(event)
    if not contract or not identifier or not transaction_hash:
        return False

    logs = transaction_receipt_logs(transaction_hash)
    return bool(
        logs
        and any(log_contains_mint(entry, contract, identifier, wallet) for entry in logs)
    )


def is_stream_mint(event: dict, wallet: str, chain: str = CHAIN) -> bool:
    if event.get("event_type") != "item_transferred":
        return False

    payload = event.get("payload") or {}
    from_address = (payload.get("from_account") or {}).get("address") or ""
    to_address = (payload.get("to_account") or {}).get("address") or ""
    payload_chain = payload.get("chain")
    if isinstance(payload_chain, dict):
        payload_chain = payload_chain.get("name")
    item_chain = ((payload.get("item") or {}).get("chain") or {})
    if isinstance(item_chain, dict):
        item_chain = item_chain.get("name")
    stream_chain = normalize_chain(payload_chain or item_chain)
    if stream_chain and stream_chain != normalize_chain(chain):
        return False

    if to_address.lower() != wallet.lower():
        return False
    if from_address.lower() == ZERO_ADDRESS:
        return True
    return (
        normalize_chain(chain) == "robinhood"
        and from_address.lower() == wallet.lower()
        and transaction_contains_mint(event, wallet)
    )


def group_mint_events(wallet: str, events: list[dict]) -> list[list[dict]]:
    groups: dict[str, list[dict]] = {}
    for event in events:
        groups.setdefault(mint_batch_id(wallet, event), []).append(event)
    return list(groups.values())


STREAM_PENDING: dict[str, tuple[str, dict, list[dict]]] = {}
STREAM_PENDING_TIMERS: dict[str, threading.Timer] = {}
STREAM_PENDING_LOCK = threading.Lock()


def flush_stream_batch(batch_id: str):
    with STREAM_PENDING_LOCK:
        pending = STREAM_PENDING.pop(batch_id, None)
        STREAM_PENDING_TIMERS.pop(batch_id, None)
    if not pending:
        return

    wallet, state, events = pending
    if handle_mint_events(wallet, events, state):
        log.info(
            "Mint realtime của ví %s: %d NFT trong cùng transaction đã gửi Telegram.",
            wallet,
            len(dedupe_mint_events(events)),
        )


def queue_stream_mint(wallet: str, event: dict, state: dict):
    batch_id = mint_batch_id(wallet, event)
    with STREAM_PENDING_LOCK:
        pending = STREAM_PENDING.get(batch_id)
        if pending:
            pending[2].append(event)
        else:
            STREAM_PENDING[batch_id] = (wallet, state, [event])

        old_timer = STREAM_PENDING_TIMERS.get(batch_id)
        if old_timer:
            old_timer.cancel()
        timer = threading.Timer(
            STREAM_BATCH_WINDOW_SECONDS,
            flush_stream_batch,
            args=(batch_id,),
        )
        timer.daemon = True
        STREAM_PENDING_TIMERS[batch_id] = timer
        timer.start()


def stream_loop(state: dict):
    """Listen for OpenSea transfer events and keep only mints into watched wallets."""
    if websocket is None:
        log.error("Thiếu websocket-client. Cài bằng: pip install websocket-client")
        return

    stream_url = f"{OPENSEA_STREAM_URL}?token={quote(OPENSEA_API_KEY)}"
    while True:
        ws = None
        try:
            ws = websocket.create_connection(
                stream_url,
                timeout=10,
                origin="https://opensea.io",
            )
            ws.settimeout(5)
            ws.send(json.dumps(["1", "1", OPENSEA_STREAM_TOPIC, "phx_join", {}]))
            log.info("OpenSea Stream connected, đang nghe mint realtime.")

            heartbeat_ref = 2
            last_heartbeat = time.monotonic()
            while True:
                if time.monotonic() - last_heartbeat >= 25:
                    ws.send(json.dumps([None, str(heartbeat_ref), "phoenix", "heartbeat", {}]))
                    heartbeat_ref += 1
                    last_heartbeat = time.monotonic()

                try:
                    raw_message = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue

                if not raw_message:
                    raise RuntimeError("OpenSea Stream đóng kết nối")

                message = json.loads(raw_message)
                if not isinstance(message, list) or len(message) < 5:
                    continue

                event = {
                    "event_type": message[3],
                    "payload": message[4] or {},
                }
                payload = event["payload"]
                to_address = (payload.get("to_account") or {}).get("address") or ""
                watched_wallet = next(
                    (
                        wallet
                        for wallet in WATCH_WALLETS
                        if wallet.lower() == to_address.lower()
                    ),
                    None,
                )
                if not watched_wallet or not is_stream_mint(event, watched_wallet):
                    continue

                if watched_wallet:
                    queue_stream_mint(watched_wallet, event, state)
        except Exception as exc:
            log.warning("OpenSea Stream mất kết nối: %s; thử lại sau 10s.", exc)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass
        time.sleep(10)


# ---------- Main loop ----------

def poll_once(state: dict):
    for wallet in WATCH_WALLETS:
        with STATE_LOCK:
            after_ts = state["last_seen_ts"].get(wallet)
            if after_ts:
                after_ts = max(0, after_ts - REST_LOOKBACK_SECONDS)
        try:
            mints = fetch_account_mint_events(wallet, after_ts)
        except requests.RequestException as e:
            log.error("Lỗi gọi OpenSea API cho %s: %s", wallet, e)
            continue

        new_count = 0
        for batch in group_mint_events(wallet, mints):
            if handle_mint_events(wallet, batch, state):
                new_count += 1

        if new_count:
            log.info("Ví %s: %d mint mới đã gửi Telegram.", wallet, new_count)

        # Luôn backfill theo cửa sổ lookback để không mất event khi Stream rớt.
        with STATE_LOCK:
            state["last_seen_ts"][wallet] = int(time.time())


def main():
    log.info(
        "Bắt đầu theo dõi mint cho %d ví trên chain=%s, poll mỗi %ds",
        len(WATCH_WALLETS), CHAIN, POLL_INTERVAL_SECONDS,
    )
    state = load_state()
    telegram_connected = announce_telegram_connection()
    threading.Thread(
        target=stream_loop,
        args=(state,),
        name="opensea-stream",
        daemon=True,
    ).start()

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
