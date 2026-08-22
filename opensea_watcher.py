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
    OPENSEA_API_KEY=xxxx                  # một key dùng chung (fallback)
    OPENSEA_API_KEYS=key1,key2,key3       # key theo đúng thứ tự WATCH_WALLETS
    WATCH_WALLETS=0xabc...,0xdef...        # phân cách bởi dấu phẩy
    WATCH_WALLET_LABELS=Ví chính,Ví phụ,Ví test  # cùng thứ tự với WATCH_WALLETS
    ELIGIBILITY_WALLETS=0xabc...,0xdef...       # optional, độc lập với WATCH_WALLETS
    OPENSEA_ELIGIBILITY_PATS=pat1,pat2         # PAT, cùng thứ tự ELIGIBILITY_WALLETS
    OPENSEA_ELIGIBILITY_API_KEYS=key1,key2      # optional, cùng thứ tự eligibility
    CHAIN=arbitrum                          # slug chain OpenSea dùng (vd: ethereum, arbitrum, base...)
    TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
    TELEGRAM_CHAT_ID=-1001234567890
    POLL_INTERVAL_SECONDS=20
    STATE_FILE=mint_watcher_state.json      # optional, mặc định như dòng này
"""

import os
import json
import time
import logging
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from html import escape
from urllib.parse import quote

import requests
from dotenv import load_dotenv

try:
    import websocket
except ImportError:  # pragma: no cover - báo lỗi rõ khi thiếu dependency trên VPS
    websocket = None

load_dotenv()

WATCH_WALLETS = list(
    dict.fromkeys(
        w.strip().lower()
        for w in os.environ["WATCH_WALLETS"].split(",")
        if w.strip()
    )
)
_raw_wallet_labels = os.environ.get("WATCH_WALLET_LABELS", "")
if _raw_wallet_labels.strip():
    _wallet_labels = [
        label.strip()
        for label in _raw_wallet_labels.split(",")
        if label.strip()
    ]
    if len(_wallet_labels) != len(WATCH_WALLETS):
        raise RuntimeError(
            "Số WATCH_WALLET_LABELS phải bằng số WATCH_WALLETS "
            f"({len(_wallet_labels)} nhãn / {len(WATCH_WALLETS)} ví)."
        )
else:
    _wallet_labels = [f"Ví {index + 1}" for index in range(len(WATCH_WALLETS))]
WALLET_LABELS = dict(zip(WATCH_WALLETS, _wallet_labels))
_raw_opensea_api_keys = os.environ.get("OPENSEA_API_KEYS") or os.environ.get(
    "OPENSEA_API_KEY", ""
)
OPENSEA_API_KEYS = [
    key.strip()
    for key in _raw_opensea_api_keys.split(",")
    if key.strip()
]
if not OPENSEA_API_KEYS:
    raise RuntimeError("Thiếu OPENSEA_API_KEY hoặc OPENSEA_API_KEYS trong .env")
if len(OPENSEA_API_KEYS) not in {1, len(WATCH_WALLETS)}:
    raise RuntimeError(
        "Số OPENSEA_API_KEYS phải là 1 hoặc bằng số WATCH_WALLETS "
        f"({len(OPENSEA_API_KEYS)} key / {len(WATCH_WALLETS)} ví)."
    )
WALLET_API_KEYS = {
    wallet: OPENSEA_API_KEYS[0] if len(OPENSEA_API_KEYS) == 1 else OPENSEA_API_KEYS[index]
    for index, wallet in enumerate(WATCH_WALLETS)
}
OPENSEA_API_KEY = OPENSEA_API_KEYS[0]
ELIGIBILITY_WALLETS = list(
    dict.fromkeys(
        wallet.strip().lower()
        for wallet in os.environ.get("ELIGIBILITY_WALLETS", "").split(",")
        if wallet.strip()
    )
)
_raw_eligibility_pats = os.environ.get("OPENSEA_ELIGIBILITY_PATS", "")
OPENSEA_ELIGIBILITY_PATS = [
    pat.strip()
    for pat in _raw_eligibility_pats.split(",")
    if pat.strip()
]
if OPENSEA_ELIGIBILITY_PATS and len(OPENSEA_ELIGIBILITY_PATS) != len(ELIGIBILITY_WALLETS):
    raise RuntimeError(
        "Số OPENSEA_ELIGIBILITY_PATS phải bằng số ELIGIBILITY_WALLETS "
        f"({len(OPENSEA_ELIGIBILITY_PATS)} PAT / {len(ELIGIBILITY_WALLETS)} ví)."
    )
if os.environ.get("OPENSEA_ELIGIBILITY_TOKENS", "").strip():
    raise RuntimeError(
        "OPENSEA_ELIGIBILITY_TOKENS là JWT cũ và không còn được dùng; "
        "hãy chuyển sang OPENSEA_ELIGIBILITY_PATS."
    )
_raw_eligibility_api_keys = os.environ.get("OPENSEA_ELIGIBILITY_API_KEYS", "")
OPENSEA_ELIGIBILITY_API_KEYS = [
    key.strip()
    for key in _raw_eligibility_api_keys.split(",")
    if key.strip()
]
if OPENSEA_ELIGIBILITY_API_KEYS and len(OPENSEA_ELIGIBILITY_API_KEYS) not in {
    1,
    len(ELIGIBILITY_WALLETS),
}:
    raise RuntimeError(
        "Số OPENSEA_ELIGIBILITY_API_KEYS phải là 1 hoặc bằng số "
        f"ELIGIBILITY_WALLETS ({len(OPENSEA_ELIGIBILITY_API_KEYS)} key / "
        f"{len(ELIGIBILITY_WALLETS)} ví)."
    )
ELIGIBILITY_WALLET_CONFIG = [
    {
        "wallet": wallet,
        "pat": OPENSEA_ELIGIBILITY_PATS[index]
        if index < len(OPENSEA_ELIGIBILITY_PATS)
        else "",
        "api_key": (
            OPENSEA_ELIGIBILITY_API_KEYS[0]
            if len(OPENSEA_ELIGIBILITY_API_KEYS) == 1
            else (
                OPENSEA_ELIGIBILITY_API_KEYS[index]
                if index < len(OPENSEA_ELIGIBILITY_API_KEYS)
                else OPENSEA_API_KEY
            )
        ),
    }
    for index, wallet in enumerate(ELIGIBILITY_WALLETS)
]
CHAIN = os.environ.get("CHAIN", "ethereum")
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "20"))
REST_LOOKBACK_SECONDS = int(os.environ.get("REST_LOOKBACK_SECONDS", "3600"))
STREAM_BATCH_WINDOW_SECONDS = float(os.environ.get("STREAM_BATCH_WINDOW_SECONDS", "1.5"))
STATE_FILE = os.environ.get("STATE_FILE", "mint_watcher_state.json")
ETH_USD_CACHE_SECONDS = int(os.environ.get("ETH_USD_CACHE_SECONDS", "60"))
ETH_USD_PRICE_URL = os.environ.get(
    "ETH_USD_PRICE_URL",
    "https://api.coingecko.com/api/v3/simple/price",
)
COLLECTION_INFO_CACHE_SECONDS = int(
    os.environ.get("COLLECTION_INFO_CACHE_SECONDS", "60")
)
ELIGIBILITY_JWT_REFRESH_SKEW_SECONDS = int(
    os.environ.get("ELIGIBILITY_JWT_REFRESH_SKEW_SECONDS", "300")
)

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

def fetch_account_mint_events(
    wallet: str,
    after_ts: int | None,
    api_key: str | None = None,
) -> list[dict]:
    """Gọi OpenSea API v2 lấy các event mint của một ví."""
    url = f"{OPENSEA_BASE_URL}/events/accounts/{wallet}"
    params = {
        "chain": CHAIN,
        "event_type": "mint",
        "limit": 200,
    }
    if after_ts:
        params["after"] = after_ts

    headers = {"accept": "application/json", "x-api-key": api_key or WALLET_API_KEYS[wallet]}

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
    collection = nft.get("collection") or asset.get("collection", "N/A")
    if isinstance(collection, dict):
        collection = collection.get("slug") or collection.get("name") or "N/A"
    return {
        "collection": collection,
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


def format_decimal_amount(value: Decimal, places: int = 8) -> str:
    """Format a token amount without scientific notation or trailing zeroes."""
    text = f"{value:.{places}f}".rstrip("0").rstrip(".")
    return text or "0"


def event_payment_token(event: dict) -> dict:
    payload = event.get("payload") or {}
    for source in (event, payload):
        token = source.get("payment_token")
        if isinstance(token, dict):
            return token
    return {}


def parse_payment_amount(raw_value: object, decimals: int) -> Decimal | None:
    """Convert a payment amount from base units to token units."""
    if raw_value is None or isinstance(raw_value, dict):
        return None
    try:
        raw_text = str(raw_value).strip()
        if not raw_text:
            return None
        # API price fields are normally integer base units. Accept a decimal
        # value too so manually enriched/custom event payloads still work.
        if "." in raw_text:
            return Decimal(raw_text)
        return Decimal(raw_text) / (Decimal(10) ** max(0, decimals))
    except (InvalidOperation, ValueError, TypeError):
        return None


def event_payment_amount(event: dict) -> tuple[Decimal, str, Decimal | None] | None:
    """Read an explicit payment amount from REST/Stream event shapes."""
    payload = event.get("payload") or {}
    token = event_payment_token(event)
    try:
        decimals = int(token.get("decimals", 18))
    except (TypeError, ValueError):
        decimals = 18
    symbol = str(token.get("symbol") or "ETH").upper()

    candidates: list[object] = []
    for source in (event, payload):
        candidates.append(source.get("total_price"))
        candidates.append(source.get("mint_price"))
        payment = source.get("payment")
        if isinstance(payment, dict):
            candidates.extend((payment.get("amount"), payment.get("value")))

    for raw_value in candidates:
        amount = parse_payment_amount(raw_value, decimals)
        if amount is not None:
            usd_price = token.get("usd_price")
            try:
                usd_rate = Decimal(str(usd_price)) if usd_price is not None else None
            except (InvalidOperation, ValueError, TypeError):
                usd_rate = None
            return amount, symbol, usd_rate
    return None


COLLECTION_INFO_CACHE: dict[str, tuple[float, dict | None]] = {}
COLLECTION_INFO_LOCK = threading.Lock()
ELIGIBILITY_CACHE: dict[str, tuple[float, dict | None]] = {}
ELIGIBILITY_LOCK = threading.Lock()
ELIGIBILITY_JWT_CACHE: dict[str, tuple[str, float]] = {}
ELIGIBILITY_JWT_LOCK = threading.Lock()


def parse_stage_timestamp(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except ValueError:
        return None


UTC_PLUS_7 = timezone(timedelta(hours=7))


def format_utc7_timestamp(value: object) -> str | None:
    timestamp = parse_stage_timestamp(value)
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=UTC_PLUS_7).strftime(
        "%d/%m/%Y %H:%M UTC+7"
    )


def drop_stage_list(drop_info: dict) -> list[dict]:
    for source in (
        drop_info,
        drop_info.get("drop") if isinstance(drop_info.get("drop"), dict) else None,
    ):
        stages = source.get("stages") if isinstance(source, dict) else None
        if isinstance(stages, list):
            return [stage for stage in stages if isinstance(stage, dict)]
    return []


def stage_label(stage: dict | None) -> str:
    if not isinstance(stage, dict):
        return ""
    return str(
        stage.get("label")
        or stage.get("name")
        or stage.get("stage")
        or stage.get("stage_name")
        or stage.get("stageName")
        or ""
    ).strip()


def normalized_stage_label(value: object) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def stage_start_value(stage: dict | None) -> object:
    if not isinstance(stage, dict):
        return None
    return (
        stage.get("start_time")
        or stage.get("startTime")
        or stage.get("start")
        or stage.get("mint_start_time")
        or stage.get("mintStartTime")
    )


def stage_end_value(stage: dict | None) -> object:
    if not isinstance(stage, dict):
        return None
    return (
        stage.get("end_time")
        or stage.get("endTime")
        or stage.get("end")
        or stage.get("mint_end_time")
        or stage.get("mintEndTime")
    )


def stage_is_active(stage: dict | None, now: float | None = None) -> bool:
    if not isinstance(stage, dict):
        return False
    now = time.time() if now is None else now
    start = parse_stage_timestamp(stage_start_value(stage))
    end = parse_stage_timestamp(stage_end_value(stage))
    if start is None and end is None:
        return False
    return (start is None or start <= now) and (end is None or now <= end)


def fetch_collection_info(collection_slug: str, api_key: str) -> dict | None:
    """Fetch and cache OpenSea drop/collection metadata for one alert."""
    cache_key = collection_slug.strip().lower()
    if not cache_key or cache_key == "n/a":
        return None

    now = time.monotonic()
    with COLLECTION_INFO_LOCK:
        cached = COLLECTION_INFO_CACHE.get(cache_key)
        if cached and now - cached[0] < COLLECTION_INFO_CACHE_SECONDS:
            return cached[1]

    headers = {"accept": "application/json", "x-api-key": api_key}
    encoded_slug = quote(collection_slug, safe="")
    info = None
    try:
        response = requests.get(
            f"{OPENSEA_BASE_URL}/drops/{encoded_slug}",
            headers=headers,
            timeout=10,
        )
        if response.status_code == 404:
            response = requests.get(
                f"{OPENSEA_BASE_URL}/collections/{encoded_slug}",
                headers=headers,
                timeout=10,
            )
        if response.status_code == 429:
            log.warning("Rate limit khi lấy metadata collection %s.", collection_slug)
        else:
            response.raise_for_status()
            payload = response.json()
            info = payload if isinstance(payload, dict) else None
    except (requests.RequestException, ValueError) as exc:
        log.warning("Không lấy được metadata collection %s: %s", collection_slug, exc)

    with COLLECTION_INFO_LOCK:
        COLLECTION_INFO_CACHE[cache_key] = (now, info)
    return info


def exchange_eligibility_pat(eligibility_wallet: str, pat: str) -> str | None:
    """Exchange a scoped PAT for a short-lived wallet JWT."""
    if not pat:
        return None

    try:
        response = requests.post(
            f"{OPENSEA_BASE_URL}/auth/tokens/exchange",
            json={
                "subjectToken": pat,
                "subjectTokenType": "ACCESS_TOKEN",
            },
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
            timeout=10,
        )
        if response.status_code in {401, 403}:
            log.warning(
                "PAT eligibility không hợp lệ hoặc thiếu scope cho ví %s.",
                eligibility_wallet,
            )
            return None
        if response.status_code == 429:
            log.warning("Rate limit khi exchange PAT eligibility cho ví %s.", eligibility_wallet)
            return None
        response.raise_for_status()
        payload = response.json()
        identity_token = (
            payload.get("access_token")
            or payload.get("accessToken")
            or payload.get("token")
        )
        if not identity_token:
            log.warning("OpenSea không trả về JWT sau khi exchange PAT cho ví %s.", eligibility_wallet)
            return None

        try:
            expires_in = float(payload.get("expires_in") or payload.get("expiresIn") or 12 * 60 * 60)
        except (TypeError, ValueError):
            expires_in = 12 * 60 * 60
        expires_at = time.monotonic() + max(60, expires_in)
    except (requests.RequestException, ValueError, TypeError) as exc:
        log.warning("Không exchange được PAT eligibility cho ví %s: %s", eligibility_wallet, exc)
        return None

    with ELIGIBILITY_JWT_LOCK:
        ELIGIBILITY_JWT_CACHE[eligibility_wallet.lower()] = (identity_token, expires_at)
    return identity_token


def get_eligibility_jwt(
    eligibility_wallet: str,
    pat: str,
    force_refresh: bool = False,
) -> str | None:
    """Return a cached JWT, exchanging the PAT when it is missing or near expiry."""
    if not pat:
        return None

    wallet_key = eligibility_wallet.lower()
    now = time.monotonic()
    if not force_refresh:
        with ELIGIBILITY_JWT_LOCK:
            cached = ELIGIBILITY_JWT_CACHE.get(wallet_key)
            if cached and now < cached[1] - ELIGIBILITY_JWT_REFRESH_SKEW_SECONDS:
                return cached[0]

    return exchange_eligibility_pat(eligibility_wallet, pat)


def invalidate_eligibility_jwt(eligibility_wallet: str) -> None:
    with ELIGIBILITY_JWT_LOCK:
        ELIGIBILITY_JWT_CACHE.pop(eligibility_wallet.lower(), None)


def fetch_drop_eligibility(
    collection_slug: str,
    eligibility_wallet: str,
    pat: str,
    api_key: str,
) -> dict | None:
    """Fetch eligibility for a separately configured wallet using a PAT-backed JWT."""
    identity_token = get_eligibility_jwt(eligibility_wallet, pat)
    if not identity_token:
        return None

    cache_key = f"{eligibility_wallet.lower()}|{collection_slug.lower()}"
    now = time.monotonic()
    with ELIGIBILITY_LOCK:
        cached = ELIGIBILITY_CACHE.get(cache_key)
        if cached and now - cached[0] < COLLECTION_INFO_CACHE_SECONDS:
            return cached[1]

    info = None
    for attempt in range(2):
        headers = {
            "accept": "application/json",
            "x-api-key": api_key,
            "Authorization": f"Bearer {identity_token}",
        }
        try:
            response = requests.get(
                f"{OPENSEA_BASE_URL}/drops/{quote(collection_slug, safe='')}/eligibility",
                headers=headers,
                timeout=10,
            )
            if response.status_code == 401 and attempt == 0:
                invalidate_eligibility_jwt(eligibility_wallet)
                identity_token = get_eligibility_jwt(
                    eligibility_wallet,
                    pat,
                    force_refresh=True,
                )
                if identity_token:
                    continue
            if response.status_code in {401, 403}:
                log.warning(
                    "Không có quyền đọc eligibility cho ví kiểm tra %s.",
                    eligibility_wallet,
                )
            elif response.status_code == 404:
                log.info("Collection %s không có dữ liệu eligibility.", collection_slug)
            elif response.status_code == 429:
                log.warning("Rate limit khi kiểm tra eligibility %s.", collection_slug)
            else:
                response.raise_for_status()
                payload = response.json()
                info = payload if isinstance(payload, dict) else None
            break
        except (requests.RequestException, ValueError) as exc:
            log.warning("Không kiểm tra được eligibility %s: %s", collection_slug, exc)
            break

    with ELIGIBILITY_LOCK:
        ELIGIBILITY_CACHE[cache_key] = (now, info)
    return info


def current_drop_stage(drop_info: dict) -> dict | None:
    sources = [drop_info]
    drop = drop_info.get("drop")
    if isinstance(drop, dict):
        sources.append(drop)
    for source in sources:
        stage = source.get("current_stage") or source.get("currentStage")
        if isinstance(stage, dict):
            return stage

    now = time.time()
    for candidate in drop_stage_list(drop_info):
        start = parse_stage_timestamp(stage_start_value(candidate))
        end = parse_stage_timestamp(stage_end_value(candidate))
        if (start is None or start <= now) and (end is None or now <= end):
            return candidate
    return None


def as_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "yes", "y", "eligible", "allowed", "1"}:
            return True
        if normalized in {"false", "no", "n", "ineligible", "denied", "0"}:
            return False
    return None


def eligibility_stage_entries(eligibility_info: dict) -> list[dict]:
    raw = None
    for key in (
        "stages",
        "eligibilities",
        "stage_eligibility",
        "stageEligibility",
        "eligibility_by_stage",
        "eligibilityByStage",
        "eligible_stages",
        "eligibleStages",
    ):
        if key in eligibility_info:
            raw = eligibility_info[key]
            break

    if isinstance(raw, list):
        entries = []
        for item in raw:
            if isinstance(item, dict):
                entries.append(item)
            elif isinstance(item, str):
                entries.append({"label": item, "eligible": True})
        return entries

    if isinstance(raw, dict):
        if any(
            field in raw
            for field in (
                "eligible",
                "is_eligible",
                "isEligible",
                "allowed",
                "can_mint",
                "canMint",
            )
        ):
            return [raw]
        entries = []
        for label, value in raw.items():
            if isinstance(value, dict):
                entry = dict(value)
                entry.setdefault("label", label)
            else:
                entry = {"label": label, "eligible": value}
            entries.append(entry)
        return entries

    return []


def stage_eligibility_value(stage: dict | None) -> bool | None:
    if not isinstance(stage, dict):
        return None
    for field in (
        "eligible",
        "is_eligible",
        "isEligible",
        "allowed",
        "can_mint",
        "canMint",
        "eligible_to_mint",
        "eligibleToMint",
    ):
        if field in stage:
            result = as_bool(stage[field])
            if result is not None:
                return result
    return None


def merged_eligibility_stages(
    eligibility_info: dict,
    drop_info: dict,
) -> list[dict]:
    entries = eligibility_stage_entries(eligibility_info)
    drop_stages = drop_stage_list(drop_info)
    merged: list[dict] = []
    used_drop_indexes: set[int] = set()

    for index, entry in enumerate(entries):
        entry_label = normalized_stage_label(stage_label(entry))
        match_index = None
        if entry_label:
            for drop_index, drop_stage in enumerate(drop_stages):
                if drop_index in used_drop_indexes:
                    continue
                if entry_label == normalized_stage_label(stage_label(drop_stage)):
                    match_index = drop_index
                    break
        if match_index is None and index < len(drop_stages):
            match_index = index

        combined = {}
        if match_index is not None:
            used_drop_indexes.add(match_index)
            combined.update(drop_stages[match_index])
        combined.update(entry)
        merged.append(combined)
    return merged


def status_for_eligible_stage(stage: dict, active_stage: dict | None) -> str:
    label = stage_label(stage) or stage_label(active_stage) or "Public"
    start = parse_stage_timestamp(stage_start_value(stage))
    now = time.time()
    if start is not None and start > now:
        start_text = format_utc7_timestamp(start)
        if start_text:
            return f"Có ở {label} — mint {start_text}"
    return f"Có ({label})"


def eligibility_status(
    eligibility_info: dict | None,
    drop_info: dict,
    active_stage: dict | None,
) -> str:
    if not eligibility_info:
        return "Chưa xác định"

    top_level = as_bool(eligibility_info.get("eligible"))
    stages = merged_eligibility_stages(eligibility_info, drop_info)
    active_label = normalized_stage_label(stage_label(active_stage))

    current_stage_record = None
    if active_label:
        for stage in stages:
            if normalized_stage_label(stage_label(stage)) == active_label:
                current_stage_record = stage
                break

    if current_stage_record is not None:
        current_eligible = stage_eligibility_value(current_stage_record)
        if current_eligible is True:
            return status_for_eligible_stage(current_stage_record, active_stage)

    eligible_stages = [
        stage
        for stage in stages
        if stage_eligibility_value(stage) is True
        and (
            parse_stage_timestamp(stage_end_value(stage)) is None
            or parse_stage_timestamp(stage_end_value(stage)) >= time.time()
        )
    ]
    future_stages = [
        stage
        for stage in eligible_stages
        if (
            parse_stage_timestamp(stage_start_value(stage)) is not None
            and parse_stage_timestamp(stage_start_value(stage)) > time.time()
        )
    ]
    if future_stages:
        future_stages.sort(
            key=lambda stage: parse_stage_timestamp(stage_start_value(stage))
            or float("inf")
        )
        return status_for_eligible_stage(future_stages[0], active_stage)
    if eligible_stages:
        return status_for_eligible_stage(eligible_stages[0], active_stage)

    if top_level is True:
        return status_for_eligible_stage(active_stage or {}, active_stage)
    if top_level is False:
        current_label = stage_label(active_stage)
        return f"Không ({current_label})" if current_label else "Không"
    return "Chưa xác định"


def eligibility_summary(
    collection_slug: str,
    drop_info: dict,
    active_stage: dict | None,
) -> str:
    """Check only ELIGIBILITY_WALLETS, never the event-listener wallets."""
    if not ELIGIBILITY_WALLETS:
        return "Chưa cấu hình"
    if not OPENSEA_ELIGIBILITY_PATS:
        return "Chưa cấu hình PAT"

    results = []
    for item in ELIGIBILITY_WALLET_CONFIG:
        eligibility_info = fetch_drop_eligibility(
            collection_slug,
            item["wallet"],
            item["pat"],
            item["api_key"],
        )
        short_wallet = f"{item['wallet'][:6]}…{item['wallet'][-4:]}"
        results.append(
            f"{short_wallet}: {eligibility_status(eligibility_info, drop_info, active_stage)}"
        )
    return " | ".join(results)


def first_present_value(sources: tuple[dict, ...], *keys: str) -> object:
    for source in sources:
        if not isinstance(source, dict):
            continue
        for key in keys:
            value = source.get(key)
            if value not in (None, ""):
                return value
    return None


def collection_alert_info(
    collection_slug: str,
    wallet: str,
    api_key: str,
) -> dict:
    drop_info = fetch_collection_info(collection_slug, api_key) or {}
    active_stage = current_drop_stage(drop_info)

    collection = drop_info.get("collection")
    drop = drop_info.get("drop")
    supply = drop_info.get("supply")
    metadata_sources = tuple(
        source
        for source in (drop_info, drop, supply, collection)
        if isinstance(source, dict)
    )
    collection_name = (
        drop_info.get("collection_name")
        or drop_info.get("collectionName")
        or (collection.get("name") if isinstance(collection, dict) else None)
        or collection_slug
    )
    total_supply = first_present_value(
        metadata_sources,
        "total_supply",
        "totalSupply",
        "minted_supply",
        "mintedSupply",
    )
    max_supply = first_present_value(
        metadata_sources,
        "max_supply",
        "maxSupply",
    )
    is_minting = drop_info.get("is_minting", drop_info.get("isMinting"))

    stage_name = None
    if active_stage:
        stage_name = active_stage.get("label") or active_stage.get("name")
        if not stage_name:
            stage_name = (
                "Public"
                if active_stage.get("is_public", active_stage.get("isPublic"))
                else "Allowlist"
            )
    elif is_minting is False:
        stage_name = "Không ở giai đoạn mint"

    return {
        "name": str(collection_name),
        "stage": str(stage_name) if stage_name else None,
        "total_supply": total_supply,
        "max_supply": max_supply,
        "eligibility": eligibility_summary(collection_slug, drop_info, active_stage),
    }


def format_mint_message(
    wallet: str,
    events: list[dict] | dict,
    api_key: str | None = None,
) -> str:
    if isinstance(events, dict):
        events = [events]
    events = dedupe_mint_events(events)
    details = [event_item_details(event) for event in events]
    first = details[0]
    quantity = sum(event_quantity(event) for event in events)

    wallet_label = escape(WALLET_LABELS.get(wallet.lower(), wallet), quote=False)
    wallet_address = escape(wallet, quote=False)
    collection_info = collection_alert_info(
        first["collection"],
        wallet,
        api_key or WALLET_API_KEYS[wallet.lower()],
    )
    lines = [
        f"Wallet: <b>{wallet_label}</b>",
        f"Address: <code>{wallet_address}</code>",
        f"Collection: {escape(collection_info['name'], quote=False)}",
    ]
    price_details = mint_price_details(events)
    if price_details:
        amount, symbol, usd_total = price_details
        price_line = (
            f"Mint price (tx): <code>{format_decimal_amount(amount)} {symbol}</code>"
        )
        if usd_total is not None:
            price_line += f" (~${usd_total:,.2f})"
        lines.append(price_line)

    total_supply = collection_info["total_supply"]
    max_supply = collection_info["max_supply"]
    if total_supply not in (None, "") and max_supply not in (None, "", "0", 0):
        lines.append(f"Minted: {escape(str(total_supply), quote=False)}/{escape(str(max_supply), quote=False)}")
    elif total_supply not in (None, ""):
        lines.append(f"Minted: {escape(str(total_supply), quote=False)}")

    lines.extend(
        [
            f"Eligible: {collection_info['eligibility']}",
            f"Quantity: {quantity} NFT(s)",
        ]
    )
    if first["opensea_url"]:
        lines.append(f"OpenSea (first item): {first['opensea_url']}")
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
        if not send_telegram(
            format_mint_message(wallet, unique_events, WALLET_API_KEYS[wallet])
        ):
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
TX_VALUE_CACHE: dict[str, Decimal] = {}
TX_VALUE_LOCK = threading.Lock()
ETH_USD_CACHE: tuple[float, Decimal] | None = None
ETH_USD_LOCK = threading.Lock()


def event_transaction_hash(event: dict) -> str:
    payload = event.get("payload") or {}
    if payload:
        return str((payload.get("transaction") or {}).get("hash") or "").lower()
    return str(event.get("transaction") or "").lower()


def transaction_value_eth(transaction_hash: str) -> Decimal | None:
    """Read native ETH sent with a mint transaction as a price fallback."""
    if not MINT_RPC_URL or not transaction_hash:
        return None

    with TX_VALUE_LOCK:
        if transaction_hash in TX_VALUE_CACHE:
            return TX_VALUE_CACHE[transaction_hash]

    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getTransactionByHash",
        "params": [transaction_hash],
    }
    try:
        response = requests.post(MINT_RPC_URL, json=body, timeout=10)
        response.raise_for_status()
        transaction = response.json().get("result") or {}
        raw_value = transaction.get("value")
        if raw_value is None:
            return None
        amount = Decimal(int(str(raw_value), 16)) / (Decimal(10) ** 18)
    except (requests.RequestException, ValueError, TypeError, InvalidOperation) as exc:
        log.warning("Không đọc được giá trị tx %s: %s", transaction_hash, exc)
        return None

    with TX_VALUE_LOCK:
        TX_VALUE_CACHE[transaction_hash] = amount
        if len(TX_VALUE_CACHE) > 2000:
            oldest_hash = next(iter(TX_VALUE_CACHE))
            del TX_VALUE_CACHE[oldest_hash]
    return amount


def eth_usd_price() -> Decimal | None:
    """Get a cached current ETH/USD rate for alerts without a token price."""
    global ETH_USD_CACHE
    now = time.monotonic()
    with ETH_USD_LOCK:
        if ETH_USD_CACHE and now - ETH_USD_CACHE[0] < ETH_USD_CACHE_SECONDS:
            return ETH_USD_CACHE[1]

    try:
        response = requests.get(
            ETH_USD_PRICE_URL,
            params={"ids": "ethereum", "vs_currencies": "usd"},
            timeout=5,
        )
        response.raise_for_status()
        usd_value = (response.json().get("ethereum") or {}).get("usd")
        price = Decimal(str(usd_value))
    except (requests.RequestException, ValueError, TypeError, InvalidOperation) as exc:
        log.warning("Không lấy được giá ETH/USD: %s", exc)
        return None

    with ETH_USD_LOCK:
        ETH_USD_CACHE = (now, price)
    return price


def mint_price_details(events: list[dict]) -> tuple[Decimal, str, Decimal | None] | None:
    """Return transaction mint amount, symbol and USD total for an alert."""
    unique_events = dedupe_mint_events(events)
    if not unique_events:
        return None

    explicit = event_payment_amount(unique_events[0])
    if explicit:
        amount, symbol, usd_rate = explicit
    else:
        transaction_hash = event_transaction_hash(unique_events[0])
        amount = transaction_value_eth(transaction_hash)
        if amount is None:
            return None
        symbol = "ETH"
        usd_rate = eth_usd_price()

    if usd_rate is None and symbol in {"ETH", "WETH"}:
        usd_rate = eth_usd_price()
    usd_total = amount * usd_rate if usd_rate is not None else None
    return amount, symbol, usd_total


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

    stream_api_key = os.environ.get("OPENSEA_STREAM_API_KEY") or OPENSEA_API_KEY
    stream_url = f"{OPENSEA_STREAM_URL}?token={quote(stream_api_key)}"
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

def poll_after_timestamp(state: dict, wallet: str) -> int:
    """Use a one-hour initial window for wallets newly added to the config."""
    with STATE_LOCK:
        last_seen_ts = state.setdefault("last_seen_ts", {}).get(wallet)

    if last_seen_ts:
        return max(0, last_seen_ts - REST_LOOKBACK_SECONDS)

    return max(0, int(time.time()) - REST_LOOKBACK_SECONDS)


def poll_once(state: dict):
    for wallet in WATCH_WALLETS:
        api_key = WALLET_API_KEYS[wallet]
        after_ts = poll_after_timestamp(state, wallet)
        try:
            mints = fetch_account_mint_events(wallet, after_ts, api_key)
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
