"""Platform-independent command parser and durable delivery queue."""

import asyncio
import hashlib
import json
import random
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx


class Rejected(ValueError):
    pass


def configuration_error(config):
    """Explain why uploads must stay disabled until the next plugin reload."""
    required = ("api_base_url", "ingest_token", "event_id", "source_instance_id", "allowed_group_ids")
    if not all(config.get(key) for key in required):
        return "请先配置 THP 投稿服务地址、上传凭据、活动 ID、实例 ID 和群白名单，保存后重载插件"
    try:
        base = urlparse(str(config["api_base_url"]))
        hostname = base.hostname
        port = base.port
    except ValueError:
        return "THP api_base_url 不是有效的服务地址"
    if base.scheme not in {"http", "https"}:
        return "THP api_base_url 必须使用 HTTP 或 HTTPS"
    if (
        not hostname
        or (port is not None and port < 1)
        or base.username
        or base.password
        or base.query
        or base.fragment
        or base.path not in {"", "/"}
    ):
        return "THP api_base_url 仅填写服务域名，不含路径或凭据"
    return None


def is_candidate(raw):
    """Filter before AstrBot wake-up, so normal group chat never wakes this plugin."""
    if raw.get("message_type") != "group":
        return False
    segments = raw.get("message")
    if not isinstance(segments, list):
        text = str(raw.get("raw_message", ""))
    else:
        leading_at = bool(
            segments
            and segments[0].get("type") == "at"
            and str(segments[0].get("data", {}).get("qq")) == str(raw.get("self_id"))
        )
        text = "".join(str(s.get("data", {}).get("text", "")) for s in segments if s.get("type") == "text")
        if leading_at:
            text = text.lstrip()
    return bool(re.match(r"^/投稿(?:\s|$)", text))


def canonical(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def source_key(payload):
    return hashlib.sha256(
        canonical(
            [payload[k] for k in ("source_instance_id", "bot_self_id", "group_id", "message_id")]
        ).encode()
    ).hexdigest()


def parse_event(raw, config):
    if raw.get("message_type") != "group":
        return None
    segments = raw.get("message")
    if not isinstance(segments, list):
        # Require OneBot array reporting, so attachments cannot masquerade as text.
        text = str(raw.get("raw_message", ""))
        if re.match(r"^/投稿(?:\s|$)", text):
            raise Rejected("消息格式不支持，请管理员将 NapCat 消息上报格式设为数组")
        return None
    if (
        segments
        and segments[0].get("type") == "at"
        and str(segments[0].get("data", {}).get("qq")) == str(raw.get("self_id"))
    ):
        segments = segments[1:]
        strip_leading = True
    else:
        strip_leading = False
    text = "".join(str(s.get("data", {}).get("text", "")) for s in segments if s.get("type") == "text")
    if strip_leading:
        text = text.lstrip()
    if not re.match(r"^/投稿(?:\s|$)", text):
        return None
    if str(raw.get("group_id", "")) not in [str(v) for v in config["allowed_group_ids"]]:
        raise Rejected("此群未开放投稿")
    if any(s.get("type") != "text" for s in segments) or "[CQ:" in text:
        raise Rejected("请发送纯文本投稿，不支持图片、引用或其他附件")
    content = text[len("/投稿") :].strip()
    if not content:
        raise Rejected("用法：/投稿 你想说的话（昵称、QQ号和头像会随投稿公开展示）")
    maximum = min(300, max(1, int(config.get("max_length", 300))))
    if len(content) > maximum:
        raise Rejected(f"投稿最多 {maximum} 字，请缩短后再发送")
    if any(ord(c) < 32 and c not in "\r\n\t" for c in content):
        raise Rejected("投稿包含不支持的控制字符")
    sender = raw.get("sender") or {}
    qq_id = str(raw.get("user_id") or sender.get("user_id") or "")
    if sender.get("user_id") is not None and str(sender["user_id"]) != qq_id:
        raise Rejected("消息发送者身份不一致")
    for val in (qq_id, str(raw.get("group_id", "")), str(raw.get("self_id", ""))):
        if not re.fullmatch(r"[0-9]{1,20}", val):
            raise Rejected("缺少有效的 QQ 身份信息")
    if raw.get("message_id") is None or str(raw["message_id"]) == "":
        raise Rejected("缺少消息编号，暂时无法可靠投稿")
    if not config.get("event_id") or not config.get("source_instance_id"):
        raise Rejected("投稿服务尚未配置活动")
    try:
        timestamp = datetime.fromtimestamp(float(raw["time"]), timezone.utc).isoformat()
    except (KeyError, ValueError, TypeError, OverflowError, OSError):
        raise Rejected("缺少有效的消息时间") from None
    nickname = str(sender.get("card") or "").strip() or str(sender.get("nickname") or "").strip() or qq_id
    return {
        "event_id": str(config["event_id"]),
        "source_instance_id": str(config["source_instance_id"]),
        "bot_self_id": str(raw["self_id"]),
        "group_id": str(raw["group_id"]),
        "message_id": str(raw["message_id"]),
        "qq_id": qq_id,
        "nickname": nickname[:128],
        "content": content,
        "source_sent_at": timestamp,
    }


class Outbox:
    """Durable upload queue.

    The sqlite connection is shared across threads (``check_same_thread=False``) so async
    handlers can delegate every call to ``asyncio.to_thread`` instead of blocking AstrBot's
    event loop on disk I/O. All access is serialised by one re-entrant lock, which also
    makes concurrent ``enqueue`` of the same source key resolve to a single row.
    """

    def __init__(self, path: Path, capacity=1000, expiry=1800, cooldown=10):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        with self._lock, self.db:
            self.db.execute("""CREATE TABLE IF NOT EXISTS outbox (
                key TEXT PRIMARY KEY, payload TEXT NOT NULL, origin TEXT NOT NULL,
                status TEXT NOT NULL, created REAL NOT NULL, next_try REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0, result TEXT, notified INTEGER NOT NULL DEFAULT 0,
                notify_after REAL NOT NULL, notify_attempts INTEGER NOT NULL DEFAULT 0)""")
        self.capacity, self.expiry, self.cooldown = capacity, expiry, cooldown

    def get(self, key):
        with self._lock:
            row = self.db.execute("SELECT * FROM outbox WHERE key=?", (key,)).fetchone()
        return dict(row) if row else None

    def enqueue(self, payload, origin):
        key, encoded, now = source_key(payload), canonical(payload), time.time()
        with self._lock:
            previous = self.get(key)
            if previous:
                if previous["payload"] != encoded:
                    raise Rejected("同一来源消息内容发生变化，请重新发送一条投稿")
                return previous, False
            if (
                self.db.execute("SELECT count(*) FROM outbox WHERE status='pending'").fetchone()[0]
                >= self.capacity
            ):
                raise Rejected("暂存队列已满，请稍后再试")
            for row in self.db.execute(
                "SELECT payload FROM outbox WHERE created>? AND status!='failed'", (now - self.cooldown,)
            ):
                old = json.loads(row[0])
                if old["event_id"] == payload["event_id"] and old["qq_id"] == payload["qq_id"]:
                    raise Rejected("投稿过于频繁，请稍后再试")
            with self.db:
                self.db.execute(
                    "INSERT INTO outbox(key,payload,origin,status,created,next_try,notify_after) VALUES(?,?,?,'pending',?,?,?)",
                    (key, encoded, origin, now, now, now + 3),
                )
            return self.get(key), True

    def update(self, key, **values):
        allowed = {"status", "next_try", "attempts", "result", "notified", "notify_after", "notify_attempts"}
        if not values.keys() <= allowed:
            raise ValueError("invalid update")
        with self._lock, self.db:
            self.db.execute(
                "UPDATE outbox SET " + ",".join(f"{k}=?" for k in values) + " WHERE key=?",
                (*values.values(), key),
            )

    def pending(self):
        with self._lock:
            return [
                dict(r)
                for r in self.db.execute(
                    "SELECT * FROM outbox WHERE status='pending' AND next_try<=? ORDER BY created LIMIT 20",
                    (time.time(),),
                )
            ]

    def notifications(self):
        with self._lock:
            return [
                dict(r)
                for r in self.db.execute(
                    "SELECT * FROM outbox WHERE status!='pending' AND notified=0 AND notify_after<=? LIMIT 20",
                    (time.time(),),
                )
            ]

    def stats(self):
        with self._lock:
            return dict(self.db.execute("SELECT status,count(*) FROM outbox GROUP BY status").fetchall())

    def close(self):
        with self._lock:
            self.db.close()


class Delivery:
    def __init__(self, outbox, config, notify, logger, client=None, concurrency=3):
        self.outbox, self.config, self.notify, self.logger = outbox, config, notify, logger
        self.slots = asyncio.Semaphore(max(1, int(concurrency)))
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(float(config.get("upload_timeout", 10)), connect=3),
            follow_redirects=False,
            trust_env=False,
        )

    async def upload(self, row):
        payload = json.loads(row["payload"])

        async def save(**values):
            await asyncio.to_thread(self.outbox.update, row["key"], **values)

        if (
            payload["event_id"] != self.config["event_id"]
            or time.time() - row["created"] > self.outbox.expiry
        ):
            await save(status="failed", result="投稿已过期或活动已切换，请重新投稿")
            return
        attempts = row["attempts"] + 1
        await save(attempts=attempts)
        delay = min(60, 2 ** min(attempts, 6)) + random.uniform(0, 1)
        try:
            response = await self.client.post(
                self.config["api_base_url"].rstrip("/") + "/api/v1/ingest/submissions",
                json=payload,
                headers={
                    "Authorization": "Bearer " + self.config["ingest_token"],
                    "Idempotency-Key": row["key"],
                },
            )
            if response.status_code in (200, 201):
                data = response.json()
                if not isinstance(data.get("event_seq"), int) or not data.get("submission_id"):
                    raise ValueError("invalid success response")
                await save(status="done", result=f"投稿成功，编号 #{data['event_seq']}")
                return
            if response.status_code == 429:
                retry = response.headers.get("Retry-After", "60")
                try:
                    delay = max(1, float(retry))
                except ValueError:
                    delay = max(1, parsedate_to_datetime(retry).timestamp() - time.time())
            elif response.status_code != 408 and response.status_code < 500:
                # Avoid reflecting arbitrary upstream bodies into a QQ group.
                messages = {
                    401: "投稿服务凭据失效，请联系管理员",
                    403: "此群或活动未获投稿授权",
                    409: "活动已关闭或消息重复冲突",
                    422: "投稿格式不正确",
                    400: "投稿请求不正确",
                }
                message = messages.get(response.status_code, "投稿服务配置错误，请联系管理员")
                await save(status="failed", result=message)
                self.logger.error(
                    "THP upload terminal status=%s key=%s", response.status_code, row["key"][:12]
                )
                return
        except (httpx.HTTPError, ValueError, TypeError, KeyError, OverflowError):
            self.logger.warning("THP upload retry key=%s attempt=%s", row["key"][:12], attempts)
        await save(next_try=time.time() + delay)

    async def deliver(self, row):
        async with self.slots:
            await self.upload(row)

    async def step(self):
        rows = await asyncio.to_thread(self.outbox.pending)
        if rows:
            done = await asyncio.gather(*(self.deliver(row) for row in rows), return_exceptions=True)
            for row, result in zip(rows, done):
                if isinstance(result, Exception):
                    self.logger.error("THP upload failed key=%s", row["key"][:12], exc_info=result)
        for row in await asyncio.to_thread(self.outbox.notifications):
            try:
                if await self.notify(row):
                    await asyncio.to_thread(self.outbox.update, row["key"], notified=1)
                    continue
            except Exception:
                self.logger.warning("THP receipt retry key=%s", row["key"][:12])
            attempts = row["notify_attempts"] + 1
            await asyncio.to_thread(
                self.outbox.update,
                row["key"],
                notify_attempts=attempts,
                notify_after=time.time() + min(60, 2 ** min(attempts, 6)),
            )

    async def run(self):
        while True:
            try:
                await self.step()
            except Exception:
                self.logger.exception("THP worker failed; persisted queue retained")
            await asyncio.sleep(0.25)

    async def close(self):
        await self.client.aclose()
