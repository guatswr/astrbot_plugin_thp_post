import asyncio
import logging
import time

import httpx
import pytest
from astrbot_plugin_thp_post.core import Delivery, Outbox, Rejected, is_candidate, parse_event


def config():
    return {
        "allowed_group_ids": ["12345"],
        "event_id": "test",
        "source_instance_id": "napcat",
        "api_base_url": "https://example.test",
        "ingest_token": "test",
    }


def event(text="/投稿 你好🎉", **kw):
    return {
        "message_type": "group",
        "group_id": 12345,
        "self_id": 10000,
        "user_id": 123456789012345678,
        "message_id": -123,
        "time": 1790078400,
        "sender": {"nickname": "昵称", "card": "群名片"},
        "message": [{"type": "text", "data": {"text": text}}],
        **kw,
    }


def test_identity_and_unicode():
    result = parse_event(event(), config())
    assert result["qq_id"] == "123456789012345678"
    assert result["nickname"] == "群名片"
    assert result["message_id"] == "-123"
    assert parse_event(event(sender={}), config())["nickname"] == result["qq_id"]
    assert parse_event(event("/投稿 " + "🎉" * 300), config())


@pytest.mark.parametrize("text", ["普通聊天", "/投稿abc", " /投稿 空格前缀"])
def test_non_commands(text):
    assert not is_candidate(event(text))
    assert parse_event(event(text), config()) is None


@pytest.mark.parametrize(
    "raw",
    [
        event("/投稿"),
        event("/投稿 " + "字" * 301),
        event(group_id=54321),
        event(message_id=None),
        event(time=None),
        event(user_id="invalid"),
        event(message=[{"type": "text", "data": {"text": "/投稿 内容"}}, {"type": "image", "data": {}}]),
        event(
            message=[{"type": "reply", "data": {"id": 1}}, {"type": "text", "data": {"text": "/投稿 内容"}}]
        ),
    ],
)
def test_invalid(raw):
    with pytest.raises(Rejected):
        parse_event(raw, config())


def test_private_and_at():
    assert parse_event(event(message_type="private"), config()) is None
    raw = event(
        message=[{"type": "at", "data": {"qq": "10000"}}, {"type": "text", "data": {"text": " /投稿 你好"}}]
    )
    assert parse_event(raw, config())["content"] == "你好"


def test_persistent_dedupe_capacity(tmp_path):
    payload = parse_event(event(), config())
    path = tmp_path / "outbox.db"
    box = Outbox(path, capacity=1)
    row, created = box.enqueue(payload, "session")
    assert created
    box.close()
    box = Outbox(path, capacity=1)
    assert box.enqueue(payload, "session")[1] is False
    with pytest.raises(Rejected):
        box.enqueue({**payload, "message_id": "2"}, "session")
    assert box.get(row["key"])["status"] == "pending"
    box.close()


async def test_lost_response_retry_no_duplicate(tmp_path):
    box = Outbox(tmp_path / "box.db")
    row, _ = box.enqueue(parse_event(event(), config()), "session")
    stored = set()
    calls = []

    async def server(request):
        key = request.headers["Idempotency-Key"]
        calls.append(key)
        if key not in stored:
            stored.add(key)
            raise httpx.ReadTimeout("response lost")
        return httpx.Response(200, json={"submission_id": "one", "event_seq": 1, "duplicate": True})

    client = httpx.AsyncClient(transport=httpx.MockTransport(server))

    async def notify(_):
        return True

    delivery = Delivery(box, config(), notify, logging.getLogger("test"), client)
    await delivery.upload(row)
    assert box.get(row["key"])["status"] == "pending"
    await delivery.upload(box.get(row["key"]))
    assert box.get(row["key"])["status"] == "done"
    assert len(stored) == 1 and calls[0] == calls[1]
    await delivery.close()
    box.close()


@pytest.mark.parametrize("status", [400, 401, 403, 409, 422])
async def test_terminal_not_retried(tmp_path, status):
    box = Outbox(tmp_path / "box.db")
    row, _ = box.enqueue(parse_event(event(), config()), "session")
    delivery = Delivery(
        box,
        config(),
        None,
        logging.getLogger("test"),
        httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(status))),
    )
    await delivery.upload(row)
    assert box.get(row["key"])["status"] == "failed"
    assert not box.pending()
    await delivery.close()
    box.close()


async def test_activity_switch_expiry_and_retry_after(tmp_path):
    box = Outbox(tmp_path / "box.db", cooldown=0)
    row, _ = box.enqueue(parse_event(event(), config()), "session")
    cfg = config()
    cfg["event_id"] = "next"
    delivery = Delivery(
        box,
        cfg,
        None,
        logging.getLogger("test"),
        httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(429, headers={"Retry-After": "120"}))
        ),
    )
    await delivery.upload(row)
    assert box.get(row["key"])["status"] == "failed"
    cfg["event_id"] = "test"
    row, _ = box.enqueue(parse_event(event(message_id=2), config()), "session")
    await delivery.upload(row)
    assert box.get(row["key"])["next_try"] >= time.time() + 119
    row["created"] -= 1801
    await delivery.upload(row)
    assert box.get(row["key"])["status"] == "failed"
    await delivery.close()
    box.close()


def test_outbox_is_usable_from_worker_threads(tmp_path):
    # AstrBot handlers delegate sqlite work to threads, so the queue must survive that.
    from concurrent.futures import ThreadPoolExecutor

    box = Outbox(tmp_path / "outbox.db")
    body = parse_event(event(), config())
    with ThreadPoolExecutor(max_workers=8) as pool:
        rows = list(pool.map(lambda _: box.enqueue(body, "session"), range(16)))
    assert len({row["key"] for row, _ in rows}) == 1
    assert sum(1 for _, created in rows if created) == 1
    assert box.get(rows[0][0]["key"])["status"] == "pending"
    box.close()


async def test_pending_rows_upload_concurrently_up_to_the_limit(tmp_path):
    box = Outbox(tmp_path / "outbox.db", cooldown=0)
    for index in range(6):
        box.enqueue(parse_event(event(message_id=index), config()), "session")
    active = peak = 0

    async def server(request):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.1)
        active -= 1
        return httpx.Response(200, json={"submission_id": "one", "event_seq": 1, "duplicate": False})

    async def notify(_):
        return True

    delivery = Delivery(
        box,
        config(),
        notify,
        logging.getLogger("test"),
        httpx.AsyncClient(transport=httpx.MockTransport(server)),
        concurrency=3,
    )
    await delivery.step()
    assert peak == 3, "uploads must overlap, but never exceed the configured limit"
    assert box.stats() == {"done": 6}
    await delivery.close()
    box.close()
