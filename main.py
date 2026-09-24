import asyncio
import contextlib
import json
import time
from urllib.parse import urlparse

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain, Reply
from astrbot.api.star import Context, Star, StarTools

from .core import Delivery, Outbox, Rejected, is_candidate, parse_event


class SubmissionFilter(filter.CustomFilter):
    def filter(self, event: AstrMessageEvent, cfg: AstrBotConfig) -> bool:
        if event.get_platform_name() != "aiocqhttp":
            return False
        return is_candidate(dict(event.message_obj.raw_message))


class THPPost(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.worker = None
        self.outbox = None
        self.delivery = None

    async def initialize(self):
        base = urlparse(self.config.get("api_base_url", ""))
        if base.scheme != "https" and not (
            base.scheme == "http" and base.hostname in {"127.0.0.1", "localhost"}
        ):
            raise ValueError("THP api_base_url 必须使用 HTTPS（本机调试除外）")
        if base.username or base.password or base.query or base.fragment or base.path not in {"", "/"}:
            raise ValueError("THP api_base_url 仅填写服务域名，不含路径或凭据")
        if not all(
            self.config.get(k)
            for k in ("ingest_token", "event_id", "source_instance_id", "allowed_group_ids")
        ):
            raise ValueError("请先配置 THP 投稿服务、活动和群白名单")
        self.outbox = Outbox(
            StarTools.get_data_dir("astrbot_plugin_thp_post") / "outbox.db",
            capacity=int(self.config.get("outbox_capacity", 1000)),
            expiry=int(self.config.get("outbox_expiry", 1800)),
            cooldown=int(self.config.get("user_cooldown", 10)),
        )
        self.delivery = Delivery(self.outbox, self.config, self.notify, logger)
        self.worker = asyncio.create_task(self.delivery.run())

    async def notify(self, row):
        payload = json.loads(row["payload"])
        return await self.context.send_message(
            row["origin"], MessageChain([Reply(id=payload["message_id"]), Plain(row["result"])])
        )

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE, priority=100)
    @filter.custom_filter(SubmissionFilter, priority=100)
    async def submission(self, event: AstrMessageEvent):
        if event.get_platform_name() != "aiocqhttp":
            return
        try:
            payload = parse_event(dict(event.message_obj.raw_message), self.config)
        except Rejected as exc:
            event.stop_event()
            await event.send(event.plain_result(str(exc)))
            return
        if payload is None:
            return
        event.stop_event()
        try:
            row, created = await asyncio.to_thread(
                self.outbox.enqueue, payload, event.unified_msg_origin
            )
        except Rejected as exc:
            await event.send(event.plain_result(str(exc)))
            return
        if not created:
            return  # Redelivered OneBot event: no duplicated feedback or new request.
        deadline = time.monotonic() + 1.2
        while row["status"] == "pending" and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
            row = await asyncio.to_thread(self.outbox.get, row["key"])
        if row["status"] != "pending":
            await event.send(event.plain_result(row["result"]))
            await asyncio.to_thread(self.outbox.update, row["key"], notified=1)
        else:
            await event.send(event.plain_result("已暂存，等待网络恢复上传；成功后会通知你"))

    async def terminate(self):
        if self.worker:
            self.worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.worker
        if self.delivery:
            await self.delivery.close()
        if self.outbox:
            self.outbox.close()
