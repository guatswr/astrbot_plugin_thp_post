import asyncio
import contextlib
import json
import time

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain, Reply
from astrbot.api.star import Context, Star, StarTools

from .core import Delivery, Outbox, Rejected, configuration_error, is_candidate, parse_event


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
        self.config_error = None

    async def initialize(self):
        self.config_error = configuration_error(self.config)
        if self.config_error:
            logger.warning(f"THP 投稿插件已加载，但尚未启用：{self.config_error}")
            return
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
        if self.config_error or self.outbox is None:
            event.stop_event()
            await event.send(event.plain_result("投稿服务尚未就绪，请联系管理员"))
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
