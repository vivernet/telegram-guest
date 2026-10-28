from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

from gateway.platforms.base import SendResult
from plugins.platforms.telegram import adapter as telegram_base
from telegram import (
    InlineQueryResultArticle,
    InputTextMessageContent,
    Message,
    Update,
)
from telegram.error import BadRequest
from telegram.ext import ApplicationHandlerStop, TypeHandler

GUEST_UPDATE_TYPE = "guest_message"
GUEST_CHAT_PREFIX = "guest:"
GUEST_THINKING_TEXT = '<tg-emoji emoji-id="5463297803235113601">✨</tg-emoji> Думаю...'

logger = logging.getLogger(__name__)


class GuestEditError(RuntimeError):
    """A Guest edit failed and the gateway must keep the final response eligible."""


def guest_chat_id(query_id: str) -> str:
    return f"{GUEST_CHAT_PREFIX}{query_id}"


def is_guest_chat_id(chat_id: object) -> bool:
    return str(chat_id).startswith(GUEST_CHAT_PREFIX)


@dataclass
class GuestQueryState:
    query_id: str
    latest_content: str = ""
    delivered_content: str = ""
    delivered_finalized: bool = False
    delivered_rich_finalized: bool = False
    rich_edit_ambiguous: bool = False
    rich_rejected_content: str = ""
    answer_attempted: bool = False
    answer_error: str | None = None
    answered: bool = False
    inline_message_id: str | None = None
    answered_wallclock: float | None = None
    placeholder_active: bool = False
    approval_actor_id: int | None = None
    approval_card_active: bool = False
    approval_cancel_attempted: bool = False
    created_mono: float = field(default_factory=time.monotonic)
    answer_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)


class GuestTelegramAdapter(telegram_base.TelegramAdapter):
    """Thin native Guest Mode layer over Hermes' bundled Telegram adapter."""

    MAX_RETAINED_QUERIES = 1024
    ANSWER_ATTEMPT_RETENTION_SECONDS = 24 * 60 * 60
    GUEST_STREAM_CONSUMER_LIMIT = 2_147_483_647
    GUEST_CONTENT_ENRICHERS = (
        (
            ("photo", "video", "audio", "voice", "document"),
            "_enrich_guest_direct_media",
            "_enrich_guest_reply_media",
        ),
        (("location", "venue"), "_enrich_guest_location", "_enrich_guest_location"),
        (("sticker",), "_enrich_guest_sticker", "_enrich_guest_sticker"),
    )

    def __init__(self, config):
        super().__init__(config)
        # Гостевому режиму нужен нативный rich-финал (Bot API rich_message), иначе
        # MarkdownV2-редактирование inline-сообщений сбоит и форматирование ломается.
        # Если API/токен не поддерживает rich — есть гарантированный фолбэк на MDv2/plain.
        self._rich_messages_enabled = True
        self._guest_queries: dict[str, GuestQueryState] = {}
        self._answer_attempted_query_ids: dict[str, float] = {}
        self._guest_session_last_query: dict[str, str] = {}
        self._guest_session_queries: dict[str, list[str]] = {}
        self._guest_last_inline: dict[str, str] = {}
        self._guest_busy: dict[str, bool] = {}
        self._guest_session_locks: dict[str, asyncio.Lock] = {}
        self._guest_active_query: dict[str, str] = {}
        self._guest_approval_bindings: dict[str, dict[str, Any]] = {}
        self._guest_handler_apps: set[int] = set()

    def _remember_guest_query(self, session_chat_id: str, query_id: str) -> GuestQueryState:
        """Track a guest query under its one-shot key while keeping the session stable.

        ``session_chat_id`` is the synthetic identity Hermes uses for history —
        either the legacy query-scoped id or a persistent ``guest:<chat>:<caller>``
        key. Delivery state stays keyed by the unique query id so each mention gets
        its own ``answerGuestQuery`` round, while ``_guest_session_last_query`` maps
        the session to its latest query (used to route sends and to find the last
        inline reply for /del).
        """
        now = time.monotonic()
        while self._answer_attempted_query_ids:
            retained_id = next(iter(self._answer_attempted_query_ids))
            if self._answer_attempted_query_ids[retained_id] > now:
                break
            self._answer_attempted_query_ids.pop(retained_id, None)
        state_key = guest_chat_id(query_id)
        state = self._guest_queries.get(state_key)
        if state is not None:
            return state
        if len(self._guest_queries) >= self.MAX_RETAINED_QUERIES:
            evictable = next((key for key, candidate in self._guest_queries.items()
                              if not candidate.approval_card_active
                              and not candidate.answer_lock.locked()), None)
            if evictable is None:
                raise RuntimeError("Guest query capacity exhausted")
            self._guest_queries.pop(evictable)
        previous_query_id = self._guest_session_last_query.get(session_chat_id)
        if previous_query_id and previous_query_id != query_id:
            previous = self._guest_queries.get(guest_chat_id(previous_query_id))
            if previous is not None and previous.inline_message_id:
                self._guest_last_inline[session_chat_id] = previous.inline_message_id
        self._guest_session_last_query[session_chat_id] = query_id
        ledger = self._guest_session_queries.setdefault(session_chat_id, [])
        if not ledger or ledger[-1] != query_id:
            ledger.append(query_id)
        if len(ledger) > self.MAX_RETAINED_QUERIES:
            del ledger[: len(ledger) - self.MAX_RETAINED_QUERIES]
        state = GuestQueryState(
            query_id=query_id,
            answer_attempted=query_id in self._answer_attempted_query_ids,
        )
        self._guest_queries[state_key] = state
        return state

    def _guest_session_chat_id(self, message: Message, query_id: str) -> str:
        """Stable session identity for a guest mention.

        Persistent mode (default) keys history on the real chat + caller IDs so a
        guest keeps context across mentions; the one-shot delivery still runs per
        ``guest_query_id``. Set ``extra.persistent_guest_sessions=false`` to fall
        back to the legacy one-shot query-scoped behaviour.
        """
        extra = getattr(self.config, "extra", None) or {}
        if not extra.get("persistent_guest_sessions", True):
            return guest_chat_id(query_id)
        chat = getattr(message, "chat", None)
        real_chat_id = str(getattr(chat, "id", "") or "").strip()
        caller = getattr(message, "sender_chat", None) or getattr(message, "from_user", None)
        caller_id = str(getattr(caller, "id", "") or "").strip()
        if not real_chat_id or not caller_id:
            return guest_chat_id(query_id)
        return f"{GUEST_CHAT_PREFIX}{real_chat_id}:{caller_id}"

    @staticmethod
    def _guest_command(text: str) -> str | None:
        cleaned = (text or "").strip()
        # Telegram requires a @mention to reach a guest bot in a group; strip
        # any leading "@handle" so both "/clean" and "@bot /clean" work.
        while cleaned.startswith("@"):
            _, sep, rest = cleaned.partition(" ")
            if not sep:
                cleaned = ""
                break
            cleaned = rest.strip()
        lower = cleaned.lower()
        if lower == "/clean":
            return "/clean"
        if lower in ("/del", "/delete"):
            return "/del"
        if lower in ("/status", "/статус", "/state"):
            return "/status"
        if lower in ("/help", "/помощь"):
            return "/help"
        return None

    def _last_guest_inline_id(self, session_chat_id: str) -> str | None:
        query_id = self._guest_session_last_query.get(session_chat_id)
        if query_id:
            state = self._guest_queries.get(guest_chat_id(query_id))
            if state is not None and state.inline_message_id:
                return state.inline_message_id
        return self._guest_last_inline.get(session_chat_id)

    async def _replace_guest_reply_with_ellipsis(
        self, inline_id: str, session_chat_id: str,
    ) -> None:
        matches = [state for state in self._guest_queries.values()
                   if state.inline_message_id == inline_id]
        if len(matches) != 1:
            return
        state = matches[0]
        failed = False
        async with state.answer_lock:
            if state.approval_card_active or state.inline_message_id != inline_id:
                return
            try:
                await self._bot._post(
                    "editMessageText",
                    {"inline_message_id": inline_id, "text": "..."},
                )
            except Exception:
                logger.warning("[Telegram Guest] /del inline edit failed", exc_info=True)
                failed = True
        if failed:
            await self._publish_guest(session_chat_id, "Не удалось очистить сообщение.")

    async def _dispatch_guest_command(
        self, cmd: str, message: Message, session_chat_id: str, event: Any,
    ) -> bool:
        if cmd == "/clean":
            store = getattr(self, "_session_store", None)
            if store is not None:
                try:
                    session_key = self._event_session_key(event)
                    if session_key:
                        store.reset_session(session_key)
                except Exception:
                    logger.warning("[Telegram Guest] /clean session reset failed", exc_info=True)
            await self._publish_guest(session_chat_id, "✅ Сессия очищена.")
            return True
        if cmd == "/del":
            replied = getattr(message, "reply_to_message", None)
            if replied is None:
                await self._publish_guest(
                    session_chat_id,
                    "Отправь эту команду ответом на сообщение бота.",
                )
                return True

            replied_text = str(getattr(replied, "text", "") or "").strip()
            logger.info(
                "[Telegram Guest] /del replied: msg_id=%s chat=%s guest_query_id=%r nested=%r",
                getattr(replied, "message_id", None),
                getattr(getattr(replied, "chat", None), "id", None),
                getattr(replied, "guest_query_id", None),
                (getattr(replied, "api_kwargs", None) or {}).get("guest_message"),
            )

            # Гостевой ответ — inline-сообщение: править его можно только по
            # inline_message_id, а не по chat_id + message_id (бот не участник
            # чата, такой запрос возвращает Forbidden).
            # 1) Точное совпадение по guest_query_id ответа (flat либо вложенный
            #     guest_message внутри reply_to_message).
            replied_query_id = str(getattr(replied, "guest_query_id", "") or "").strip()
            if not replied_query_id:
                _replied_guest = (getattr(replied, "api_kwargs", None) or {}).get(
                    "guest_message"
                )
                if isinstance(_replied_guest, dict):
                    replied_query_id = str(
                        _replied_guest.get("guest_query_id") or ""
                    ).strip()
            if replied_query_id:
                target_state = self._guest_queries.get(guest_chat_id(replied_query_id))
                inline_id = (
                    target_state.inline_message_id
                    if target_state is not None
                    else None
                )
                # Если запись уже вытеснена/устарела — пробуем сам guest_query_id
                # как inline_message_id (запасной вариант).
                if inline_id is None:
                    inline_id = replied_query_id
                await self._replace_guest_reply_with_ellipsis(inline_id, session_chat_id)
                return True

            # 2) Telegram не присылает guest_query_id в reply_to_message. Ищем
            #     целевой пузырь внутри этой сессии по дате отправки и/или по
            #     тексту (чаще всего пользователь отвечает на последний видимый
            #     ответ бота).
            replied_ts = getattr(getattr(replied, "date", None), "timestamp", None)
            replied_ts = replied_ts() if replied_ts is not None else None

            def _session_states():
                for query_id in self._guest_session_queries.get(session_chat_id, []):
                    st = self._guest_queries.get(guest_chat_id(query_id))
                    if st is not None and st.inline_message_id:
                        yield st

            matched = None
            if replied_ts is not None:
                timed = [
                    st
                    for st in _session_states()
                    if st.answered_wallclock is not None
                    and abs(st.answered_wallclock - replied_ts) <= 5
                ]
                if timed:
                    matched = min(
                        timed, key=lambda s: abs(s.answered_wallclock - replied_ts)
                    )
            if matched is None and replied_text:
                textual = [
                    st
                    for st in _session_states()
                    if (st.delivered_content or st.latest_content) == replied_text
                ]
                if textual:
                    matched = max(textual, key=lambda s: s.created_mono)
            if matched is not None:
                await self._replace_guest_reply_with_ellipsis(
                    matched.inline_message_id, session_chat_id
                )
                return True

            # 3) Последний пузырь сессии — запасной вариант.
            inline_id = self._last_guest_inline_id(session_chat_id)
            if inline_id:
                await self._replace_guest_reply_with_ellipsis(inline_id, session_chat_id)
                return True

            # Unknown chat/message identities cannot be checked against a card.
            await self._publish_guest(session_chat_id, "Не нашёл сообщение для удаления.")
            return True
        if cmd == "/status":
            await self._publish_guest(session_chat_id, self._guest_status_text(session_chat_id))
            return True
        if cmd == "/help":
            await self._publish_guest(session_chat_id, self._guest_help_text())
            return True
        return False

    def _guest_status_text(self, session_chat_id: str) -> str:
        """Краткая сводка состояния последнего запроса гостевой сессии."""
        previous_query_id = self._guest_session_last_query.get(session_chat_id)
        busy = self._guest_busy.get(session_chat_id, False)
        lines = ["⚙️ Статус", ""]
        if previous_query_id:
            state = self._guest_queries.get(guest_chat_id(previous_query_id))
            if state is not None:
                elapsed = int(time.monotonic() - state.created_mono)
                if busy:
                    state_text = "идёт обработка…"
                elif state.delivered_finalized:
                    state_text = "завершено, финальный ответ опубликован"
                elif state.answer_error:
                    state_text = f"Telegram не подтвердил публикацию: {state.answer_error}"
                elif state.delivered_content:
                    state_text = "завершено, опубликован только промежуточный ответ"
                elif state.answered:
                    state_text = "запрос принят; финальная публикация не подтверждена"
                else:
                    state_text = "завершено, публикация не выполнялась"
                lines.append(f"• Обработка: {state_text}")
                lines.append(f"• Прошло: {elapsed} с")
                lines.append(f"• Объём ответа: {len(state.latest_content)} симв.")
                lines.append(f"• Финальная публикация: {'да' if state.delivered_finalized else 'нет'}")
            else:
                lines.append("• Активных запросов нет")
        else:
            lines.append("• Активных запросов нет")
        lines.append("")
        lines.append("Напиши вопрос или упомяни бота — отвечу.")
        return "\n".join(lines)

    def _guest_help_text(self) -> str:
        """Список команд, доступных гостю."""
        return (
            "🤖 Команды гостевого режима:\n\n"
            "• /help — этот список команд\n"
            "• /status (/статус) — статус обработки последнего запроса\n"
            "• /clean — сбросить историю текущей сессии\n"
            "• /del — удалить сообщение бота (ответом на него)\n\n"
            "Просто напиши вопрос — и я отвечу."
        )

    def _ensure_guest_handler(self, app) -> bool:
        """Install the catch-all guest handler without risking normal Telegram."""
        if GUEST_UPDATE_TYPE not in Update.ALL_TYPES:
            logger.error(
                "[Telegram Guest] python-telegram-bot 22.8+ is required "
                "for guest_message"
            )
            return False
        installed = getattr(self, "_guest_handler_apps", None)
        if installed is None:
            installed = self._guest_handler_apps = set()
        app_id = id(app)
        if app_id in installed:
            return True
        try:
            app.add_handler(TypeHandler(Update, self._handle_guest_update_and_stop), group=-100)
        except Exception:
            logger.warning(
                "[Telegram Guest] Could not install guest handler; normal Telegram remains active",
                exc_info=True,
            )
            return False
        installed.add(app_id)
        return True

    def _register_handlers(self, app) -> None:
        """Keep Guest wiring on PTB applications rebuilt by older Hermes cores."""
        self._ensure_guest_handler(app)
        super()._register_handlers(app)

    @staticmethod
    def _configured_guest_ids(config, key: str) -> set[str]:
        values: list[Any] = []
        extra = getattr(config, "extra", {}) or {}
        configured = extra.get(key)
        if isinstance(configured, (list, tuple, set)):
            values.extend(configured)
        elif configured:
            values.extend(str(configured).split(","))

        return {str(value).strip() for value in values if str(value).strip()}

    @classmethod
    def _configured_guest_user_ids(cls, config) -> set[str]:
        return cls._configured_guest_ids(config, "allow_from")

    def _guest_caller_authorized(self, message: Message) -> bool:
        extra = getattr(self.config, "extra", {}) or {}
        chat = getattr(message, "chat", None)
        host_chat_id = str(getattr(chat, "id", "") or "").strip()
        host_chat_type = str(getattr(chat, "type", "") or "").strip().lower()
        if host_chat_id and host_chat_type in {"group", "supergroup"}:
            allowed_groups = self._configured_guest_ids(
                self.config, "guest_group_allow_from"
            )
            if host_chat_id in allowed_groups or "*" in allowed_groups:
                return True

        sender_chat = getattr(message, "sender_chat", None)
        sender_chat_id = str(getattr(sender_chat, "id", "") or "").strip()
        user = getattr(message, "from_user", None)
        user_id = str(getattr(user, "id", "") or "").strip()

        if sender_chat_id:
            caller_id = sender_chat_id
            chat_type = "group"
            chat_id = host_chat_id or caller_id
            allowlist_key = "group_allow_from"
        elif user_id:
            caller_id = user_id
            chat_type = "dm"
            chat_id = user_id
            allowlist_key = "allow_from"
        else:
            return False

        # Explicit scope-specific policy wins. Channel-profile Guest messages
        # carry Telegram's fake Channel_Bot in from_user, so authorize the real
        # sender_chat against group_allow_from when configured; otherwise fall
        # back to the global allow_from list shared by every Telegram sender.
        configured_key = allowlist_key
        if sender_chat_id and extra.get("group_allow_from") is None:
            configured_key = "allow_from"
        if extra.get(configured_key) is not None:
            allowed = self._configured_guest_ids(self.config, configured_key)
            return caller_id in allowed or "*" in allowed

        # Use the profile-scoped authorization callback injected by GatewayRunner.
        auth_check = getattr(self, "_authorization_check", None)
        if callable(auth_check):
            try:
                return bool(auth_check(caller_id, chat_type, chat_id))
            except Exception:
                logger.warning(
                    "[Telegram Guest] Central caller authorization failed; denying request",
                    exc_info=True,
                )
                return False

        return False

    async def _enrich_guest_reply_media(
        self,
        message: Message,
        replied: Message,
        event: Any,
    ) -> None:
        """Reuse Hermes' generic replied-media cache for Guest messages."""
        media_count_before = len(event.media_urls)
        await self._cache_replied_media(message, event)
        if (
            getattr(replied, "voice", None) is not None
            and media_count_before == 0
            and len(event.media_urls) == 1
            and len(event.media_types) == 1
            and event.media_types[0].startswith("audio/")
        ):
            # The bundled generic cache classifies all audio as AUDIO. Preserve
            # a standalone voice note as VOICE so the gateway sends it through
            # STT. Mixed events keep their primary type; the per-item audio MIME
            # still routes only the voice attachment through transcription.
            event.message_type = telegram_base.MessageType.VOICE

        if (
            event.message_type == telegram_base.MessageType.VOICE
            and any(
                not media_type.startswith("audio/")
                for media_type in event.media_types
            )
        ):
            # VOICE is a whole-event fallback in Hermes' STT router. Once an
            # event mixes audio with another MIME, use TEXT as the neutral type
            # so each attachment follows its own MIME-specific pipeline.
            event.message_type = telegram_base.MessageType.TEXT

    async def _enrich_guest_direct_media(
        self,
        message: Message,
        content: Message,
        event: Any,
    ) -> None:
        """Cache media carried directly by the Guest query."""
        await self._cache_observed_media(content, event)
        if getattr(content, "voice", None) is not None and event.media_urls:
            event.message_type = telegram_base.MessageType.VOICE

    async def _enrich_guest_location(
        self,
        message: Message,
        content: Message,
        event: Any,
    ) -> None:
        """Append a direct or quoted location as agent-readable context."""
        venue = getattr(content, "venue", None)
        location = (
            getattr(venue, "location", None)
            if venue is not None
            else getattr(content, "location", None)
        )
        lat = getattr(location, "latitude", None)
        lon = getattr(location, "longitude", None)
        if lat is None or lon is None:
            return

        label = "Replied-to Telegram location pin" if content is not message else "Telegram location pin"
        parts = [f"[{label}]"]
        title = getattr(venue, "title", None) if venue is not None else None
        address = getattr(venue, "address", None) if venue is not None else None
        if title:
            parts.append(f"Venue: {title}")
        if address:
            parts.append(f"Address: {address}")
        parts.extend(
            (
                f"latitude: {lat}",
                f"longitude: {lon}",
                f"Map: https://www.google.com/maps/search/?api=1&query={lat},{lon}",
            )
        )
        event.text = self._append_observed_note(event.text, "\n".join(parts))
        event.message_type = telegram_base.MessageType.LOCATION

    async def _enrich_guest_sticker(
        self,
        message: Message,
        content: Message,
        event: Any,
    ) -> None:
        """Describe a direct or quoted sticker while preserving prompt text."""
        prompt = event.text
        await self._handle_sticker(content, event)
        event.text = self._append_observed_note(prompt, event.text)
        event.message_type = telegram_base.MessageType.STICKER

    @classmethod
    def _guest_content_enricher(cls, content: Message, handler_index: int) -> str | None:
        for fields, direct_handler, reply_handler in cls.GUEST_CONTENT_ENRICHERS:
            if any(getattr(content, field, None) is not None for field in fields):
                return (direct_handler, reply_handler)[handler_index]
        return None

    async def _enrich_guest_direct(self, message: Message, event: Any) -> None:
        handler_name = self._guest_content_enricher(message, 0)
        if handler_name is not None:
            await getattr(self, handler_name)(message, message, event)

    async def _enrich_guest_reply(self, message: Message, event: Any) -> None:
        """Dispatch quoted Guest content through the matching Hermes enricher."""
        replied = getattr(message, "reply_to_message", None)
        if replied is None:
            return

        handler_name = self._guest_content_enricher(replied, 1)
        if handler_name is not None:
            await getattr(self, handler_name)(message, replied, event)

    async def _handle_guest_update(self, update: Update, context) -> None:
        raw = (getattr(update, "api_kwargs", None) or {}).get(GUEST_UPDATE_TYPE)
        message = getattr(update, "guest_message", None)
        if message is not None:
            query_id = str(getattr(message, "guest_query_id", "") or "").strip()
        elif isinstance(raw, dict):
            query_id = str(raw.get("guest_query_id") or "").strip()
            try:
                message = Message.de_json(raw, self._bot)
            except Exception:
                logger.warning("[Telegram Guest] Invalid guest_message payload", exc_info=True)
                return
        else:
            return
        if not query_id:
            return
        if not message:
            return
        prompt = getattr(message, "text", None) or getattr(message, "caption", None) or ""
        if not prompt and self._guest_content_enricher(message, 0) is None:
            return
        if not self._guest_caller_authorized(message):
            sender_chat = getattr(message, "sender_chat", None)
            caller = sender_chat or getattr(message, "from_user", None)
            logger.warning(
                "[Telegram Guest] Blocked unauthorized caller %s",
                getattr(caller, "id", None),
            )
            return

        session_chat_id = self._guest_session_chat_id(message, query_id)
        if self._answer_attempted_query_ids.get(query_id, 0) > time.monotonic():
            return
        try:
            state = self._remember_guest_query(session_chat_id, query_id)
        except RuntimeError:
            logger.warning("[Telegram Guest] Query capacity exhausted; rejecting update")
            return
        if state.answer_attempted:
            logger.info(
                "[Telegram Guest] Ignoring duplicate query=%s",
                hashlib.sha256(query_id.encode("utf-8")).hexdigest()[:8],
            )
            return

        user = getattr(message, "from_user", None)
        user_id = getattr(user, "id", None)
        if (getattr(message, "sender_chat", None) is None
                and getattr(user, "is_bot", None) is False
                and type(user_id) is int and user_id > 0):
            state.approval_actor_id = user_id

        event = self._build_message_event(
            message,
            telegram_base.MessageType.TEXT,
            update_id=getattr(update, "update_id", None),
        )
        event.text = prompt
        event.source.chat_id = session_chat_id
        event.source.chat_type = "guest"
        caller = getattr(message, "sender_chat", None) or getattr(message, "from_user", None)
        if getattr(caller, "id", None) is not None:
            event.source.user_id = str(caller.id)
        thread_id_raw = getattr(message, "message_thread_id", None)
        if thread_id_raw is not None and getattr(event.source, "thread_id", None) is None:
            event.source.thread_id = str(thread_id_raw)

        command = self._guest_command(self._clean_bot_trigger_text(prompt))
        if command:
            handled = await self._dispatch_guest_command(
                command, message, session_chat_id, event,
            )
            if handled:
                return

        locks = getattr(self, "_guest_session_locks", None)
        if locks is None:
            locks = self._guest_session_locks = {}
        session_lock = locks.setdefault(session_chat_id, asyncio.Lock())
        async with session_lock:
            active_queries = getattr(self, "_guest_active_query", None)
            if active_queries is None:
                active_queries = self._guest_active_query = {}
            active_queries[session_chat_id] = query_id
            state.placeholder_active = True
            thinking = await self._publish_guest(session_chat_id, GUEST_THINKING_TEXT)
            if not thinking.success:
                state.placeholder_active = False
                active_queries.pop(session_chat_id, None)
                logger.warning(
                    "[Telegram Guest] Could not publish initial thinking response query=%s: %s",
                    hashlib.sha256(query_id.encode("utf-8")).hexdigest()[:8],
                    thinking.error or "unknown error",
                )
                return

            await self._enrich_guest_direct(message, event)
            await self._enrich_guest_reply(message, event)
            self._guest_busy[session_chat_id] = True
            try:
                await self.handle_message(event)
            finally:
                self._guest_busy[session_chat_id] = False
                if active_queries.get(session_chat_id) == query_id:
                    active_queries.pop(session_chat_id, None)

    async def _handle_guest_update_and_stop(self, update: Update, context) -> None:
        """Handle Guest updates exclusively so normal message handlers cannot double-process them."""
        raw = (getattr(update, "api_kwargs", None) or {}).get(GUEST_UPDATE_TYPE)
        if getattr(update, "guest_message", None) is None and not isinstance(raw, dict):
            return
        await self._handle_guest_update(update, context)
        raise ApplicationHandlerStop


    async def _try_edit_guest_rich(
        self,
        state: GuestQueryState,
        content: str,
        query_ref: str,
    ) -> bool:
        """Finalize an existing Guest inline response as Bot API rich content.

        ``True`` means the rich edit succeeded (or Telegram confirmed it was
        already identical). ``False`` means a permanent rejection made a
        MarkdownV2 fallback safe. Ambiguous/transient failures raise so callers
        never issue a second edit after the rich request may have succeeded.
        """
        try:
            await self._bot.do_api_request(
                "editMessageText",
                api_kwargs={
                    "inline_message_id": state.inline_message_id,
                    "rich_message": self._rich_message_payload(content),
                },
            )
        except Exception as exc:
            if "not modified" in str(exc).lower():
                return True
            if self._is_rich_fallback_error(exc):
                if self._is_rich_capability_error(exc):
                    self._rich_send_disabled = True
                logger.info(
                    "[Telegram Guest] Rich final rejected for query=%s; "
                    "falling back to MarkdownV2: %s",
                    query_ref,
                    telegram_base._redact_telegram_error_text(exc),
                )
                return False
            raise GuestEditError(
                telegram_base._redact_telegram_error_text(exc)
            ) from exc
        logger.info(
            "[Telegram Guest] Rich-finalized response query=%s chars=%d",
            query_ref,
            len(content),
        )
        return True

    async def _deliver_guest_approval_card(
        self, *, query_id: str, inline_message_id: str, actor_id: int,
        preview: str, approve_token: str, deny_token: str,
    ) -> bool:
        """Unwired transport only: no callback authentication or approval grant.

        Caller must supply a safe preview and opaque tokens. A failed or
        cancelled edit leaves the bubble locked because delivery is ambiguous.
        """
        if not isinstance(query_id, str) or not query_id:
            return False
        state = self._guest_queries.get(guest_chat_id(query_id))
        if state is None or state.query_id != query_id:
            return False
        if (type(actor_id) is not int or actor_id != state.approval_actor_id
                or not isinstance(inline_message_id, str) or not inline_message_id
                or not isinstance(preview, str) or not preview.strip()
                or telegram_base.utf16_len(preview) > self.MAX_MESSAGE_LENGTH
                or not all(isinstance(t, str) and 1 <= len(t.encode('utf-8')) <= 64
                           for t in (approve_token, deny_token))
                or approve_token == deny_token):
            return False
        async with state.answer_lock:
            if (not self._bot or not state.answered or state.rich_edit_ambiguous
                    or state.inline_message_id != inline_message_id
                    or state.approval_card_active):
                return False
            state.approval_card_active = True
            try:
                result = await self._bot._post("editMessageText", {
                    "inline_message_id": inline_message_id,
                    "text": preview,
                    "reply_markup": {"inline_keyboard": [[
                        {"text": "Подтвердить", "callback_data": approve_token},
                        {"text": "Отклонить", "callback_data": deny_token},
                    ]]},
                })
                return result is not None and result is not False
            except Exception:
                return False

    async def _send_exec_approval_prompt(self, prompt) -> SendResult:
        """Route guest approvals into the exact active guest reply; fail closed otherwise."""
        session_chat_id = str(getattr(prompt, "chat_id", "") or "")
        if not is_guest_chat_id(session_chat_id):
            return await super()._send_exec_approval_prompt(prompt)
        query_id = getattr(self, "_guest_active_query", {}).get(session_chat_id)
        state = self._guest_queries.get(guest_chat_id(query_id)) if query_id else None
        actor_id = getattr(state, "approval_actor_id", None)
        inline_id = getattr(state, "inline_message_id", None)
        if (state is None or not state.answered or not actor_id or not inline_id
                or state.rich_edit_ambiguous):
            return SendResult(success=False, error="Guest approval target is unavailable")

        from tools.approval import list_gateway_approvals, register_gateway_settle
        pending = list_gateway_approvals(prompt.session_key)
        matches = [entry for entry in pending
                   if entry.get("command") == prompt.command
                   and entry.get("description") == prompt.description
                   and "mcp_elicitation" in entry.get("pattern_keys", [])]
        if len(matches) != 1 or not matches[0].get("request_id"):
            return SendResult(success=False, error="Could not uniquely bind the guest approval request")

        approval_id = secrets.token_urlsafe(12)
        approve_token = f"ga:ok:{approval_id}"
        deny_token = f"ga:no:{approval_id}"
        bindings = getattr(self, "_guest_approval_bindings", None)
        if bindings is None:
            bindings = self._guest_approval_bindings = {}
        bindings[approval_id] = {
            "query_id": query_id,
            "inline_message_id": inline_id,
            "actor_id": actor_id,
            "session_key": prompt.session_key,
            "request_id": matches[0]["request_id"],
            "state": state,
        }
        reason = str(getattr(prompt, "description", "") or "Требуется подтвердить действие.")
        preview = f"⚠️ Подтверждение действия\n\n{reason}"
        if telegram_base.utf16_len(preview) > self.MAX_MESSAGE_LENGTH:
            preview = preview[: self.MAX_MESSAGE_LENGTH - 1] + "…"
        delivered = await self._deliver_guest_approval_card(
            query_id=query_id,
            inline_message_id=inline_id,
            actor_id=actor_id,
            preview=preview,
            approve_token=approve_token,
            deny_token=deny_token,
        )
        if not delivered:
            bindings.pop(approval_id, None)
            return SendResult(success=False, error="Guest approval card could not be delivered")
        loop = asyncio.get_running_loop()

        def _settle(reason: str) -> None:
            if loop.is_closed():
                logger.warning("[Telegram Guest] Approval card settlement loop is unavailable")
                return
            coroutine = self._settle_guest_approval_card(approval_id, reason)
            try:
                asyncio.run_coroutine_threadsafe(coroutine, loop)
            except RuntimeError:
                coroutine.close()
                logger.warning("[Telegram Guest] Approval card settlement loop is unavailable")

        if not register_gateway_settle(prompt.session_key, matches[0]["request_id"], _settle):
            # The decision may have raced card publication. Retire any still-live
            # card instead of leaving a button for an approval no longer pending.
            await self._settle_guest_approval_card(approval_id, "already_resolved")
        return SendResult(success=True, message_id=str(inline_id))

    async def _settle_guest_approval_card(self, approval_id: str, reason: str) -> None:
        """Retire a card when its gateway request times out, closes, or resolves elsewhere."""
        bindings = getattr(self, "_guest_approval_bindings", {})
        binding = bindings.pop(approval_id, None)
        if binding is None:
            return
        state = binding["state"]
        if reason == "resolved":
            text = "Подтверждение обработано в другом интерфейсе; кнопки больше не действуют."
        else:
            text = "Подтверждение истекло или сессия завершена; действие не выполнено."
        retired = False
        try:
            async with state.answer_lock:
                if (self._bot and state.approval_card_active
                        and state.inline_message_id == binding["inline_message_id"]):
                    await self._bot._post("editMessageText", {
                        "inline_message_id": binding["inline_message_id"],
                        "text": text,
                        "reply_markup": {"inline_keyboard": []},
                    })
                    retired = True
        except Exception:
            logger.warning("[Telegram Guest] Could not retire settled approval card", exc_info=True)
        if retired:
            state.approval_card_active = False

    async def _handle_guest_approval_callback(self, query, data: str) -> None:
        """Resolve only a live one-shot decision bound to the original guest actor and bubble."""
        parts = data.split(":", 2)
        if len(parts) != 3 or parts[1] not in {"ok", "no"}:
            await query.answer(text="Некорректное подтверждение.")
            return
        binding = getattr(self, "_guest_approval_bindings", {}).get(parts[2])
        if binding is None:
            await query.answer(text="Подтверждение уже истекло.")
            return
        user = getattr(query, "from_user", None)
        user_id = getattr(user, "id", None)
        inline_id = getattr(query, "inline_message_id", None)
        if (type(user_id) is not int or user_id != binding["actor_id"]
                or getattr(user, "is_bot", True) is not False
                or inline_id != binding["inline_message_id"]):
            await query.answer(text="Это подтверждение предназначено другому пользователю.")
            return

        # Claim before awaiting anything: duplicate taps cannot resolve the same gate twice.
        self._guest_approval_bindings.pop(parts[2], None)
        choice = "once" if parts[1] == "ok" else "deny"
        try:
            from tools.approval import resolve_gateway_approval
            count = resolve_gateway_approval(
                binding["session_key"], choice, request_id=binding["request_id"])
        except Exception:
            logger.error("[Telegram Guest] Approval callback resolution failed", exc_info=True)
            count = 0

        state = binding["state"]
        message = "Решение принято. Выполняю действие…" if choice == "once" else "Действие отклонено."
        if count:
            try:
                async with state.answer_lock:
                    await self._bot._post("editMessageText", {
                        "inline_message_id": inline_id,
                        "text": message,
                        "reply_markup": {"inline_keyboard": []},
                    })
            except Exception:
                logger.warning("[Telegram Guest] Could not retire approval card", exc_info=True)
            finally:
                state.approval_card_active = False
            await query.answer(text="Подтверждено." if choice == "once" else "Отклонено.")
        else:
            try:
                async with state.answer_lock:
                    await self._bot._post("editMessageText", {
                        "inline_message_id": inline_id,
                        "text": "Подтверждение истекло; действие не выполнено.",
                        "reply_markup": {"inline_keyboard": []},
                    })
            except Exception:
                logger.warning("[Telegram Guest] Could not retire expired approval card", exc_info=True)
            finally:
                state.approval_card_active = False
            await query.answer(text="Подтверждение истекло.")

    async def _handle_callback_query(self, update, context) -> None:
        """Intercept guest approval callbacks; preserve all bundled Telegram callbacks."""
        query = getattr(update, "callback_query", None)
        data = getattr(query, "data", "") if query else ""
        if isinstance(data, str) and data.startswith("ga:"):
            await self._handle_guest_approval_callback(query, data)
            return
        await super()._handle_callback_query(update, context)

    async def _cancel_guest_approval_card(
        self, *, query_id: str, inline_message_id: str,
    ) -> bool:
        """Remove displayed buttons only; caller must revoke tokens separately.

        This is not authorization or a broker decision. Keep the bubble pinned
        and closed to output even when the transport result is ambiguous.
        """
        state = self._guest_queries.get(guest_chat_id(query_id))
        if state is None or state.query_id != query_id:
            return False
        async with state.answer_lock:
            if (not self._bot or not state.approval_card_active
                    or state.inline_message_id != inline_message_id
                    or state.approval_cancel_attempted):
                return False
            state.approval_cancel_attempted = True
            try:
                result = await self._bot._post("editMessageText", {
                    "inline_message_id": inline_message_id,
                    "text": "Кнопки скрыты; решение не принято.",
                    "reply_markup": {"inline_keyboard": []},
                })
                return result is True
            except Exception:
                return False

    async def _publish_guest(
        self,
        chat_id: str,
        content: str,
        *,
        allow_after_answer: bool = True,
        finalize: bool = False,
        turn_final: bool = False,
    ) -> SendResult:
        state = self._guest_queries.get(chat_id)
        if state is None:
            # Stable-session sends arrive with the session key, not the one-shot
            # query key; resolve through the session -> latest query map.
            query_id = self._guest_session_last_query.get(chat_id)
            if query_id:
                state = self._guest_queries.get(guest_chat_id(query_id))
        if state is None:
            return SendResult(success=False, error="Unknown guest query")
        query_ref = hashlib.sha256(state.query_id.encode("utf-8")).hexdigest()[:8]
        async with state.answer_lock:
            if state.approval_card_active:
                return SendResult(success=False, error="Guest approval card active", retryable=False)
            if state.answered and not allow_after_answer and not state.placeholder_active:
                logger.info(
                    "[Telegram Guest] Suppressed unmarked send after answer query=%s",
                    query_ref,
                )
                return SendResult(
                    success=True,
                    message_id=state.inline_message_id or chat_id,
                )
            if not self._bot:
                return SendResult(success=False, error="Not connected")

            text = (content or state.latest_content or "").strip()
            if (
                state.answered
                and state.latest_content
                and state.latest_content != state.delivered_content
                and text != state.latest_content
                and state.latest_content.endswith(text)
            ):
                # Hermes fallback sends only an un-delivered suffix because most
                # platforms can append a fresh message. Guest Bots own one inline
                # response, so replacing it with that suffix would truncate the
                # visible answer. Retry the complete pending replacement instead.
                text = state.latest_content
            if not text:
                return SendResult(success=False, error="Empty guest response")
            if state.answered and state.rich_edit_ambiguous:
                return SendResult(
                    success=True,
                    message_id=state.inline_message_id or chat_id,
                )
            rich_text = text
            rich_rejected = state.rich_rejected_content == rich_text
            rich_eligible = bool(
                state.answered
                and finalize
                and turn_final
                and not rich_rejected
                and self._rich_eligible(rich_text)
            )
            if (
                state.answered
                and state.delivered_finalized
                and rich_text == state.delivered_content
                and (state.delivered_rich_finalized or not rich_eligible)
            ):
                return SendResult(
                    success=True,
                    message_id=state.inline_message_id or chat_id,
                )
            if not rich_eligible and telegram_base.utf16_len(text) > self.MAX_MESSAGE_LENGTH:
                text = self.truncate_message(
                    text, self.MAX_MESSAGE_LENGTH, len_fn=telegram_base.utf16_len
                )[0]
            state.latest_content = text

            if state.answered:
                if not state.inline_message_id:
                    raise GuestEditError(
                        "Guest response cannot be edited: missing inline_message_id"
                    )
                if rich_eligible:
                    # Consume the one safe rich attempt before awaiting I/O. If
                    # cancellation or a transient error makes the outcome
                    # ambiguous, later gateway reconciliation must not edit the
                    # same inline message again.
                    state.rich_edit_ambiguous = True
                    rich_success = await self._try_edit_guest_rich(
                        state, rich_text, query_ref
                    )
                    state.rich_edit_ambiguous = False
                    if rich_success:
                        state.rich_rejected_content = ""
                        state.delivered_content = rich_text
                        state.delivered_finalized = True
                        state.delivered_rich_finalized = True
                        state.placeholder_active = False
                        return SendResult(
                            success=True,
                            message_id=state.inline_message_id,
                        )
                    rich_eligible = False
                    state.rich_rejected_content = rich_text
                    if telegram_base.utf16_len(rich_text) > self.MAX_MESSAGE_LENGTH:
                        text = self.truncate_message(
                            rich_text,
                            self.MAX_MESSAGE_LENGTH,
                            len_fn=telegram_base.utf16_len,
                        )[0]
                        state.latest_content = text
                if text == state.delivered_content and (
                    not finalize or state.delivered_finalized
                ):
                    state.placeholder_active = False
                    return SendResult(
                        success=True,
                        message_id=state.inline_message_id,
                    )
                try:
                    logger.info(
                        "[Telegram Guest] Editing response query=%s chars=%d",
                        query_ref,
                        len(text),
                    )
                    payload = {
                        "inline_message_id": state.inline_message_id,
                        "text": text,
                    }
                    # Пробуем MarkdownV2 для любого редактирования (в т.ч. прогресс-блоков
                    # вида "💻 terminal ``` ... ```"), чтобы markdown рендерился, а не
                    # показывался сырьём с бэктиками. При сбое парсинга — фолбэк на plain.
                    md_payload = dict(payload)
                    md_payload["text"] = self.format_message(text)
                    md_payload["parse_mode"] = telegram_base.ParseMode.MARKDOWN_V2
                    try:
                        raw_response = await self._bot._post(
                            "editMessageText",
                            md_payload,
                        )
                        state.delivered_finalized = finalize
                        state.delivered_rich_finalized = False
                    except Exception as format_exc:
                        if not isinstance(format_exc, BadRequest):
                            raise
                        if "not modified" in str(format_exc).lower():
                            raw_response = True
                            state.delivered_finalized = finalize
                            state.delivered_rich_finalized = False
                        else:
                            logger.warning(
                                "[Telegram Guest] MarkdownV2 edit failed for query=%s; "
                                "falling back to plain text: %s",
                                query_ref,
                                telegram_base._redact_telegram_error_text(format_exc),
                            )
                            try:
                                raw_response = await self._bot._post(
                                    "editMessageText",
                                    payload,
                                )
                            except BadRequest as plain_exc:
                                if "not modified" not in str(plain_exc).lower():
                                    raise
                                raw_response = True
                            state.delivered_finalized = finalize
                            state.delivered_rich_finalized = False
                except Exception as exc:
                    raise GuestEditError(
                        telegram_base._redact_telegram_error_text(exc)
                    ) from exc
                state.delivered_content = text
                state.placeholder_active = False
                logger.info(
                    "[Telegram Guest] Edited response query=%s result_type=%s",
                    query_ref,
                    type(raw_response).__name__,
                )
                return SendResult(
                    success=True,
                    message_id=state.inline_message_id,
                    raw_response=raw_response,
                )

            if state.answer_attempted:
                return SendResult(
                    success=False,
                    error=state.answer_error or "Guest response was already attempted",
                    retryable=False,
                )
            state.answer_attempted = True
            self._answer_attempted_query_ids[state.query_id] = (
                time.monotonic() + self.ANSWER_ATTEMPT_RETENTION_SECONDS
            )
            result_id = hashlib.sha256(state.query_id.encode("utf-8")).hexdigest()[:32]
            is_thinking = state.placeholder_active and text == GUEST_THINKING_TEXT
            result = InlineQueryResultArticle(
                id=result_id,
                title=getattr(self._bot, "first_name", None) or "Hermes",
                input_message_content=InputTextMessageContent(
                    message_text=text,
                    parse_mode=(
                        telegram_base.ParseMode.HTML if is_thinking else None
                    ),
                ),
            )
            try:
                raw_response = await self._bot._post(
                    "answerGuestQuery",
                    {"guest_query_id": state.query_id, "result": result},
                )
            except Exception as exc:  # noqa: BLE001 - isolate transport failures
                state.answer_error = telegram_base._redact_telegram_error_text(exc)
                return SendResult(
                    success=False,
                    error=state.answer_error,
                    retryable=False,
                )

            state.answered = True
            state.answered_wallclock = time.time()
            inline_message_id = None
            if isinstance(raw_response, dict):
                inline_message_id = raw_response.get("inline_message_id")
            if inline_message_id is None:
                inline_message_id = getattr(raw_response, "inline_message_id", None)
            state.inline_message_id = str(inline_message_id) if inline_message_id else None
            state.delivered_content = text
            state.delivered_finalized = False
            state.delivered_rich_finalized = False
            logger.info(
                "[Telegram Guest] Answered query=%s chars=%d inline_id=%s result_type=%s",
                query_ref,
                len(text),
                bool(state.inline_message_id),
                type(raw_response).__name__,
            )
            return SendResult(
                success=True,
                message_id=state.inline_message_id or chat_id,
                raw_response=raw_response,
            )

    def max_message_length_for_chat(self, chat_id: str) -> int:
        if is_guest_chat_id(chat_id):
            # Prevent StreamConsumer from splitting one replaceable Guest bubble
            # into append-only chunks. _publish_guest enforces Telegram's actual
            # 4096 UTF-16-unit cap before each answer/edit request.
            return self.GUEST_STREAM_CONSUMER_LIMIT
        return super().max_message_length_for_chat(chat_id)

    def _resolve_guest_state(self, chat_id: str) -> GuestQueryState | None:
        state = self._guest_queries.get(str(chat_id))
        if state is not None:
            return state
        query_id = self._guest_session_last_query.get(str(chat_id))
        if query_id:
            return self._guest_queries.get(guest_chat_id(query_id))
        return None

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SendResult:
        if not is_guest_chat_id(chat_id):
            return await super().send(chat_id, content, reply_to=reply_to, metadata=metadata)

        marked_visible = bool(
            metadata and (metadata.get("expect_edits") or metadata.get("notify"))
        )
        # The answered check happens under answer_lock so an auxiliary send queued
        # behind the initial response cannot become an accidental second frame.
        return await self._publish_guest(
            str(chat_id),
            content,
            allow_after_answer=marked_visible,
            finalize=bool(metadata and metadata.get("notify")),
            turn_final=bool(metadata and metadata.get("is_turn_final")),
        )

    async def send_or_update_status(
        self,
        chat_id: str,
        status_key: str,
        content: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> SendResult:
        if not is_guest_chat_id(chat_id):
            return await super().send_or_update_status(
                chat_id,
                status_key,
                content,
                metadata=metadata,
            )
        if self._resolve_guest_state(chat_id) is None:
            return SendResult(success=False, error="Unknown guest query")
        return SendResult(success=True)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> SendResult:
        if not is_guest_chat_id(chat_id):
            return await super().edit_message(
                chat_id,
                message_id,
                content,
                finalize=finalize,
                metadata=metadata,
            )

        state = self._guest_queries.get(str(chat_id))
        if state is None:
            return SendResult(success=False, error="Unknown guest query")
        if state.answered and state.inline_message_id and message_id != state.inline_message_id:
            return SendResult(success=False, error="Guest message identity mismatch", retryable=False)
        return await self._publish_guest(
            str(chat_id),
            content,
            finalize=finalize,
            turn_final=bool(metadata and metadata.get("is_turn_final")),
        )

    async def send_typing(
        self,
        chat_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if is_guest_chat_id(chat_id):
            return
        await super().send_typing(chat_id, metadata=metadata)


def _build_adapter(config):
    return GuestTelegramAdapter(config)


def _patch_stream_consumer_turn_final_metadata() -> None:
    """Bridge explicit turn finality on Hermes cores that drop it before edits.

    Current StreamConsumer builds know whether a finalized frame ends the whole
    turn, but their private ``_edit_message`` helper forwards only static
    metadata. Guest rich content must never infer finality from ``finalize``
    alone because segment, split, and cancellation boundaries also finalize.
    """
    try:
        from gateway.stream_consumer import GatewayStreamConsumer
    except Exception:
        return

    current_send_or_edit = getattr(GatewayStreamConsumer, "_send_or_edit", None)
    current_edit_message = getattr(GatewayStreamConsumer, "_edit_message", None)
    if not callable(current_send_or_edit) or not callable(current_edit_message):
        return
    if getattr(current_send_or_edit, "_telegram_guest_turn_final_patch", False):
        return

    try:
        send_params = inspect.signature(current_send_or_edit).parameters
        edit_params = inspect.signature(current_edit_message).parameters
    except (TypeError, ValueError):
        return
    # A newer core owns this contract itself; never stack a compatibility shim.
    if "is_turn_final" in edit_params:
        return
    # Older cores that do not expose the lifecycle fact cannot be patched safely.
    if "is_turn_final" not in send_params:
        return

    turn_final_attr = "_telegram_guest_current_turn_final"
    missing = object()

    async def _send_or_edit(
        self,
        text: str,
        *,
        finalize: bool = False,
        is_turn_final: bool = True,
    ) -> bool:
        if not is_guest_chat_id(getattr(self, "chat_id", "")):
            return await current_send_or_edit(
                self,
                text,
                finalize=finalize,
                is_turn_final=is_turn_final,
            )
        previous = getattr(self, turn_final_attr, missing)
        setattr(self, turn_final_attr, bool(is_turn_final))
        try:
            return await current_send_or_edit(
                self,
                text,
                finalize=finalize,
                is_turn_final=is_turn_final,
            )
        finally:
            if previous is missing:
                try:
                    delattr(self, turn_final_attr)
                except AttributeError:
                    pass
            else:
                setattr(self, turn_final_attr, previous)

    async def _edit_message(
        self,
        *,
        message_id: str,
        content: str,
        finalize: bool = False,
    ):
        if not is_guest_chat_id(getattr(self, "chat_id", "")):
            return await current_edit_message(
                self,
                message_id=message_id,
                content=content,
                finalize=finalize,
            )

        kwargs = {
            "chat_id": self.chat_id,
            "message_id": message_id,
            "content": content,
            "finalize": finalize,
        }
        metadata = dict(self.metadata) if self.metadata else {}
        if finalize:
            metadata["is_turn_final"] = bool(
                getattr(self, turn_final_attr, False)
            )
        if metadata:
            try:
                params = inspect.signature(self.adapter.edit_message).parameters
                if "metadata" in params or any(
                    param.kind is inspect.Parameter.VAR_KEYWORD
                    for param in params.values()
                ):
                    kwargs["metadata"] = metadata
            except (TypeError, ValueError):
                pass
        return await self.adapter.edit_message(**kwargs)

    _send_or_edit._telegram_guest_turn_final_patch = True  # type: ignore[attr-defined]
    _edit_message._telegram_guest_turn_final_patch = True  # type: ignore[attr-defined]
    GatewayStreamConsumer._send_or_edit = _send_or_edit
    GatewayStreamConsumer._edit_message = _edit_message


_patch_stream_consumer_turn_final_metadata()


def _wire_guest_handler(app, adapter) -> None:
    """Register Guest updates through Hermes' native platform-handler API."""
    if not isinstance(adapter, GuestTelegramAdapter):
        logger.warning(
            "[Telegram Guest] Expected GuestTelegramAdapter, got %s",
            type(adapter).__name__,
        )
        return
    adapter._ensure_guest_handler(app)


def register(ctx) -> None:
    ctx.register_platform_handler("telegram", _wire_guest_handler)
    ctx.register_platform(
        name="telegram",
        label="Telegram",
        adapter_factory=_build_adapter,
        check_fn=telegram_base.check_telegram_requirements,
        is_connected=telegram_base._is_connected,
        required_env=["TELEGRAM_BOT_TOKEN"],
        install_hint="Run `hermes setup` to install Telegram support.",
        setup_fn=telegram_base.interactive_setup,
        apply_yaml_config_fn=telegram_base._apply_yaml_config,
        allowed_users_env="TELEGRAM_ALLOWED_USERS",
        allow_all_env="TELEGRAM_ALLOW_ALL_USERS",
        cron_deliver_env_var="TELEGRAM_HOME_CHANNEL",
        standalone_sender_fn=telegram_base._standalone_send,
        max_message_length=4096,
        emoji="✈️",
        allow_update_command=True,
    )
