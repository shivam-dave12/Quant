"""Small async event bus with explicit publication rather than global mutable state."""
from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

@dataclass(frozen=True)
class Event:
    topic: str
    payload: Any
    ts_ns: int

Handler = Callable[[Event], Awaitable[None]]

class EventBus:
    def __init__(self) -> None:
        self._handlers: dict[str, list[Handler]] = {}
        self._lock = asyncio.Lock()
    async def subscribe(self, topic: str, handler: Handler) -> None:
        async with self._lock:
            self._handlers.setdefault(topic, []).append(handler)
    async def publish(self, event: Event) -> None:
        async with self._lock:
            targets = tuple(self._handlers.get(event.topic, ()))
        if targets:
            await asyncio.gather(*(h(event) for h in targets))
