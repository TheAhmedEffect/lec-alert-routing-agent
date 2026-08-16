"""
Channel adapters — and the distinction that makes row R2 possible.

A PERSON IS NOT THEIR TRANSPORT
-------------------------------
Slack going down and a human going offline are different events with different
correct responses, and conflating them causes a whole class of pointless
re-routes. So transports live in their own table (`channel_health`) and fail
independently of presence.

The property that matters most is Channel.is_persistent, defined back in
schemas.py:

    Slack        synchronous — if nobody is at the keyboard, nothing happens
    Email, SMS   persistent  — the message waits on the device

That is why a presence drop mid-SMS is a NON-EVENT (decision matrix row R2) and
the same drop mid-Slack is a real problem (row R3). "Offline" means away from
keyboard, not unreachable.

The adapters here are simulated, with injectable latency and injectable failure.
That is deliberate rather than lazy: real transports would make the demo
scenarios non-deterministic, and a walkthrough whose outcome changes between
takes cannot be narrated.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import config
from .models_orm import ChannelHealth
from .schemas import Channel, StakeholderRecord

Clock = Callable[[], float]
SessionFactory = async_sessionmaker[AsyncSession]


class ChannelError(RuntimeError):
    """Base class for transport faults. Never a routing decision by itself."""


class ChannelConnectError(ChannelError):
    """The transport refused the handshake.

    Maps to attempt state `failed`, NOT `aborted`. The distinction matters: the
    transport refused us, we did not change our mind. Both are terminal and both
    keep the person in `attempted`, but conflating them makes the audit trail
    lie about what happened.
    """


class ChannelSendError(ChannelError):
    """The handshake succeeded and the payload did not land."""


@dataclass
class ChannelAdapter:
    """One simulated transport.

    Latency is injectable so tests run instantly while the demo keeps a window
    wide enough for an interrupt to land inside it. Failures are injectable so
    Appendix A's `failover` scenario is reproducible rather than hoped for.
    """

    channel: Channel
    connect_seconds: float = 0.05
    send_seconds: float = 0.25
    fail_on_connect: bool = False
    fail_on_send: bool = False
    #: Every (recipient, body) this adapter actually delivered. The demo reads
    #: it, and tests assert on it to prove an aborted dispatch sent NOTHING.
    delivered: list[tuple[str, str]] = field(default_factory=list)

    @property
    def is_persistent(self) -> bool:
        return self.channel.is_persistent

    async def connect(self, recipient_id: str) -> None:
        await asyncio.sleep(self.connect_seconds)
        if self.fail_on_connect:
            raise ChannelConnectError(
                f"{self.channel.value} adapter refused connection for {recipient_id}"
            )

    async def send(self, recipient_id: str, body: str) -> str:
        """Transmit. THIS IS THE CANCELLABLE PART.

        The await below is the abort window: while it is suspended the dispatch
        can be cancelled cleanly, because nothing has left the building yet. The
        append happens only after the sleep completes, so a cancelled send
        provably delivers nothing — which is what
        test_abort_pre_commit_sends_nothing asserts against.
        """
        await asyncio.sleep(self.send_seconds)
        if self.fail_on_send:
            raise ChannelSendError(
                f"{self.channel.value} adapter failed to deliver to {recipient_id}"
            )
        receipt = f"{self.channel.value}-{len(self.delivered) + 1:04d}"
        self.delivered.append((recipient_id, body))
        return receipt


class ChannelBank:
    """The set of adapters, one per channel, plus failure injection.

    Scenarios reach for `fail(Channel.SLACK)` to reproduce the `failover` case
    deterministically. Nothing in the routing path may consult this to make a
    decision — transport health belongs in `channel_health`, which is what the
    decision matrix reads.
    """

    def __init__(
        self,
        adapters: dict[Channel, ChannelAdapter] | None = None,
        *,
        connect_seconds: float = 0.05,
        send_seconds: float = 0.25,
    ) -> None:
        self.adapters = adapters or {
            channel: ChannelAdapter(
                channel, connect_seconds=connect_seconds, send_seconds=send_seconds
            )
            for channel in Channel
        }

    def __getitem__(self, channel: Channel) -> ChannelAdapter:
        return self.adapters[channel]

    def fail(
        self, channel: Channel, *, on_connect: bool = True, on_send: bool = False
    ) -> None:
        adapter = self.adapters[channel]
        adapter.fail_on_connect = on_connect
        adapter.fail_on_send = on_send

    def heal(self, channel: Channel) -> None:
        adapter = self.adapters[channel]
        adapter.fail_on_connect = False
        adapter.fail_on_send = False

    @property
    def delivered(self) -> list[tuple[Channel, str, str]]:
        """Everything actually transmitted, across all transports.

        The single most useful assertion in Module 3's suite: after an aborted
        dispatch this list must be empty for that recipient.
        """
        return [
            (channel, recipient, body)
            for channel, adapter in self.adapters.items()
            for recipient, body in adapter.delivered
        ]

    def reset(self) -> None:
        for adapter in self.adapters.values():
            adapter.delivered.clear()
            adapter.fail_on_connect = False
            adapter.fail_on_send = False


# ─────────────────────────────────────────────────────────────────────────────
# Transport health
# ─────────────────────────────────────────────────────────────────────────────


async def healthy_channels(
    session_factory: SessionFactory, person: StakeholderRecord
) -> tuple[Channel, ...]:
    """This person's working transports, in preference order.

    Reads `channel_health`, which init_db() seeds with one healthy row per
    (person, channel) in their channel_order. Because those rows always exist, a
    missing row means "not a channel this person uses" rather than "unknown" —
    absence is unambiguous, which is exactly why they were seeded.

    Order is preserved from channel_order: preferred first, then fallbacks. That
    sequence is what row R6 walks during a failover.
    """
    async with session_factory() as session:
        rows = (
            await session.scalars(
                select(ChannelHealth).where(
                    ChannelHealth.stakeholder_id == person.id,
                    ChannelHealth.healthy == 1,
                )
            )
        ).all()
    healthy = {Channel(row.channel) for row in rows}
    return tuple(c for c in person.channel_order if c in healthy)


async def first_healthy_channel(
    session_factory: SessionFactory, person: StakeholderRecord
) -> Channel | None:
    channels = await healthy_channels(session_factory, person)
    return channels[0] if channels else None


async def first_persistent_channel(
    session_factory: SessionFactory, person: StakeholderRecord
) -> Channel | None:
    """The best healthy transport that survives the recipient being offline.

    Row R4 needs this: when the incumbent drops offline and nobody qualified is
    available to take over, the honest move is to keep them and switch to a
    channel that will still be waiting when they come back.
    """
    for channel in await healthy_channels(session_factory, person):
        if channel.is_persistent:
            return channel
    return None
