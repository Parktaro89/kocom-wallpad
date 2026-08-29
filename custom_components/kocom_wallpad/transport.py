"""Transport for Kocom Wallpad."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple
import asyncio
import serial_asyncio
import time

from .const import LOGGER


@dataclass
class AsyncConnection:
    """Async Connection."""
    host: str
    port: Optional[int]
    serial_baud: int = 9600
    connect_timeout: float = 5.0
    reconnect_backoff: Tuple[float, float] = (1.0, 30.0)  # min, max seconds

    def __post_init__(self) -> None:
        """Initialize the connection."""
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._last_activity_mono: float = time.monotonic()
        self._last_reconn_delay: float = 0.0
        self._connected = False
        self._reconn_lock = asyncio.Lock()
        self._read_lock = asyncio.Lock()

    async def open(self) -> None:
        try:
            if self.port is None:
                self._reader, self._writer = await serial_asyncio.open_serial_connection(
                    url=self.host, baudrate=self.serial_baud
                )
                LOGGER.info("Connection opened for serial: %s", self.host)
            else:
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=self.connect_timeout,
                )
                LOGGER.info("Connection opened for socket: %s:%s", self.host, self.port)
            self._connected = True
            self._touch()
        except Exception as e:
            LOGGER.warning("Connection open failed: %r", e)
            self._connected = False
            await self.reconnect()

    async def close(self) -> None:
        self._connected = False
        if self._writer is not None:
            LOGGER.info("Closing connection")
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            finally:
                self._writer = None
        self._reader = None

    def _is_connected(self) -> bool:
        return self._connected and self._writer is not None

    def _touch(self) -> None:
        self._last_activity_mono = time.monotonic()

    def idle_since(self) -> float:
        return max(0.0, time.monotonic() - self._last_activity_mono)

    async def send(self, data: bytes) -> int:
        if not self._is_connected() or not self._writer:
            raise ConnectionResetError("Connection lost")
        try:
            LOGGER.debug("Sending: %s", data.hex())
            self._writer.write(data)
            await self._writer.drain()
            self._touch()
            return len(data)
        except Exception as e:
            LOGGER.warning("Send failed: %r", e)
            asyncio.create_task(self.reconnect())
            raise ConnectionResetError("Connection lost") from e

    async def recv(self, nbytes: int, timeout: float = 0.05) -> bytes:
        if not self._is_connected() or not self._reader:
            await asyncio.sleep(0.5)
            return b""

        async with self._read_lock:
            try:
                # wait_for를 제거하고 데이터가 들어올 때까지 대기
                chunk = await self._reader.read(nbytes)
                if not chunk:
                    LOGGER.warning("Connection closed by peer (EOF)")
                    asyncio.create_task(self.reconnect())
                    return b""
                self._touch()
                return chunk
            except Exception as e:
                LOGGER.warning("Recv failed: %r", e)
                asyncio.create_task(self.reconnect())
                return b""

    async def reconnect(self) -> None:
        if self._reconn_lock.locked():
            return

        async with self._reconn_lock:
            self._connected = False
            delay_min, delay_max = self.reconnect_backoff
            delay = self._last_reconn_delay if self._last_reconn_delay > 0.0 else delay_min

            if self._writer is not None:
                try:
                    self._writer.close()
                    await self._writer.wait_closed()
                except Exception:
                    pass
                self._writer = None
            self._reader = None

            LOGGER.info("Connection lost. Reconnecting in %.1f sec...", delay)
            await asyncio.sleep(delay)
            self._last_reconn_delay = min(delay * 2, delay_max)
            await self.open()

            if self._is_connected():
                LOGGER.info("Connection reconnected")
                self._last_reconn_delay = delay_min