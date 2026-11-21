"""Bounded, explicitly opted-in FIFO handoff to UditBot's existing executor.

ONE worker calls the unchanged TradingBotPipeline.process_raw_message() in
one thread. It never parallelizes journal claims, risk reservations, STOP,
broker requests, or other mutations. No messages are silently dropped due to
capacity; normal backpressure awaits queue space. Unclean process death is
still a restart gap—not a durable inbound-message guarantee.

Default production runner path is unchanged unless enabled by operator flag.
"""
from __future__ import annotations
import asyncio
from dataclasses import dataclass
import math
import time
from typing import Any, Callable


class IngressClosed(RuntimeError):
    pass


@dataclass
class _Request:
    kwargs: dict
    future: asyncio.Future
    queued_ns: int


class OrderedSignalDispatcher:
    def __init__(self, processor: Callable[..., Any], *,
                 max_pending: int = 64):
        if not callable(processor):
            raise TypeError("callable processor required")
        if type(max_pending) is not int or not 1 <= max_pending <= 10000:
            raise ValueError("max_pending must be in 1..10000")
        self.processor=processor
        self.queue=asyncio.Queue(maxsize=max_pending)
        self._worker: asyncio.Task | None=None
        self._closed=False
        self._started=False
        self._pending_admissions=0
        self._admissions_drained=asyncio.Event()
        self._admissions_drained.set()
        self.accepted=0
        self.completed=0
        self.processing_failures=0
        self.max_observed_backlog=0
        self.queue_wait_ms: list[float]=[]

    async def start(self):
        if self._started:
            raise RuntimeError("dispatcher already started")
        self._started=True
        self._worker=asyncio.create_task(
            self._run(),name="uditbot-ordered-ingress"
        )

    async def submit(self, **kwargs):
        if not self._started or self._closed:
            raise IngressClosed("dispatcher is not accepting new messages")
        loop=asyncio.get_running_loop()
        request=_Request(dict(kwargs),loop.create_future(),time.perf_counter_ns())
        # Await capacity, but count unfinished admissions separately so a
        # shutdown cannot cancel the worker before a waiting put is handled.
        self._pending_admissions+=1
        self._admissions_drained.clear()
        try:
            await self.queue.put(request)
            self.accepted+=1
            self.max_observed_backlog=max(
                self.max_observed_backlog,self.queue.qsize()
            )
        finally:
            self._pending_admissions-=1
            if self._pending_admissions==0:
                self._admissions_drained.set()
        try:
            return await asyncio.shield(request.future)
        except asyncio.CancelledError:
            # Once queued, processing still proceeds. Dropping an acknowledged
            # submission because the callback task was canceled is unsafe.
            raise

    async def _run(self):
        while True:
            request=await self.queue.get()
            try:
                wait_ms=max(0.0,(time.perf_counter_ns()-request.queued_ns)/1_000_000)
                self.queue_wait_ms.append(wait_ms)
                if len(self.queue_wait_ms)>500:
                    self.queue_wait_ms.pop(0)
                try:
                    result=await asyncio.to_thread(self.processor,**request.kwargs)
                except Exception as error:
                    # Do not swallow asyncio.CancelledError (BaseException).
                    # A distinct exception is delivered to the callback rather
                    # than sharing the executor exception/traceback object with
                    # the long-running worker coroutine. This prevents a
                    # re-entrant exception traceback from destabilizing the
                    # worker on older Python runtimes.
                    self.processing_failures+=1
                    reason=type(error).__name__
                    if not request.future.done():
                        request.future.set_exception(
                            RuntimeError("Ordered ingress processing failed: "+reason)
                        )
                else:
                    if not request.future.done():
                        request.future.set_result(result)
            finally:
                self.completed+=1
                self.queue.task_done()

    async def close(self):
        self._closed=True
        if self._worker is None:
            return
        # First resolve ALL callbacks that entered admission before close().
        # They may be blocked waiting for queue capacity.
        await self._admissions_drained.wait()
        # Now drain all accepted submissions before ending the worker. It
        # does not itself cancel or retry any Dhan broker mutation.
        await self.queue.join()
        self._worker.cancel()
        try:
            await self._worker
        except asyncio.CancelledError:
            pass
        self._worker=None

    def metrics(self):
        values=sorted(self.queue_wait_ms)
        def pct(p):
            if not values:
                return None
            i=(len(values)-1)*p/100.0
            lo=int(math.floor(i))
            hi=int(math.ceil(i))
            return round(values[lo]+(values[hi]-values[lo])*(i-lo),3)
        return {
            "accepted":self.accepted,
            "completed":self.completed,
            "pending":self.queue.qsize(),
            "processing_failures":self.processing_failures,
            "max_observed_backlog":self.max_observed_backlog,
            "queue_wait_p50_ms":pct(50),
            "queue_wait_p95_ms":pct(95),
            "queue_wait_p99_ms":pct(99),
            "capacity_drop_count":0,
        }
