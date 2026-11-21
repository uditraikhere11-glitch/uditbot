"""Offline FIFO semantics, bounded backpressure and cancellation safety."""
import asyncio
import time
import unittest

from src.engine.ordered_ingress import IngressClosed, OrderedSignalDispatcher


class TestOrderedIngress(unittest.IsolatedAsyncioTestCase):
    async def test_no_parallel_execution_authority(self):
        current=0
        peak=0
        seen=[]
        def worker(*,number):
            nonlocal current,peak
            current+=1
            peak=max(current,peak)
            time.sleep(0.002)
            seen.append(number)
            current-=1
            return number*2
        disp=OrderedSignalDispatcher(worker,max_pending=16)
        await disp.start()
        # Schedule in known admission order, yielding each turn to the loop.
        tasks=[]
        for i in range(12):
            tasks.append(asyncio.create_task(disp.submit(number=i)))
            await asyncio.sleep(0)
        self.assertEqual(await asyncio.gather(*tasks),[i*2 for i in range(12)])
        await disp.close()
        self.assertEqual(seen,list(range(12)))
        self.assertEqual(peak,1)
        self.assertEqual(disp.metrics()["capacity_drop_count"],0)

    async def test_bounded_queue_does_not_discard_burst(self):
        received=[]
        def worker(*,number):
            time.sleep(0.005)
            received.append(number)
            return number
        disp=OrderedSignalDispatcher(worker,max_pending=2)
        await disp.start()
        vals=await asyncio.gather(*(disp.submit(number=i) for i in range(20)))
        await disp.close()
        self.assertEqual(vals,list(range(20)))
        self.assertEqual(received,list(range(20)))
        m=disp.metrics()
        self.assertEqual(m["accepted"],20)
        self.assertEqual(m["completed"],20)
        self.assertLessEqual(m["max_observed_backlog"],2)

    async def test_worker_exception_is_visible_and_subsequent_work_continues(self):
        def worker(*,number):
            if number==1:
                raise RuntimeError("synthetic failure")
            return number
        disp=OrderedSignalDispatcher(worker)
        await disp.start()
        self.assertEqual(await asyncio.wait_for(disp.submit(number=0),timeout=3),0)
        with self.assertRaises(RuntimeError):
            await asyncio.wait_for(disp.submit(number=1),timeout=3)
        self.assertEqual(await asyncio.wait_for(disp.submit(number=2),timeout=3),2)
        await asyncio.wait_for(disp.close(),timeout=3)
        self.assertEqual(disp.processing_failures,1)

    async def test_shutdown_drains_accepted_work(self):
        completed=[]
        def worker(*,number):
            time.sleep(0.002)
            completed.append(number)
            return number
        disp=OrderedSignalDispatcher(worker,max_pending=8)
        await disp.start()
        tasks=[]
        for i in range(8):
            tasks.append(asyncio.create_task(disp.submit(number=i)))
            await asyncio.sleep(0)
        await disp.close()
        await asyncio.gather(*tasks)
        self.assertEqual(completed,list(range(8)))
        with self.assertRaises(IngressClosed):
            await disp.submit(number=9)

    async def test_close_waits_for_pending_admissions_before_worker_cancel(self):
        observed=[]
        def worker(*,number):
            time.sleep(0.004)
            observed.append(number)
            return number
        disp=OrderedSignalDispatcher(worker,max_pending=1)
        await disp.start()
        tasks=[]
        for i in range(9):
            tasks.append(asyncio.create_task(disp.submit(number=i)))
            await asyncio.sleep(0)
        self.assertGreaterEqual(disp._pending_admissions,1)
        # Close begins while some callbacks are blocked waiting for capacity.
        await disp.close()
        self.assertEqual(await asyncio.gather(*tasks),list(range(9)))
        self.assertEqual(observed,list(range(9)))
        self.assertEqual(disp.metrics()["accepted"],9)
        self.assertEqual(disp.metrics()["completed"],9)

    async def test_cancelled_callback_does_not_cancel_queued_trade(self):
        recorded=[]
        def worker(*,number):
            recorded.append(number)
            time.sleep(0.005)
            return number
        disp=OrderedSignalDispatcher(worker)
        await disp.start()
        t=asyncio.create_task(disp.submit(number=1))
        await asyncio.sleep(0.001)
        t.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await t
        await disp.close()
        self.assertEqual(recorded,[1])
        self.assertEqual(disp.metrics()["completed"],1)

    async def test_cancellation_while_waiting_for_capacity_cannot_lose_signal(self):
        # Telethon callback cancellation used to remove a blocked Queue.put()
        # producer without durable admission, silently dropping a valid signal.
        seen=[]
        def worker(*,number):
            time.sleep(0.005)
            seen.append(number)
            return number
        disp=OrderedSignalDispatcher(worker,max_pending=1)
        await disp.start()
        tasks=[]
        for i in range(10):
            tasks.append(asyncio.create_task(disp.submit(number=i)))
            await asyncio.sleep(0)
        self.assertGreater(disp._pending_admissions,1)
        tasks[-1].cancel()  # most recent admission is still waiting
        with self.assertRaises(asyncio.CancelledError):
            await tasks[-1]
        await asyncio.wait_for(disp.close(),timeout=5)
        self.assertEqual(await asyncio.gather(*tasks[:-1]),list(range(9)))
        self.assertEqual(seen,list(range(10)))
        report=disp.metrics()
        self.assertEqual(report["accepted"],10)
        self.assertEqual(report["completed"],10)
        self.assertEqual(report["detached_callbacks"],1)
        self.assertEqual(report["capacity_drop_count"],0)

    async def test_fifo_during_repeated_saturation_and_shutdown(self):
        seen=[]
        def worker(*,number):
            time.sleep(0.002)
            seen.append(number)
            return number
        disp=OrderedSignalDispatcher(worker,max_pending=1)
        await disp.start()
        tasks=[]
        for i in range(30):
            tasks.append(asyncio.create_task(disp.submit(number=i)))
            await asyncio.sleep(0)
        await asyncio.wait_for(disp.close(),timeout=5)
        self.assertEqual(await asyncio.gather(*tasks),list(range(30)))
        self.assertEqual(seen,list(range(30)))
        self.assertEqual(disp.metrics()["max_observed_backlog"],1)

    async def test_misconfiguration_is_rejected(self):
        for v in (0,-1,1.5,True,20000):
            with self.subTest(v=v), self.assertRaises(ValueError):
                OrderedSignalDispatcher(lambda **kw:None,max_pending=v)
