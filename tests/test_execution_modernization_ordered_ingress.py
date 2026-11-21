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
        self.assertEqual(await disp.submit(number=0),0)
        with self.assertRaises(RuntimeError):
            await disp.submit(number=1)
        self.assertEqual(await disp.submit(number=2),2)
        await disp.close()
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

    async def test_misconfiguration_is_rejected(self):
        for v in (0,-1,1.5,True,20000):
            with self.subTest(v=v), self.assertRaises(ValueError):
                OrderedSignalDispatcher(lambda **kw:None,max_pending=v)
