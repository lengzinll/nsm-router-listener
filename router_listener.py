import asyncio
import time

from db import batch_insert_router_logs, initdb, delete_old_router_logs
from utils import parse_mikrotik_log

HOST = "0.0.0.0"
PORT = 13196
MAX_QUEUE = 100000

BATCH_SIZE = 5000
BATCH_FLUSH_INTERVAL = 30.0  # seconds - flush partial batches so logs don't stall

RETENTION_DAYS = 180          # keep only last 6 months
CLEANUP_INTERVAL = 24 * 3600   # run cleanup every 1 day

log_queue = asyncio.Queue(maxsize=MAX_QUEUE)

class UDPLogReceiver(asyncio.DatagramProtocol):
    __slots__ = ("queue",)

    def __init__(self, queue):
        self.queue = queue

    def datagram_received(self, data, addr):
        try:
            text = data.decode("utf-8", errors="replace").strip()
            self.queue.put_nowait({"raw": text, "source_ip": addr[0]})
        except asyncio.QueueFull:
            pass  # drop silently under load; print() itself is a CPU cost at high throughput
        except Exception as e:
            print("UDP decode error:", e)


async def log_worker():
    connection = await initdb()
    batch = []
    last_flush = time.monotonic()

    async def flush():
        nonlocal batch, last_flush
        if not batch:
            last_flush = time.monotonic()
            return
        try:
            await batch_insert_router_logs(connection, batch)
            print(f"Batch inserted: {len(batch)} logs")
        except Exception as e:
            print("Batch insert error:", e)
        finally:
            batch = []
            last_flush = time.monotonic()

    while True:
        try:
            # Block until at least one item is available
            event = await asyncio.wait_for(log_queue.get(), timeout=BATCH_FLUSH_INTERVAL)
            log_queue.task_done()
        except asyncio.TimeoutError:
            # No new logs within the flush interval - flush whatever's pending
            await flush()
            continue

        try:
            parsed = parse_mikrotik_log(event["raw"], event["source_ip"])
            batch.append(parsed)
        except Exception as e:
            print("Worker error:", e)

        # Drain any additional items already sitting in the queue without
        # re-awaiting per item, cutting scheduler wakeups drastically.
        while len(batch) < BATCH_SIZE:
            try:
                event = log_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                parsed = parse_mikrotik_log(event["raw"], event["source_ip"])
                batch.append(parsed)
            except Exception as e:
                print("Worker error:", e)
            finally:
                log_queue.task_done()

        if len(batch) >= BATCH_SIZE:
            await flush()
        elif time.monotonic() - last_flush >= BATCH_FLUSH_INTERVAL:
            await flush()


async def cleanup_worker():
    connection = await initdb()
    print("Cleanup worker started")
    while True:
        try:
            deleted = await delete_old_router_logs(connection, RETENTION_DAYS)
            if deleted:
                print(f"Cleanup: deleted {deleted} logs older than {RETENTION_DAYS} days")
        except Exception as e:
            print("Cleanup error:", e)
        await asyncio.sleep(CLEANUP_INTERVAL)


async def main():
    loop = asyncio.get_running_loop()
    receiver = UDPLogReceiver(log_queue)
    transport, protocol = await loop.create_datagram_endpoint(lambda: receiver, local_addr=(HOST, PORT))
    print(f"Listening UDP logs on {PORT}")

    worker_task = asyncio.create_task(log_worker())
    cleanup_task = asyncio.create_task(cleanup_worker())

    try:
        await asyncio.gather(worker_task, cleanup_task)
    finally:
        transport.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested...")
    except Exception as e:
        print("Fatal error:", e)
    finally:
        print("Stopped cleanly")