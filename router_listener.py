import asyncio
import time

from db import batch_insert_router_logs, initdb
from utils import parse_mikrotik_log

HOST = "0.0.0.0"
PORT = 13196
MAX_Queue = 100000

# Async queue
log_queue = asyncio.Queue(maxsize=MAX_Queue)

class UDPLogReceiver(asyncio.DatagramProtocol):
    def __init__(self, queue):
        self.queue = queue
        self.total_logs = 0
        self.total_bytes = 0
        self.start_time = time.time()

    def datagram_received(self, data, addr):
        try:
            text = data.decode("utf-8", errors="replace").strip()
            event = {
                "raw": text,
                "source_ip": addr[0],
            }

            # Put into asyncio queue
            try:
                self.queue.put_nowait(event)
                self.total_logs += 1
                self.total_bytes += len(data)
            except asyncio.QueueFull:
                print("Queue full - dropping log")

        except Exception as e:
            print(
                "UDP decode error:",
                e
            )

async def log_worker():

    connection = await initdb()
    batch = []
    batch_size = 5000

    while True:
        event = await log_queue.get()
        try:
            parsed = parse_mikrotik_log(event["raw"], event["source_ip"])
            batch.append(parsed)
            if len(batch) >= batch_size:
                try:
                    await batch_insert_router_logs( connection, batch )
                    print(f"Inserted {len(batch)} logs")
                    batch.clear()
                except Exception as e:
                    print("Batch insert error:",e)
        except Exception as e:
            print("Worker error:",e )
        finally:
            log_queue.task_done()
        await asyncio.sleep(0)

async def statistics(receiver):
    while True:
        await asyncio.sleep(10)
        mb = ( receiver.total_bytes / 1024 / 1024 )
        print(
            f"""
========= STATUS =========
Logs received : {receiver.total_logs}
Data received : {mb:.2f} MB
Queue size    : {log_queue.qsize()}
==========================
"""
        )

async def main():
    loop = asyncio.get_running_loop()
    receiver = UDPLogReceiver( log_queue )
    transport, protocol = await loop.create_datagram_endpoint(lambda: receiver,local_addr=(HOST,PORT))
    print(f"Listening UDP logs on {PORT}")

    # Start async worker
    worker_task = asyncio.create_task(log_worker())
    stats_task = asyncio.create_task(statistics(receiver))

    try:
        await asyncio.gather(worker_task,stats_task)
    finally:
        transport.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested...")
    except Exception as e:
        print("Fatal error:",e)
    finally:
        print("Stopped cleanly")