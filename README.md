# LightQueue

LightQueue is an asynchronous background task queue and worker engine for Python 3.11+. It handles immediate and scheduled background tasks across processes without requiring an external message broker for local development.

## Why it exists

Celery was built before Python had native async support. It relies on worker threads that block during I/O and uses a fork of multiprocessing with known memory leaks. RQ is lighter, but it relies on Unix `fork()`, which makes it unstable on macOS and unsupported on Windows.

LightQueue uses a hybrid runtime:
- Network I/O, broker polling, and signal handling run as coroutines on the main `asyncio` event loop.
- CPU-heavy synchronous tasks run in a `ProcessPoolExecutor` to bypass Python's Global Interpreter Lock (GIL).
- Asynchronous tasks (`async def`) execute directly on the event loop, avoiding inter-process serialization overhead.

## Architecture

```
[ Producer ] -> (delay / apply_async)
                     |
                     v
             [ BaseBroker ]
            /              \
 [ InMemoryBroker ]   [ RedisBroker ]
            \              /
             v            v
      [ WorkerCoordinator (asyncio) ]
         - Concurrency semaphore
         - Scheduled task polling
         - Signal trap (SIGINT/SIGTERM)
               /            \
              / (async)      \ (sync)
             v                v
      [ Event Loop ]    [ ProcessPool ]
```

### Components

- `TaskMessage`: Data envelope storing UUID, function name, serialized arguments, retry counts, execution deadlines, and scheduled run timestamps.
- `TaskRegistry`: Mapping between string task names and executable Python functions, with dynamic import fallback for worker subprocesses.
- `BaseBroker`: Abstract interface specifying `enqueue`, `dequeue`, `ack`, `push_dlq`, and `poll_scheduled`.
- `InMemoryBroker`: Built on `asyncio.Queue` for ready tasks and a priority min-heap (`heapq`) for delayed runs. Tracks in-flight tasks in memory so unit tests run without external dependencies.
- `RedisBroker`: Uses Redis Lists (`LPUSH`/`BRPOP`) for ready tasks, Sorted Sets (`ZADD`/`ZRANGEBYSCORE`) for scheduled tasks, and Hashes for in-flight tracking.
- `WorkerCoordinator`: Polls the broker using an `asyncio.Semaphore` to cap concurrency, dispatches jobs, and runs a periodic scheduler loop.
- `TaskExecutor`: Executes functions with timeout enforcement via `asyncio.wait_for`. Synchronous functions run in child processes; coroutines run in the event loop.

### Reliability and failure handling

- **Backpressure**: The coordinator acquires a semaphore slot before pulling tasks from the broker. Workers do not prefetch more work than they can process.
- **Scheduled execution**: Tasks submitted with `countdown` or `eta` wait in a priority min-heap or Redis Sorted Set. The coordinator checks for due tasks every 100 milliseconds and promotes them to the ready queue.
- **Exponential backoff with jitter**: Failed tasks retry with a delay calculated as `min(max_delay, base * (2 ** retries)) + uniform(0, 1)`. The randomized jitter prevents retrying tasks from hitting recovering services at the same millisecond.
- **Dead-letter queue**: When a task reaches its maximum retry limit, it moves to the dead-letter queue along with its error message and traceback.
- **Graceful shutdown**: On `SIGINT` or `SIGTERM`, the coordinator stops pulling new tasks and allows active jobs up to 10 seconds to finish. Tasks that exceed the timeout are canceled and the process pool shuts down cleanly.

## Requirements and installation

- Python 3.11 or newer
- Redis 5.0+ (optional, only required when using `RedisBroker`)

Clone the repository and install dependencies:

```bash
git clone https://github.com/your-username/lightqueue.git
cd lightqueue
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
pip install pytest pytest-asyncio fakeredis
```

## Usage

### Defining and enqueuing tasks

```python
from lightqueue.app import LightQueue
from lightqueue.broker.memory import InMemoryBroker

app = LightQueue(broker=InMemoryBroker())

@app.task(max_retries=3, timeout=10.0)
def process_data(user_id: int):
    # Runs in a child process
    return f"Processed user {user_id}"

@app.task(max_retries=2, timeout=5.0)
async def send_webhook(url: str, payload: dict):
    # Runs directly on the asyncio loop
    import httpx
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json=payload)
        return resp.status_code

# Enqueue for immediate execution
await process_data.delay(42)

# Schedule to run 30 seconds in the future
await send_webhook.apply_async(
    args=["https://example.com/hook", {"status": "ok"}],
    countdown=30.0,
)
```

### Running the worker

Start a worker from Python:

```python
import asyncio
from lightqueue.worker.coordinator import WorkerCoordinator

async def main():
    coordinator = WorkerCoordinator(broker=app.broker, concurrency=4)
    coordinator.install_signal_handlers()
    await coordinator.run()

asyncio.run(main())
```

Or start it through the command-line interface:

```bash
# In-memory broker
python -m lightqueue.cli worker --concurrency 4 --broker memory

# Redis broker
python -m lightqueue.cli worker --concurrency 8 --broker redis --redis-url redis://localhost:6379/0 --app myapp.tasks
```

## Running tests

Run the 18 automated unit and integration tests:

```bash
python -m pytest -v
```

Tests cover serialization roundtrips, in-memory queueing, min-heap scheduling, process timeouts, concurrency limits, retries, dead-letter routing, graceful draining on shutdown, CLI parsing, and Redis operations through `fakeredis`.
