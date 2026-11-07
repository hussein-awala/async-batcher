from __future__ import annotations

import abc
import asyncio
import inspect
import logging
import math
import warnings
from collections import namedtuple
from collections.abc import Mapping, Set
from concurrent import futures
from contextvars import ContextVar, copy_context
from functools import partial
from itertools import islice
from numbers import Real
from typing import TYPE_CHECKING, Generic, Self, TypeVar, Unpack, overload
from weakref import ref

from async_batcher.exceptions import QueueFullException
from async_batcher.map import MapOutcome, map_items, map_outcomes
from async_batcher.observability import BatcherStats, BatchEvent, _BatchTiming

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterable, Awaitable, Callable, Iterable, Iterator
    from concurrent.futures import Executor
    from types import TracebackType
    from typing import Protocol

    from async_batcher._options import BatcherOptions, LegacyOptions

T = TypeVar("T")
S = TypeVar("S", covariant=True)

if TYPE_CHECKING:

    class _BatchResults(Protocol[S]):
        def __len__(self) -> int: ...

        def __iter__(self) -> Iterator[S | Exception | asyncio.CancelledError]: ...


# Child tasks inherit this context, but must not keep completed processors alive.
_processing_ancestors: ContextVar[tuple[ref[asyncio.Task], ...]] = ContextVar(
    "async_batcher_processing_ancestors", default=()
)


def _run_sync_batch(processor, batch):
    try:
        return processor(batch)
    except StopIteration as error:
        # Older supported asyncio versions cannot transfer StopIteration from
        # an executor Future, leaving the awaiting worker unresolved.
        raise RuntimeError("Synchronous process_batch raised StopIteration") from error


class AsyncBatcher(Generic[T, S], abc.ABC):
    """Group items by size or age and process batches asynchronously.

    Args:
        max_batch_size: Maximum items per batch, or -1 for no limit. Defaults to -1.
        max_queue_time: Maximum seconds to collect a partial batch, measured from
            its oldest item's arrival. A busy processor can delay dispatch beyond
            this limit. Defaults to 0.01.
        max_queue_size: Maximum queued items, or -1/0 for no limit. Defaults to -1.
        max_batch_cost: Optional positive, finite budget for each batch. Requires
            item_cost. Both count and cost limits apply when configured.
        item_cost: Synchronous function returning a nonnegative, finite real cost.
            Called once per submission. Items exceeding max_batch_cost are rejected
            before enqueueing. Requires max_batch_cost.
        concurrency: Maximum concurrent batches, or -1 for no limit. Defaults to 1.
        executor: Same-interpreter executor for synchronous processors, such as
            ThreadPoolExecutor or a custom executor. None uses asyncio's default
            thread executor. ProcessPoolExecutor and InterpreterPoolExecutor are
            unsupported because batchers hold event-loop state that cannot be
            transferred between processes or interpreters.
        on_batch: Optional synchronous callback receiving a payload-free BatchEvent
            after each dispatched batch finishes cleanup. It must return quickly
            without blocking the event loop. Ordinary callback errors are logged
            and do not change request outcomes.
    """

    logger = logging.getLogger(__name__)
    QueueItem = namedtuple("QueueItem", ["item", "future", "enqueued_at", "cost"], defaults=[0])

    def __init__(
        self,
        *,
        max_batch_size: int = -1,
        max_queue_time: float = 0.01,
        concurrency: int = 1,
        max_queue_size: int = -1,
        max_batch_cost: float | Real | None = None,
        item_cost: Callable[[T], float | Real] | None = None,
        executor: Executor | None = None,
        on_batch: Callable[[BatchEvent], None] | None = None,
        **kwargs: Unpack[LegacyOptions],
    ) -> None:
        super().__init__()
        for argument in ("sleep_time", "buffering_time"):
            if argument in kwargs:
                kwargs.pop(argument)
                warnings.warn(
                    f"The '{argument}' has no effect and will be removed in a future version.",
                    stacklevel=2,
                )
        if "batch_size" in kwargs:
            warnings.warn(
                "The 'batch_size' argument is deprecated and will be removed in a future version. "
                "Use 'max_batch_size' instead.",
                stacklevel=2,
            )
            max_batch_size = kwargs.pop("batch_size")
        if kwargs:
            raise TypeError(f"Unexpected arguments: {', '.join(sorted(kwargs))}")
        for name, value in (("max_batch_size", max_batch_size), ("concurrency", concurrency)):
            if isinstance(value, bool) or not isinstance(value, int) or (value != -1 and value < 1):
                raise ValueError(f"{name} must be a positive integer or -1 for no limit")
        if isinstance(max_queue_size, bool) or not isinstance(max_queue_size, int) or max_queue_size < -1:
            raise ValueError("max_queue_size must be a nonnegative integer or -1 for no limit")
        if (
            not isinstance(max_queue_time, Real)
            or isinstance(max_queue_time, bool)
            or not math.isfinite(max_queue_time)
            or max_queue_time < 0
        ):
            raise ValueError("max_queue_time must be a finite, nonnegative number")
        if (max_batch_cost is None) != (item_cost is None):
            raise ValueError("max_batch_cost and item_cost must be configured together")
        if max_batch_cost is not None:
            self._validate_cost(max_batch_cost, "max_batch_cost", positive=True)
            if not callable(item_cost) or inspect.iscoroutinefunction(item_cost):
                raise TypeError("item_cost must be a synchronous callable")
        if executor is not None:
            unsupported_executors = (futures.ProcessPoolExecutor,)
            interpreter_executor = getattr(futures, "InterpreterPoolExecutor", None)
            if interpreter_executor is not None:
                unsupported_executors += (interpreter_executor,)
            if isinstance(executor, unsupported_executors):
                raise TypeError(
                    f"{type(executor).__name__} is not supported; executor must run in the same interpreter"
                )
        if on_batch is not None:
            if not callable(on_batch):
                raise TypeError("on_batch must be a synchronous callable")
            callback = on_batch.__call__
            if (
                inspect.iscoroutinefunction(on_batch)
                or inspect.iscoroutinefunction(callback)
                or inspect.isasyncgenfunction(on_batch)
                or inspect.isasyncgenfunction(callback)
            ):
                raise TypeError("on_batch must be a synchronous callable")
        self.max_batch_size = max_batch_size
        self.max_queue_time = max_queue_time
        self.max_batch_cost = max_batch_cost
        self.item_cost = item_cost
        self.concurrency = concurrency
        self.executor = executor
        self._on_batch = on_batch
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue = asyncio.Queue(maxsize=max_queue_size)
        self._next_batch_item: AsyncBatcher.QueueItem | None = None
        self._queue_changed = asyncio.Event()
        self._queue_space_available = asyncio.Event()
        self._shutdown_generation = 0
        self._current_task: asyncio.Task | None = None
        self._running_batches: dict[int, asyncio.Task] = {}
        self._processing_tasks: set[asyncio.Task] = set()
        self._pending_futures: set[asyncio.Future] = set()
        self._concurrency_semaphore = asyncio.Semaphore(concurrency) if concurrency > 0 else None
        self._stop = asyncio.Event()
        self._is_running = asyncio.Event()
        self._force_stopped = False
        self._context_entered = False
        self._accepted = 0
        self._rejected = 0
        self._succeeded = 0
        self._failed = 0
        self._cancelled = 0
        self._in_flight_items = 0
        self._batches_dispatched = 0
        self._batches_completed = 0

    @staticmethod
    @overload
    def from_callable(
        processor: Callable[[list[T]], _BatchResults[S] | Awaitable[_BatchResults[S]]],
        **kwargs: Unpack[BatcherOptions[T]],
    ) -> AsyncBatcher[T, S]: ...

    @staticmethod
    @overload
    def from_callable(
        processor: Callable[[list[T]], None | Awaitable[None]],
        **kwargs: Unpack[BatcherOptions[T]],
    ) -> AsyncBatcher[T, None]: ...

    @staticmethod
    @overload
    def from_callable(
        processor: Callable[[list[T]], _BatchResults[S] | Awaitable[_BatchResults[S] | None] | None],
        **kwargs: Unpack[BatcherOptions[T]],
    ) -> AsyncBatcher[T, S | None]: ...

    @staticmethod
    def from_callable(
        processor: Callable[[list[T]], _BatchResults[S] | Awaitable[_BatchResults[S] | None] | None],
        **kwargs: Unpack[BatcherOptions[T]],
    ) -> AsyncBatcher[T, S | None]:
        """Wrap a bulk function using the same options and lifecycle as a subclass.

        Async functions and objects with an async __call__ run on the event loop.
        Regular functions run in the configured thread executor. The function
        receives the input list as one positional argument.
        """
        from async_batcher.callable import create_batcher

        return create_batcher(processor, **kwargs)

    async def __aenter__(self) -> Self:
        self._get_loop()
        if self._context_entered:
            raise RuntimeError("Batcher context is already entered")
        if self._stop.is_set() and (
            (self._current_task is not None and not self._current_task.done()) or self._running_batches
        ):
            raise RuntimeError("Batcher is stopping")
        self._context_entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Drain accepted work even when the body fails; leave caller resources open."""
        try:
            await self.stop()
        finally:
            self._context_entered = False

    @staticmethod
    def _validate_cost(value, name, *, positive=False):
        valid = isinstance(value, Real) and not isinstance(value, bool)
        if valid:
            try:
                valid = math.isfinite(value) and (value > 0 if positive else value >= 0)
            except OverflowError:
                valid = False
        if not valid:
            sign = "positive" if positive else "nonnegative"
            raise ValueError(f"{name} must be a finite, {sign} real number")

    def _queued_count(self) -> int:
        return self._queue.qsize() + (self._next_batch_item is not None)

    def _queue_is_full(self):
        return self._queue.maxsize > 0 and self._queued_count() >= self._queue.maxsize

    @staticmethod
    def _outcome(future: asyncio.Future) -> str:
        if future.cancelled():
            return "cancelled"
        if future.exception() is not None:
            return "failed"
        return "succeeded"

    def _request_done(self, future: asyncio.Future) -> None:
        self._pending_futures.remove(future)
        outcome = self._outcome(future)
        setattr(self, f"_{outcome}", getattr(self, f"_{outcome}") + 1)

    def stats(self) -> BatcherStats:
        """Read an immutable snapshot on the batcher's event-loop thread.

        Reading does not bind a loop or reset totals. It examines outstanding
        futures so completion callbacks queued for the next loop turn do not
        make the snapshot stale. Rejections count admission refusals on the
        owning loop; invalid arguments and foreign-loop calls are excluded.
        """
        outcomes = {"succeeded": self._succeeded, "failed": self._failed, "cancelled": self._cancelled}
        pending = 0
        for future in self._pending_futures:
            if future.done():
                outcomes[self._outcome(future)] += 1
            else:
                pending += 1
        return BatcherStats(
            queued_items=self._queued_count(),
            pending_items=pending,
            in_flight_items=self._in_flight_items,
            active_batches=self._batches_dispatched - self._batches_completed,
            accepted=self._accepted,
            rejected=self._rejected,
            batches_dispatched=self._batches_dispatched,
            batches_completed=self._batches_completed,
            **outcomes,
        )

    @abc.abstractmethod
    def process_batch(self, batch: list[T]) -> _BatchResults[S] | Awaitable[_BatchResults[S] | None] | None:
        """Return one result per item in an ordered, sized collection, or None.

        Results must follow input order. Mappings, sets, and text/byte containers
        are invalid. The complete result collection is validated before any
        request succeeds; result iteration errors fail the entire batch.

        Subclasses may implement this as a synchronous method. Exceptions raised
        by the processor fail the entire batch; exception objects in the result
        list fail only their corresponding requests. A returned asyncio.CancelledError
        cancels only its corresponding request. StopIteration becomes RuntimeError
        with the original exception as its cause. Synchronous processors receive
        a copy of the batch worker's context.
        """

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise RuntimeError("Batcher must be used from the same event loop")
        return loop

    def map(
        self,
        source: Iterable[T] | AsyncIterable[T],
        *,
        max_pending: int = 128,
        ordered: bool = True,
    ) -> AsyncGenerator[S, None]:
        """Yield results with at most max_pending consumed, unyielded items.

        Errors fail iteration promptly, including in ordered mode. Closing the
        iterator cancels its request tasks and closes its source, without stopping
        this batcher. Use contextlib.aclosing when breaking iteration early.
        """
        return map_items(self.process, source, max_pending=max_pending, ordered=ordered)

    def map_outcomes(
        self,
        source: Iterable[T] | AsyncIterable[T],
        *,
        max_pending: int = 128,
        ordered: bool = True,
    ) -> AsyncGenerator[MapOutcome[T, S], None]:
        """Yield indexed results and ordinary item errors with bounded consumption.

        Source errors and cancellation still terminate iteration. Source ownership
        and cleanup follow map(); use contextlib.aclosing when breaking early.
        """
        return map_outcomes(self.process, source, max_pending=max_pending, ordered=ordered)

    async def _wait_for_queue_space(self, generation: int):
        while True:
            if self._stop.is_set() or generation != self._shutdown_generation:
                raise RuntimeError("Batcher is stopping")
            if not self._queue_is_full():
                return
            self._queue_space_available.clear()
            await self._queue_space_available.wait()

    def _begin_shutdown(self):
        if not self._stop.is_set():
            self._shutdown_generation += 1
        self._stop.set()
        self._queue_changed.set()
        self._queue_space_available.set()

    async def process(
        self,
        item: T,
        *,
        wait_for_space: bool = False,
        enqueue_timeout: float | None = None,
    ) -> S:
        """Enqueue an item and await its result, restarting after completed shutdown.

        Submissions during shutdown are rejected until the dispatcher and its
        batch tasks finish cleanup. The first process(), run(), or stop() call
        binds the batcher to its event loop; later calls from other loops fail
        before changing batcher state, including after shutdown.

        A full queue rejects immediately unless wait_for_space is true. An optional
        finite, nonnegative enqueue_timeout limits only that admission wait, not
        processing. Cancellation or shutdown before admission never enqueues the item.
        """
        if not isinstance(wait_for_space, bool):
            raise TypeError("wait_for_space must be a boolean")
        if enqueue_timeout is not None:
            try:
                valid_timeout = (
                    not isinstance(enqueue_timeout, bool)
                    and isinstance(enqueue_timeout, Real)
                    and math.isfinite(enqueue_timeout)
                    and enqueue_timeout >= 0
                )
            except OverflowError:
                valid_timeout = False
            if not valid_timeout:
                raise ValueError("enqueue_timeout must be a finite, nonnegative number or None")
            if not wait_for_space:
                raise ValueError("enqueue_timeout requires wait_for_space=True")
        loop = self._get_loop()
        cost = 0
        if self.item_cost is not None:
            cost = self.item_cost(item)
            # A mistakenly async callback must not leak an unawaited coroutine.
            if inspect.iscoroutine(cost):
                cost.close()
            self._validate_cost(cost, "item_cost result")
            if cost > self.max_batch_cost:
                raise ValueError("item_cost result exceeds max_batch_cost")
        if self._stop.is_set():
            if (self._current_task is not None and not self._current_task.done()) or self._running_batches:
                self._rejected += 1
                raise RuntimeError("Batcher is stopping")
            self._stop.clear()
            self._force_stopped = False
            self._current_task = None
        if self._queue_is_full():
            if not wait_for_space:
                self._rejected += 1
                raise QueueFullException("The queue is full, cannot process more items at the moment.")
            try:
                async with asyncio.timeout(enqueue_timeout):
                    await self._wait_for_queue_space(self._shutdown_generation)
            except (TimeoutError, RuntimeError):
                self._rejected += 1
                raise
        future = loop.create_future()
        self._pending_futures.add(future)
        future.add_done_callback(self._request_done)
        self._queue.put_nowait(self.QueueItem(item, future, loop.time(), cost))
        self._accepted += 1
        self._queue_changed.set()
        if self._current_task is None:
            self._current_task = loop.create_task(self._run())
        return await future

    async def _fill_batch_from_queue(self) -> list[QueueItem]:
        batch = []
        batch_cost = 0
        loop = asyncio.get_running_loop()
        deadline = None
        while self.max_batch_size < 0 or len(batch) < self.max_batch_size:
            try:
                if self._next_batch_item is not None:
                    item = self._next_batch_item
                    self._next_batch_item = None
                else:
                    item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                if self._stop.is_set():
                    break
                self._queue_changed.clear()
                if deadline is None:
                    await self._queue_changed.wait()
                else:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    try:
                        await asyncio.wait_for(self._queue_changed.wait(), timeout=remaining)
                    except TimeoutError:
                        break
            else:
                if self.max_batch_cost is not None and batch_cost + item.cost > self.max_batch_cost:
                    # Keep this request ahead of the queue with its original age.
                    # It still counts toward queue capacity until collection resumes.
                    self._next_batch_item = item
                    break
                batch.append(item)
                # Moving a request into a batch frees shared queue capacity,
                # including a request retained after a cost-limited split.
                self._queue_space_available.set()
                batch_cost += item.cost
                if deadline is None:
                    deadline = item.enqueued_at + self.max_queue_time
                if self.max_batch_cost is not None and batch_cost >= self.max_batch_cost:
                    break
        return batch

    async def _batch_run(self, batch: list[QueueItem], timing: _BatchTiming):
        if self._force_stopped:
            return
        task = asyncio.current_task()
        self._processing_tasks.add(task)
        context_token = _processing_ancestors.set((*_processing_ancestors.get(), ref(task)))
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        timing.started_at = started_at
        try:
            batch_items = [q_item.item for q_item in batch]
            if inspect.iscoroutinefunction(self.process_batch):
                results = await self.process_batch(batch=batch_items)
            else:
                context = copy_context()
                results = await loop.run_in_executor(
                    self.executor, context.run, _run_sync_batch, self.process_batch, batch_items
                )
            if results is None:
                results = [None] * len(batch)
            elif isinstance(results, (Mapping, Set, str, bytes, bytearray)):
                raise TypeError(
                    "process_batch must return an ordered result collection, not a mapping, set or text"
                )
            if len(results) != len(batch):
                raise ValueError(f"Expected to get {len(batch)} results, but got {len(results)}.")
            # Snapshot before resolving any request. Iteration can fail or disagree
            # with __len__; bound it so malformed collections cannot grow forever.
            results = list(islice(results, len(batch) + 1))
            if len(results) > len(batch):
                raise ValueError(f"Expected to get {len(batch)} results, but got more.")
            if len(results) < len(batch):
                raise ValueError(f"Expected to get {len(batch)} results, but got {len(results)}.")
        except Exception as error:
            self.logger.error("Error processing batch", exc_info=True)
            for q_item in batch:
                if not q_item.future.done():
                    q_item.future.set_exception(error)
        else:
            for q_item, result in zip(batch, results, strict=True):
                if q_item.future.done():
                    continue
                if isinstance(result, asyncio.CancelledError):
                    q_item.future.cancel(str(result) if result.args else None)
                elif isinstance(result, StopIteration):
                    error = RuntimeError("process_batch returned StopIteration")
                    error.__cause__ = result
                    q_item.future.set_exception(error)
                elif isinstance(result, Exception):
                    q_item.future.set_exception(result)
                else:
                    q_item.future.set_result(result)
        finally:
            timing.finished_at = loop.time()
            _processing_ancestors.reset(context_token)
            self._processing_tasks.discard(task)
            self.logger.debug(
                "Processed batch of %s elements in %s seconds.", len(batch), loop.time() - started_at
            )

    def _batch_done(self, task_id: int, batch: list[QueueItem], timing: _BatchTiming, task: asyncio.Task):
        # A done callback also runs when a task is cancelled before its first step.
        self._running_batches.pop(task_id, None)
        self._in_flight_items -= len(batch)
        self._batches_completed += 1
        if self._concurrency_semaphore is not None:
            self._concurrency_semaphore.release()
        error = None if task.cancelled() else task.exception()
        for q_item in batch:
            if not q_item.future.done():
                if error is not None:
                    q_item.future.set_exception(error)
                else:
                    q_item.future.cancel()
        if self._on_batch is not None:
            completed_at = asyncio.get_running_loop().time()
            outcomes = {"succeeded": 0, "failed": 0, "cancelled": 0}
            for q_item in batch:
                outcomes[self._outcome(q_item.future)] += 1
            self._notify_batch(
                BatchEvent(
                    batch_id=timing.batch_id,
                    size=len(batch),
                    queue_wait_seconds=(timing.started_at if timing.started_at is not None else completed_at)
                    - batch[0].enqueued_at,
                    processing_seconds=(
                        timing.finished_at - timing.started_at if timing.started_at is not None else None
                    ),
                    **outcomes,
                )
            )

    def _notify_batch(self, event: BatchEvent) -> None:
        try:
            returned = self._on_batch(event)
            if inspect.isawaitable(returned):
                if inspect.iscoroutine(returned):
                    returned.close()
                elif isinstance(returned, asyncio.Future):
                    # The observer may return a borrowed request Future/Task.
                    # Observe late failures without changing its cancellation.
                    returned.add_done_callback(
                        lambda future: None if future.cancelled() else future.exception()
                    )
                else:
                    # Never drive an unknown __await__: even creating its
                    # iterator can execute application code. Only an explicit
                    # synchronous cleanup protocol grants that ownership.
                    close = getattr(returned, "close", None)
                    if callable(close) and not (
                        inspect.iscoroutinefunction(close)
                        or inspect.isasyncgenfunction(close)
                        or inspect.iscoroutinefunction(close.__call__)
                        or inspect.isasyncgenfunction(close.__call__)
                    ):
                        cleanup = close()
                        if inspect.iscoroutine(cleanup):
                            cleanup.close()
                raise TypeError("on_batch must not return an awaitable")
        except Exception:
            self.logger.error("Error in on_batch callback", exc_info=True)

    def _cancel_pending(self):
        for future in self._pending_futures:
            future.cancel()
        while not self._queue.empty():
            self._queue.get_nowait()
        self._next_batch_item = None

    async def run(self) -> None:
        """Start a dispatcher and wait until all accepted work has finished.

        Processing starts a dispatcher automatically. Only one may run. The
        dispatcher has its own task, so shutdown does not wait for unrelated
        work performed by this method's caller after run() returns.
        """
        loop = self._get_loop()
        if self._current_task is not None:
            raise RuntimeError("Batcher already has a dispatcher")
        self._current_task = loop.create_task(self._run())
        await self._current_task

    async def _run(self):
        current_task = asyncio.current_task()
        if self._current_task is not None and self._current_task is not current_task:
            raise RuntimeError("Batcher already has a dispatcher")
        self._current_task = current_task
        self._is_running.set()
        task_id = 0
        try:
            while not self._should_stop():
                semaphore_acquired = False
                try:
                    if self._concurrency_semaphore is not None:
                        await self._concurrency_semaphore.acquire()
                        semaphore_acquired = True
                    batch = await self._fill_batch_from_queue()
                    if self._force_stopped:
                        break
                    if batch:
                        self._batches_dispatched += 1
                        self._in_flight_items += len(batch)
                        timing = _BatchTiming(self._batches_dispatched)
                        worker = self._batch_run(batch, timing)
                        try:
                            task = asyncio.create_task(worker)
                        except BaseException:
                            worker.close()
                            self._batches_dispatched -= 1
                            self._in_flight_items -= len(batch)
                            raise
                        self._running_batches[task_id] = task
                        task.add_done_callback(partial(self._batch_done, task_id, batch, timing))
                        # The completion callback now owns this semaphore permit.
                        semaphore_acquired = False
                        task_id += 1
                finally:
                    if semaphore_acquired:
                        self._concurrency_semaphore.release()
            # A ready queue event can consume cancellation inside wait_for().
            # Enforce forced shutdown even when the dispatcher exits normally.
            if self._force_stopped:
                for task in self._running_batches.values():
                    task.cancel()
            if self._running_batches:
                await asyncio.shield(asyncio.gather(*self._running_batches.values(), return_exceptions=True))
        except BaseException:
            # A caller may cancel the public run() task directly. Later stop()
            # calls must join this cleanup rather than cancel it a second time.
            self._force_stopped = True
            self._begin_shutdown()
            self._cancel_pending()
            for task in self._running_batches.values():
                task.cancel()
            if self._running_batches:
                await asyncio.gather(*self._running_batches.values(), return_exceptions=True)
            raise
        finally:
            self._is_running.clear()

    def _should_stop(self):
        return self._stop.is_set() and self._queued_count() == 0

    async def is_running(self) -> bool:
        """Return whether the dispatcher is running or draining active batches."""
        return self._is_running.is_set()

    async def stop(self, force: bool = False, timeout: float | None = None) -> None:
        """Reject new work while waiting for shutdown.

        Graceful shutdown flushes the queue and waits for active batches. A timeout
        raises asyncio.TimeoutError while draining continues. Forced shutdown cancels all
        pending results and awaits task cleanup. Running executor functions cannot
        be interrupted and may finish after forced shutdown returns. A later process()
        call restarts the batcher once dispatcher and batch-task cleanup is complete.
        A processor or its child tasks cannot await shutdown while that processor
        is active, because shutdown must wait for the processor itself.
        """
        self._get_loop()
        if asyncio.current_task() in self._processing_tasks or any(
            ancestor() in self._processing_tasks for ancestor in _processing_ancestors.get()
        ):
            raise RuntimeError(
                "stop() cannot be awaited from process_batch; call it from outside the processor"
            )
        self._begin_shutdown()
        if force and not self._force_stopped:
            self._force_stopped = True
            self._cancel_pending()
            if self._current_task is not None:
                self._current_task.cancel()
        tasks = list(self._running_batches.values())
        if self._current_task is not None:
            tasks.append(self._current_task)
        if tasks:
            # shield prevents a timeout or a cancelled stop caller from destroying
            # accepted work. return_exceptions permits expected forced cancellation.
            results = await asyncio.wait_for(
                asyncio.shield(asyncio.gather(*tasks, return_exceptions=True)), timeout=timeout
            )
            for result in results:
                if isinstance(result, Exception):
                    raise result
