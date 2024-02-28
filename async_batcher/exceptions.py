from __future__ import annotations


class AsyncBatchException(Exception):
    pass


class QueueFullException(AsyncBatchException):
    pass
