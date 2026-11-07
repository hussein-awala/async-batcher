from __future__ import annotations

from typing import TYPE_CHECKING

from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from sklearn.base import BaseEstimator


class SklearnAsyncBatcher(AsyncBatcher):
    """Batcher for scikit-learn models.

    Dense feature rows are passed through as a batch. Sparse feature vectors or
    single-row matrices are stacked as CSR without densifying; dense rows can be
    mixed into such a batch. Sparse predictions are returned as one CSR row per
    request, preserving sparsity and the leading singleton sample dimension.

    Args:
        model: The scikit-learn model to use for prediction.
        max_batch_size (int, optional): The max number of items to process in a batch.
            Defaults to -1 (no limit).
        max_queue_time (float, optional): Collection delay measured from the oldest queued input.
            Busy processors can delay dispatch beyond this limit. Defaults to 0.01 seconds.
        concurrency (int, optional): The max number of concurrent batches to process.
            Defaults to 1. If -1, it will process all batches concurrently.
        max_queue_size (int, optional): Maximum queued inputs. -1 and 0 mean no limit. Defaults to -1.
        executor (Executor, optional): The executor to use to process the batch.
            If None, it will use the default asyncio executor. Defaults to None.
        **kwargs: Additional core batcher options, including max_batch_cost, item_cost,
            and on_batch. Costs are evaluated on each original input sample before
            dense or sparse samples are combined. Hooks run on the event loop.
    """

    def __init__(
        self,
        *,
        model: BaseEstimator,
        max_batch_size: int = -1,
        max_queue_time: float = 0.01,
        concurrency: int = 1,
        max_queue_size: int = -1,
        executor: Executor | None = None,
        **kwargs,
    ):
        super().__init__(
            max_batch_size=max_batch_size,
            max_queue_time=max_queue_time,
            concurrency=concurrency,
            max_queue_size=max_queue_size,
            executor=executor,
            **kwargs,
        )
        self.model = model

    def process_batch(self, batch):
        if not batch:
            return []
        if not hasattr(self.model, "predict"):
            raise AttributeError("Model does not have a predict method")

        from scipy import sparse

        inputs = batch
        if any(sparse.issparse(row) for row in batch):
            rows = []
            for row in batch:
                if sparse.issparse(row) and row.ndim == 1:
                    row = row.reshape((1, -1))
                row = sparse.csr_matrix(row)
                if row.shape[0] != 1:
                    raise ValueError("Each input in a sparse batch must contain exactly one sample")
                rows.append(row)
            inputs = sparse.vstack(rows, format="csr")
        predictions = self.model.predict(inputs)
        if sparse.issparse(predictions):
            if predictions.ndim != 2 or predictions.shape[0] != len(batch):
                raise ValueError(f"Expected {len(batch)} predictions as rows of a sparse matrix")
            predictions = predictions.tocsr()
            return [predictions[index : index + 1] for index in range(len(batch))]
        return predictions
