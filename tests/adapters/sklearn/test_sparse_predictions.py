from __future__ import annotations

import asyncio
from unittest.mock import Mock

import pytest

np = pytest.importorskip("numpy")
sparse = pytest.importorskip("scipy.sparse")
pytest.importorskip("sklearn")

from async_batcher.ml.sklearn.model import SklearnAsyncBatcher  # noqa: E402
from sklearn.linear_model import LogisticRegression  # noqa: E402
from sklearn.multiclass import OneVsRestClassifier  # noqa: E402


@pytest.fixture
def multilabel_model():
    inputs = np.asarray([[0, 0], [0, 1], [1, 0], [1, 1]])
    targets = sparse.csr_matrix(inputs)
    return OneVsRestClassifier(LogisticRegression()).fit(inputs, targets)


@pytest.mark.asyncio
@pytest.mark.parametrize("input_kind", ["dense", "matrix", "array", "mixed"])
async def test_sparse_multilabel_predictions_are_routed_without_densifying(multilabel_model, input_kind):
    dense = np.asarray([[1, 1], [0, 0], [1, 0]])
    inputs = list(dense)
    if input_kind == "matrix":
        inputs = [sparse.csr_matrix(row) for row in dense]
    elif input_kind == "array":
        inputs = [sparse.csr_array(row) for row in dense]
    elif input_kind == "mixed":
        inputs[0] = sparse.coo_matrix([dense[0]])
    expected = multilabel_model.predict(dense).tocsr()
    batcher = SklearnAsyncBatcher(model=multilabel_model, max_batch_size=3)
    try:
        results = await asyncio.wait_for(asyncio.gather(*(batcher.process(row) for row in inputs)), 5)
        assert len(results) == len(inputs)
        for index, result in enumerate(results):
            assert sparse.issparse(result)
            assert result.shape == (1, 2)
            np.testing.assert_array_equal(result.toarray(), expected[index : index + 1].toarray())
    finally:
        await batcher.stop()


def test_sparse_features_work_with_dense_prediction_models():
    inputs = np.asarray([[0, 0], [0, 1], [1, 0], [1, 1]])
    model = LogisticRegression().fit(sparse.csr_matrix(inputs), [0, 0, 1, 1])
    batcher = SklearnAsyncBatcher(model=model)
    np.testing.assert_array_equal(
        batcher.process_batch([sparse.csc_matrix([row]) for row in inputs]), model.predict(inputs)
    )


def test_sparse_input_cannot_smuggle_multiple_samples_into_one_request():
    model = Mock()
    with pytest.raises(ValueError, match="one sample"):
        SklearnAsyncBatcher(model=model).process_batch([sparse.csr_matrix([[1, 2], [3, 4]])])
    model.predict.assert_not_called()


@pytest.mark.parametrize("rows", [1, 3])
def test_sparse_output_row_count_is_checked_before_routing(rows):
    model = Mock()
    model.predict.return_value = sparse.csr_array((rows, 2))
    with pytest.raises(ValueError, match="2 predictions"):
        SklearnAsyncBatcher(model=model).process_batch([[1, 2], [3, 4]])


def test_empty_batch_does_not_call_the_estimator():
    model = Mock()
    assert SklearnAsyncBatcher(model=model).process_batch([]) == []
    model.predict.assert_not_called()


@pytest.mark.parametrize("sparse_type", [sparse.csc_matrix, sparse.coo_array, sparse.csr_array])
def test_sparse_prediction_formats_remain_sparse(sparse_type, monkeypatch):
    predictions = sparse_type([[1, 0], [0, 1]])
    monkeypatch.setattr(predictions, "toarray", Mock(side_effect=AssertionError("Do not densify outputs")))
    model = Mock()
    model.predict.return_value = predictions
    results = SklearnAsyncBatcher(model=model).process_batch([[1, 2], [3, 4]])
    for index, result in enumerate(results):
        assert sparse.issparse(result)
        assert result.shape == (1, 2)
        np.testing.assert_array_equal(result.toarray(), [[int(index == 0), int(index == 1)]])
    predictions.toarray.assert_not_called()


@pytest.mark.asyncio
async def test_missing_predict_fails_callers_and_replacement_model_recovers():
    batcher = SklearnAsyncBatcher(model=object(), max_batch_size=2, max_queue_time=1)
    try:
        failures = await asyncio.wait_for(
            asyncio.gather(batcher.process([1, 2]), batcher.process([3, 4]), return_exceptions=True), 5
        )
        assert all(isinstance(error, AttributeError) for error in failures)
        assert all("predict method" in str(error) for error in failures)
        batcher.model = Mock()
        batcher.model.predict.return_value = [0, 1]
        results = await asyncio.wait_for(asyncio.gather(batcher.process([1, 2]), batcher.process([3, 4])), 5)
        assert results == [0, 1]
        batcher.model.predict.assert_called_once_with([[1, 2], [3, 4]])
    finally:
        await batcher.stop(force=True, timeout=5)


def test_sparse_output_requires_a_sample_axis_even_when_lengths_match():
    model = Mock()
    model.predict.return_value = sparse.csr_array([0, 1])
    with pytest.raises(ValueError, match="2 predictions as rows of a sparse matrix"):
        SklearnAsyncBatcher(model=model).process_batch([[1, 2], [3, 4]])
