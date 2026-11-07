from __future__ import annotations

import pytest

pytest.importorskip("fastapi", reason="FastAPI recipes require the example-test dependency group")
pytest.importorskip("httpx2", reason="HTTP recipe tests require the example-test dependency group")
