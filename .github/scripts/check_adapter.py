from __future__ import annotations

import importlib
import sys

# Fail explicitly when a selected extra or Keras backend is absent. Otherwise
# importorskip could make an incorrectly provisioned adapter job appear green.
MODULES = {
    "aws": ("aioboto3",),
    "sqlalchemy": ("sqlalchemy", "aiosqlite"),
    "scylla": ("cassandra.cluster",),
    "keras": ("jax", "keras"),
    "sklearn": ("sklearn",),
}

if __name__ == "__main__":
    for name in MODULES[sys.argv[1]]:
        module = importlib.import_module(name)
        print(f"Imported {name}: {getattr(module, '__version__', 'available')}")
