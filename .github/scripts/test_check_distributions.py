from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
import venv
from email.parser import BytesParser
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

from check_distributions import validate_metadata, validate_wheel, verify_origin

SCRIPT = Path(__file__).with_name("check_distributions.py").resolve()


class DistributionValidationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.project = {
            "name": "async-batcher",
            "version": "0.0.1",
            "requires-python": ">=3.11,<4.0",
            "dependencies": [],
            "optional-dependencies": {"aws": ["aioboto3>=15.5,<16"]},
        }
        self.metadata = (
            b"Metadata-Version: 2.4\nName: async-batcher\nVersion: 0.0.1\n"
            b"Requires-Python: >=3.11, <4.0\nProvides-Extra: aws\n"
            b"Requires-Dist: aioboto3>=15.5,<16 ; extra == 'aws'\n\n"
        )
        self.contents = {
            "async_batcher/__init__.py": b"from .batcher import AsyncBatcher\n",
            "async_batcher/batcher.py": b"class AsyncBatcher: pass\n",
            "async_batcher/aws/dynamodb/get.py": b"class AsyncDynamoDbGetBatcher: pass\n",
        }
        for name, content in self.contents.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
        (self.root / "LICENSE").write_bytes(b"license text\n")

    def wheel(self, *, omit=None, metadata=None, changed=None):
        path = self.root / "test.whl"
        with ZipFile(path, "w") as archive:
            for name, content in (self.contents | (changed or {})).items():
                if name != omit:
                    archive.writestr(name, content)
            archive.writestr("async_batcher-0.0.1.dist-info/METADATA", metadata or self.metadata)
            archive.writestr("async_batcher-0.0.1.dist-info/licenses/LICENSE", b"license text\n")
        return path

    def test_complete_wheel_preserves_module_files_and_optional_metadata(self):
        validate_wheel(self.wheel(), self.root, self.project)

    def test_missing_core_or_adapter_module_fails_even_though_checkout_contains_it(self):
        for name in self.contents:
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "package files differ"):
                validate_wheel(self.wheel(omit=name), self.root, self.project)

    def test_stale_packaged_module_fails(self):
        with self.assertRaisesRegex(ValueError, "different source bytes"):
            validate_wheel(
                self.wheel(changed={"async_batcher/batcher.py": b"stale = True\n"}), self.root, self.project
            )

    def test_wrong_identity_python_support_extra_or_requirement_is_rejected(self):
        for before, after in (
            ("Name: async-batcher", "Name: other-project"),
            ("Version: 0.0.1", "Version: 1.0.0"),
            ("Requires-Python: >=3.11, <4.0", "Requires-Python: >=3.12"),
            ("Provides-Extra: aws\n", ""),
            ("Provides-Extra: aws", "Provides-Extra: unknown"),
            ("aioboto3>=15.5,<16", "aioboto3>=1"),
            (" ; extra == 'aws'", ""),
            ("extra == 'aws'", "extra == 'other'"),
        ):
            with self.subTest(after=after), self.assertRaises(ValueError):
                validate_wheel(
                    self.wheel(metadata=self.metadata.replace(before.encode(), after.encode())),
                    self.root,
                    self.project,
                )

    def test_conditional_extra_keeps_its_python_marker(self):
        project = self.project | {
            "optional-dependencies": {"aws": ['aioboto3>=15.5,<16; python_version >= "3.12"']}
        }
        metadata = self.metadata.replace(b"extra == 'aws'", b"(python_version >= '3.12') and extra == 'aws'")
        validate_metadata(BytesParser().parsebytes(metadata), project)

    def test_checkout_origin_is_rejected(self):
        module = SimpleNamespace(
            __name__="async_batcher", __file__=str(self.root / "async_batcher/__init__.py")
        )
        with self.assertRaisesRegex(ValueError, "outside installed site-packages"):
            verify_origin(module, self.root / "environment/lib/site-packages")

    def test_missing_install_cannot_be_hidden_by_pythonpath_pointing_at_checkout(self):
        environment = self.root / "empty-environment"
        venv.EnvBuilder(with_pip=False).create(environment)
        python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        expectations = self.root / "expected.json"
        expectations.write_text(json.dumps({"checkout": str(self.root / "source"), "project": self.project}))
        result = subprocess.run(
            [str(python), "-I", str(SCRIPT), "--probe", str(expectations)],
            cwd=self.root,
            env=os.environ | {"PYTHONPATH": str(SCRIPT.parents[2])},
            text=True,
            capture_output=True,
            timeout=30,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PackageNotFoundError", result.stderr)


if __name__ == "__main__":
    unittest.main()
