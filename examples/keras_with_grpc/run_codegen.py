from __future__ import annotations

from grpc_tools import protoc

if __name__ == "__main__":
    protoc.main(
        (
            "",
            "-I./protos",
            "--python_out=.",
            "--grpc_python_out=.",
            "--pyi_out=.",
            "./protos/predictor.proto",
        )
    )
