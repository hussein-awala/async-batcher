# Keras inference with gRPC

[server.py](server.py) serves the bundled HDF5 model using `grpc.aio` and a batcher.
[client.py](client.py) sends 1,000 concurrent requests to `localhost:50051`, each with ten random
features, and prints one prediction per request. The demonstration uses an insecure local channel.
On shutdown, the server stops accepting RPCs and waits up to 30 seconds for handlers before
stopping the batcher.

Use Python 3.12 on a platform with a TensorFlow wheel. The requirements select TensorFlow 2.21+
and Keras 3.15.1+; TensorFlow 2.21's [published wheels](https://pypi.org/project/tensorflow/2.21.0/#files)
do not include Python 3.14/3.15 or Intel macOS. The core library supports those Python versions
independently. See the [Keras guide](../../docs/machine-learning.md#keras) for JAX usage.

## Install and run

From the repository root:

```bash
uv venv --python 3.12 examples/keras_with_grpc/.venv
uv pip install --python examples/keras_with_grpc/.venv/bin/python -e '.[keras]' -r examples/keras_with_grpc/requirements.txt
cd examples/keras_with_grpc
KERAS_BACKEND=tensorflow .venv/bin/python server.py
```

In a second terminal, from `examples/keras_with_grpc`:

```bash
.venv/bin/python client.py
```

Stop the server with Ctrl+C. Its model path is resolved relative to `server.py`; inference loads
with `compile=False` rather than restoring training configuration.

The example uses its own environment and requirements, separate from `uv.lock`. Invoke that
interpreter directly to retain the additional packages. On Windows, use `.venv/Scripts/python.exe`
and set `KERAS_BACKEND=tensorflow` before starting Python.

## Regenerate protocol bindings

Edit [predictor.proto](protos/predictor.proto), then run from `examples/keras_with_grpc`:

```bash
.venv/bin/python run_codegen.py
```

`grpcio-tools` is installed by the example requirements. The script writes `predictor_pb2.py`,
`predictor_pb2.pyi`, and `predictor_pb2_grpc.py` in the current directory. Review generated changes
and test the server/client together after changing the protocol.

The example was adapted from the official gRPC
[graceful shutdown server](https://github.com/grpc/grpc/blob/master/examples/python/helloworld/async_greeter_server_with_graceful_shutdown.py)
and [async client](https://github.com/grpc/grpc/blob/master/examples/python/helloworld/async_greeter_client.py).
