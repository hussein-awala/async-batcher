# Async Batcher with Keras model and GRPC

This example shows how to use the `async_batcher` library with GRPC to process data
in batches by a Keras (Tensorflow) model.

In this example, we serve a TensorFlow model using GRPC, you can find the server implementation in the
[server.py](server.py) file and the client implementation in the [client.py](client.py) file.

If you want to update the server implementation, you can edit the protobuf file
[predictor.proto](protos/predictor.proto) and regenerate the server and client code using the
[run_codegen.py](run_codegen.py) script:
```bash
# Install the required packages
pip install -r requirements.txt

# Run the code generation script
python run_codegen.py
```

## How to use
Install the required packages:
```bash
pip install -r requirements.txt
```
Then, you can run the GRPC server:
```bash
python server.py
```
In another terminal, you can run the client to make requests to the server (1000 simultaneous requests):
```bash
python client.py
```

**Note**  
To show how simple it is to integrate the batcher into existing code, I created this example based on examples
provided the official GRPC repository:
- [async_greeter_server_with_graceful_shutdown.py](https://github.com/grpc/grpc/blob/master/examples/python/helloworld/async_greeter_server_with_graceful_shutdown.py)
- [async_greeter_client.py](https://github.com/grpc/grpc/blob/master/examples/python/helloworld/async_greeter_client.py)
