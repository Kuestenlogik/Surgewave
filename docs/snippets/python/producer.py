#!/usr/bin/env python3
"""
gRPC Producer Example - Python
Demonstrates language-independent API
"""

import grpc
import streaming_pb2
import streaming_pb2_grpc
import time

def main():
    print("=== gRPC Producer Example (Python) ===\n")

    # Connect to Surgewave broker
    channel = grpc.insecure_channel('localhost:9093')
    stub = streaming_pb2_grpc.StreamingServiceStub(channel)

    print("Sending messages via gRPC...\n")

    # Send 10 messages
    for i in range(10):
        message = f"Message {i} from Python"
        key = f"python-key-{i}"

        record = streaming_pb2.Record(
            key=key.encode('utf-8'),
            value=message.encode('utf-8'),
            timestamp=int(time.time() * 1000)
        )

        request = streaming_pb2.ProduceRequest(
            topic="python-test-topic",
            partition=-1,  # Auto-assign
            record=record,
            acks_required=True
        )

        try:
            response = stub.Produce(request)

            if response.error_code == streaming_pb2.ErrorCode.NONE:
                print(f"✓ Sent: {message}")
                print(f"  Topic: {response.topic}")
                print(f"  Partition: {response.partition}")
                print(f"  Offset: {response.offset}")
                print()
            else:
                print(f"✗ Error: {response.error_message}")

        except grpc.RpcError as e:
            print(f"✗ RPC Error: {e.details()}")

        time.sleep(0.5)

    print("Done!")
    channel.close()

if __name__ == '__main__':
    main()
