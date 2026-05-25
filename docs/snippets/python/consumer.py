#!/usr/bin/env python3
"""
gRPC Consumer Example - Python
Demonstrates streaming consumption
"""

import grpc
import streaming_pb2
import streaming_pb2_grpc
from datetime import datetime

def main():
    print("=== gRPC Consumer Example (Python) ===\n")

    # Connect to Surgewave broker
    channel = grpc.insecure_channel('localhost:9093')
    stub = streaming_pb2_grpc.StreamingServiceStub(channel)

    print("Consuming messages via gRPC streaming...\n")

    request = streaming_pb2.ConsumeRequest(
        topic="python-test-topic",
        partition=0,
        offset=-2,  # Start from earliest (-2) or latest (-1)
        max_records=10,
        max_wait_ms=1000
    )

    message_count = 0

    try:
        # Server-side streaming
        for response in stub.Consume(request):
            if response.error_code != streaming_pb2.ErrorCode.NONE:
                print(f"Error: {response.error_message}")
                break

            for record in response.records:
                key = record.key.decode('utf-8')
                value = record.value.decode('utf-8')
                timestamp = datetime.fromtimestamp(record.timestamp / 1000.0)

                message_count += 1

                print(f"[{message_count}] Received:")
                print(f"  Key: {key}")
                print(f"  Value: {value}")
                print(f"  Offset: {record.offset}")
                print(f"  Timestamp: {timestamp}")
                print()

            # Stop after 10 messages for demo
            if message_count >= 10:
                break

    except grpc.RpcError as e:
        print(f"RPC Error: {e.details()}")
    except KeyboardInterrupt:
        print("\nStopped by user")

    print(f"Total messages consumed: {message_count}")
    channel.close()

if __name__ == '__main__':
    main()
