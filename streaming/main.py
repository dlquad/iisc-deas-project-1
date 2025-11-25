import asyncio
import threading
import data_producer
import spark_consumer
import inference_consumer

def run_spark_consumer():
    spark_consumer.start_consumer()

def run_inference_consumer():
    inference_consumer.start_consumer()

def run_producer():
    data_producer.start_producer()

async def main():
    """Start both producer and consumer asynchronously."""
    spark_consumer_thread = threading.Thread(target=run_spark_consumer, daemon=True)
    inference_consumer_thread = threading.Thread(target=run_inference_consumer, daemon=True)
    producer_thread = threading.Thread(target=run_producer, daemon=True)
    
    spark_consumer_thread.start()
    inference_consumer_thread.start()
    producer_thread.start()
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")

if __name__ == "__main__":
    asyncio.run(main())