from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, Producer, Consumer
import os
from threading import Thread
import asyncio

from utils import KafkaTopics, logger

broker_url = os.getenv("BROKER_URL", "localhost:9092") 
num_partitions = int(os.getenv('NUM_PARTITIONS', 1))
replication_factor = int(os.getenv('REPLICATION_FACTOR', 1))

config = {"bootstrap.servers": broker_url}
topics_to_create = [topic.value for topic in KafkaTopics]

### Producer Initialization ###

class AIOProducer:
    def __init__(self, configs, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._producer = Producer(configs)
        self._cancelled = False
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    def _poll_loop(self):
        while not self._cancelled:
            self._producer.poll(0.1)

    def close(self):
        self._cancelled = True
        self._poll_thread.join()
        
    def produce(self, key=None, topic="", value="", on_delivery=None):
        """
        A produce method in which delivery notifications are made available
        via both the returned future and on_delivery callback (if specified).
        """
        result = self._loop.create_future()

        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(
                    result.set_exception, KafkaException(err))
            else:
                self._loop.call_soon_threadsafe(
                    result.set_result, msg)
            if on_delivery:
                self._loop.call_soon_threadsafe(
                    on_delivery, msg)
            # logger.info(f"Sent producer data: {msg}")
        self._producer.produce(topic=topic, value=value, on_delivery=ack, key=key)
        return result

admin = None

### Kafka Initialization ###
if not admin:
    admin = AdminClient(config)
    new_topics = [NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor) for topic in topics_to_create]
    fs = admin.create_topics(new_topics=new_topics, request_timeout=10)

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))
            
    producer = AIOProducer(config)
    
config['group.id'] = "inference-consumer"
inference_consumer = Consumer(config, logger=logger)
inference_consumer.subscribe([KafkaTopics.PROCESSED_DATA.value])