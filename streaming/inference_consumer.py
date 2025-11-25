import json
from confluent_kafka import KafkaException

from utils import logger, KafkaTopics, infer_text_label 
from kafka_manager import inference_consumer, producer

CONFIDENCE_THRESHOLD = 0.8

def process_message(msg):
    logger.info("\n")
    logger.info(f'{msg.topic()} [{msg.partition()}] at offset {msg.offset()} with key {str(msg.key())}:')
    
    try:
        data = json.loads(msg.value().decode('utf-8'))
        
        label, confidence = infer_text_label(data.get("text", ""))
        data["label"] = label
        data["confidence"] = confidence
        
        if confidence < CONFIDENCE_THRESHOLD:
            producer.produce(topic=KafkaTopics.VERIFY_DATA.value, value=json.dumps(data).encode('utf-8'))
        else:
            producer.produce(topic=KafkaTopics.LABELLED_DATA.value, value=json.dumps(data).encode('utf-8'))
    except Exception as e:
        logger.error(f"Error decoding message: {str(e)}")
        return

def start_consumer():
    try:
        while True:
            try:
                msg = inference_consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                else:
                    process_message(msg)

            except KafkaException as kafka_err:
                logger.error(f"Kafka error: {str(kafka_err)}")
                continue
            except Exception as e:
                logger.error(f"Consumer loop error occurred: {str(e)}")
                continue
                
    except KeyboardInterrupt:
        logger.info('%% Aborted by user\n')
    finally:
        if inference_consumer:
            inference_consumer.close()