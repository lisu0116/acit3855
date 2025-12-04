import connexion
from connexion import NoContent
import yaml, logging, logging.config
from pykafka import KafkaClient
import json, datetime, os

with open('/config/receiver/app_conf.yaml', 'r') as f:
    app_conf = yaml.safe_load(f)

with open('/config/receiver/log_conf.yaml', 'r') as f:
    log_conf = yaml.safe_load(f)
    logging.config.dictConfig(log_conf)

logger = logging.getLogger('basicLogger')

kafka_host = f"{app_conf['events']['hostname']}:{app_conf['events']['port']}"
client = KafkaClient(hosts=kafka_host)
topic = client.topics[app_conf['events']['topic'].encode('utf-8')]
producer = topic.get_sync_producer()


def _publish_event(body: dict, event_type: str):
    msg = {
        "type": event_type,
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": body
    }
    msg_str = json.dumps(msg)
    logger.debug(f"Producing message to Kafka: {msg_str}")
    producer.produce(msg_str.encode('utf-8'))


def receive_checkin(body):
    _publish_event(body, "checkin")
    return NoContent, 201


def receive_borrowing(body):
    _publish_event(body, "borrowing")
    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=False)

if __name__ == "__main__":
    logger.info("Starting receiver service on port 8080 (Kafka producer mode)")
    app.run(host= "0.0.0.0", port=8080)
