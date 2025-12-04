import connexion
from connexion import NoContent
import logging
import logging.config
import yaml
import json

from pykafka import KafkaClient

with open("/config/analyzer/app_conf.yaml", "r") as f:
    app_conf = yaml.safe_load(f)

with open("/config/analyzer/log_conf.yaml", "r") as f:
    log_conf = yaml.safe_load(f)
    logging.config.dictConfig(log_conf)

logger = logging.getLogger("basicLogger")


# === Kafka helpers ===
def _get_kafka_client_and_topic():
    """Create Kafka client and topic based on app_conf.yaml."""
    kafka_host = f"{app_conf['events']['hostname']}:{app_conf['events']['port']}"
    client = KafkaClient(hosts=kafka_host)
    topic = client.topics[app_conf["events"]["topic"].encode("utf-8")]
    return client, topic


def _read_all_events():
    """
    Read all events from the Kafka topic from the beginning.
    Returns a list of parsed JSON events.
    """
    _, topic = _get_kafka_client_and_topic()

    consumer = topic.get_simple_consumer(
        reset_offset_on_start=True,
        consumer_timeout_ms=1000
    )

    events = []

    logger.info("Reading all events from Kafka...")

    for msg in consumer:
        if msg is None:
            continue
        try:
            evt = json.loads(msg.value.decode("utf-8"))
            events.append(evt)
        except json.JSONDecodeError:
            logger.warning("Invalid JSON in Kafka message")

    logger.info(f"Total events read: {len(events)}")
    return events


def _get_event_by_type_and_index(event_type: str, index: int):
    """
    Return the N-th event (0-based) of the specified type from the Kafka stream.
    """
    if index is None or index < 0:
        return {"message": "index must be a non-negative integer"}, 400

    events = _read_all_events()
    filtered = [e for e in events if e.get("type") == event_type]

    if index >= len(filtered):
        return {"message": f"No {event_type} event at index {index}"}, 404

    evt = filtered[index]
    payload = evt.get("payload", {})
    return payload, 200


# === Route handlers ===
def get_checkin_event(index: int):
    return _get_event_by_type_and_index("checkin", index)


def get_borrowing_event(index: int):
    return _get_event_by_type_and_index("borrowing", index)


def get_stats():
    events = _read_all_events()

    num_checkins = sum(1 for e in events if e.get("type") == "checkin")
    num_borrowings = sum(1 for e in events if e.get("type") == "borrowing")

    body = {
        "num_checkins": num_checkins,
        "num_borrowings": num_borrowings
    }
    return body, 200

app = connexion.FlaskApp(__name__, specification_dir=".")
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)


if __name__ == "__main__":
    logger.info("Starting analyzer service on port 8110")
    app.run(host="0.0.0.0", port=8110)
