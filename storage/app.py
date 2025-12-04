import connexion
import logging, logging.config, yaml, json, os
import functools
from datetime import datetime, timezone, timedelta
from threading import Thread
from pykafka import KafkaClient
from pykafka.common import OffsetType
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from db import make_session
from models import CheckIn, Borrowing

with open('/config/storage/app_conf.yaml', 'r') as f:
    app_conf = yaml.safe_load(f)

with open('/config/storage/log_conf.yaml', 'r') as f:
    log_conf = yaml.safe_load(f)
    logging.config.dictConfig(log_conf)

logger = logging.getLogger('basicLogger')

def parse_iso8601(s: str):
    """
    ISO-8601 문자열(예: 2025-11-27T00:00:00Z)을 datetime으로 파싱.
    - 끝이 'Z'면 UTC(+00:00)으로 처리
    - 공백/인코딩 등 약간 어긋난 것들도 최대한 처리
    """
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return datetime.fromtimestamp(s, tz=timezone.utc)

    s = str(s).strip()
    if not s:
        return None

    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    elif "+" not in s[10:] and "-" not in s[10:]:
        s = s + "+00:00"

    try:
        return datetime.fromisoformat(s)
    except Exception as e:
        logger.error(f"Failed to parse ISO8601 timestamp '{s}': {e}")
        raise


def use_db_session(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        session = make_session(
            # user=app_conf['datastore']['user'],
            # password=app_conf['datastore']['password'],
            # host=app_conf['datastore']['hostname'],
            # port=app_conf['datastore']['port'],
            # db=app_conf['datastore']['db']
        )
        try:
            return func(session, *args, **kwargs)
        finally:
            session.close()
    return wrapper

def process_messages():
    kafka_host = f"{app_conf['events']['hostname']}:{app_conf['events']['port']}"
    client = KafkaClient(hosts=kafka_host)
    topic = client.topics[app_conf['events']['topic'].encode('utf-8')]

    consumer = topic.get_simple_consumer(
        consumer_group=b'event_group',
        reset_offset_on_start=False,
        auto_offset_reset=OffsetType.LATEST
    )

    logger.info("Kafka consumer thread started, waiting for messages...")

    for msg in consumer:
        if msg is None:
            continue

        msg_str = msg.value.decode('utf-8')
        evt = json.loads(msg_str)
        logger.debug(f"Message received from Kafka: {evt}")

        evt_type = evt["type"]
        payload  = dict(evt["payload"]) 

        if evt_type == "checkin":
            payload["batch_timestamp"] = parse_iso8601(payload["batch_timestamp"])
            payload["interval_start"]  = parse_iso8601(payload["interval_start"])
            payload["interval_end"]    = parse_iso8601(payload["interval_end"])
        elif evt_type == "borrowing":
            payload["batch_timestamp"] = parse_iso8601(payload["batch_timestamp"])
            payload["borrowed_at"]     = parse_iso8601(payload["borrowed_at"])
            payload["returned_at"]     = parse_iso8601(payload.get("returned_at"))

        session = make_session(
            # user=app_conf['datastore']['user'],
            # password=app_conf['datastore']['password'],
            # host=app_conf['datastore']['hostname'],
            # port=app_conf['datastore']['port'],
            # db=app_conf['datastore']['db']
        )
        try:
            if evt_type == "checkin":
                row = CheckIn(**payload)
            else:
                row = Borrowing(**payload)

            session.add(row)
            session.commit()
            logger.info(f"Stored {evt_type} event with trace_id={payload.get('trace_id')}")
        except IntegrityError:
            session.rollback()
            logger.warning("IntegrityError on insert, skipping duplicate/invalid event")
        finally:
            session.close()

        consumer.commit_offsets()

def start_consumer_thread():
    t = Thread(target=process_messages)
    t.daemon = True
    t.start()
    logger.info("Background Kafka consumer thread launched")

@use_db_session
def get_checkins(session, start_timestamp=None, end_timestamp=None):
    logger.info(f"GET /library/check-ins with start={start_timestamp}, end={end_timestamp}")

    if not start_timestamp and not end_timestamp:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=1)
        logger.info(f"No timestamps provided, using default window start={start}, end={end}")
    else:
        try:
            start = parse_iso8601(start_timestamp)
            end   = parse_iso8601(end_timestamp)
        except Exception:
            return {"message": "Invalid timestamp format. Use ISO-8601 like '2025-11-27T00:00:00Z'."}, 400

        if start is None or end is None:
            return {"message": "Both 'start_timestamp' and 'end_timestamp' are required."}, 400

    if end <= start:
        logger.warning(f"Invalid time window: start={start}, end={end}")
        return {"message": "'end_timestamp' must be later than 'start_timestamp'."}, 400

    stmt = (
        select(CheckIn)
        .where(CheckIn.date_created >= start)
        .where(CheckIn.date_created < end)
        .order_by(CheckIn.date_created.asc())
    )

    try:
        rows = session.execute(stmt).scalars().all()
    except Exception as e:
        logger.exception(f"DB error while querying check-ins: {e}")
        return {"message": "Internal error while querying the database."}, 500

    logger.info(f"Query checkins: found {len(rows)} between {start} and {end}")
    return [r.to_dict() for r in rows], 200

@use_db_session
def get_borrowings(session, start_timestamp=None, end_timestamp=None):
    logger.info(f"GET /library/borrowings with start={start_timestamp}, end={end_timestamp}")

    if not start_timestamp and not end_timestamp:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=1)
        logger.info(f"No timestamps provided, using default window start={start}, end={end}")
    else:
        try:
            start = parse_iso8601(start_timestamp)
            end   = parse_iso8601(end_timestamp)
        except Exception:
            return {"message": "Invalid timestamp format. Use ISO-8601 like '2025-11-27T00:00:00Z'."}, 400

        if start is None or end is None:
            return {"message": "Both 'start_timestamp' and 'end_timestamp' are required."}, 400

    if end <= start:
        logger.warning(f"Invalid time window: start={start}, end={end}")
        return {"message": "'end_timestamp' must be later than 'start_timestamp'."}, 400

    stmt = (
        select(Borrowing)
        .where(Borrowing.date_created >= start)
        .where(Borrowing.date_created < end)
        .order_by(Borrowing.date_created.asc())
    )

    try:
        rows = session.execute(stmt).scalars().all()
    except Exception as e:
        logger.exception(f"DB error while querying borrowings: {e}")
        return {"message": 'Internal error while querying the database.'}, 500

    logger.info(f"Query borrowings: found {len(rows)} between {start} and {end}")
    return [r.to_dict() for r in rows], 200

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=False)

if __name__ == "__main__":
    start_consumer_thread()
    logger.info("Starting storage service on port 8090 (Kafka consumer mode)")
    app.run(port=8090, host="0.0.0.0")
