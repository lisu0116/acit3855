import connexion
from connexion import NoContent
import json, os, logging, logging.config, yaml
from datetime import datetime, timezone
from apscheduler.schedulers.background import BackgroundScheduler
import httpx

with open('/config/processing/log_conf.yaml', 'r') as f:
    logging.config.dictConfig(yaml.safe_load(f))
logger = logging.getLogger('basicLogger')

with open('/config/processing/app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f)

DATA_FILE   = app_config['datastore']['filename']
CHECKINS_URL   = app_config['eventstores']['checkins_url']
BORROWINGS_URL = app_config['eventstores']['borrowings_url']
INTERVAL_SEC   = app_config['scheduler']['interval']

def _now_iso():
    return datetime.now(timezone.utc).replace(tzinfo=timezone.utc).isoformat().replace('+00:00','Z')

def _load_stats():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f:
            return json.load(f)
    return {
        "total_checkins": 0,
        "total_borrowings": 0,
        "max_entry_count": 0,
        "avg_borrow_duration_days": 0.0,
        "avg_borrow_duration_count": 0,  
        "last_updated": "1970-01-01T00:00:00Z"
    }

def _save_stats(stats):
    with open(DATA_FILE, 'w') as f:
        json.dump(stats, f)

def _fetch(url, start_iso, end_iso):
    r = httpx.get(url, params={"start_timestamp": start_iso, "end_timestamp": end_iso}, timeout=10)
    return r.status_code, (r.json() if r.status_code == 200 else None)

def populate_stats():
    logger.info("Periodic processing started")
    stats = _load_stats()

    start_iso = stats["last_updated"]  
    end_iso   = _now_iso()             
    ok1, data1 = _fetch(CHECKINS_URL, start_iso, end_iso)
    ok2, data2 = _fetch(BORROWINGS_URL, start_iso, end_iso)

    if ok1 != 200 or ok2 != 200:
        logger.error(f"Storage GET failed: checkins={ok1}, borrowings={ok2}")
        logger.info("Periodic processing ended (errors)")
        return

    logger.info(f"Received new events: checkins={len(data1)}, borrowings={len(data2)}")

    stats["total_checkins"]   += len(data1)
    stats["total_borrowings"] += len(data2)

    for ev in data1:
        if ev["entry_count"] > stats["max_entry_count"]:
            stats["max_entry_count"] = ev["entry_count"]

    n0   = stats.get("avg_borrow_duration_count", 0)
    avg0 = stats.get("avg_borrow_duration_days", 0.0)
    for ev in data2:
        dur = float(ev["borrow_duration_days"])
        avg0 = (avg0 * n0 + dur) / (n0 + 1)
        n0  += 1
    stats["avg_borrow_duration_days"] = avg0
    stats["avg_borrow_duration_count"] = n0

    latest_times = []
    if data1:
        latest_times += [ev["batch_timestamp"] for ev in data1]
    if data2:
        latest_times += [ev["batch_timestamp"] for ev in data2]
    stats["last_updated"] = max(latest_times) if latest_times else end_iso

    _save_stats(stats)
    logger.debug(f"Updated stats: {stats}")
    logger.info("Periodic processing ended")

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 'interval', seconds=INTERVAL_SEC)
    sched.start()

def get_stats():
    logger.info("GET /stats request received")
    if not os.path.exists(DATA_FILE):
        logger.error("Statistics do not exist")
        return {"message": "Statistics do not exist"}, 404
    with open(DATA_FILE, 'r') as f:
        data = json.load(f)

    data.pop("avg_borrow_duration_count", None)
    logger.debug(f"Stats response: {data}")
    logger.info("GET /stats request completed")
    return data, 200

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    init_scheduler()
    app.run(host="0.0.0.0", port=8100)
