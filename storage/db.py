from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml

with open('/config/storage/app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f)

user = app_config["datastore"]["user"]
password = app_config["datastore"]["password"]
hostname = app_config["datastore"]["hostname"]
port = app_config["datastore"]["port"]
db = app_config["datastore"]["db"]

db_url = f"mysql+pymysql://{user}:{password}@{hostname}:{port}/{db}"
ENGINE = create_engine(db_url, pool_pre_ping=True)

def make_session():
    return sessionmaker(bind=ENGINE)()

