# db_setup.py
from sqlalchemy import create_engine
from models import Base

engine = create_engine('sqlite:///dividends.db')
Base.metadata.create_all(engine)

