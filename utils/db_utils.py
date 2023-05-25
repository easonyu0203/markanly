import os

import dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

dotenv.load_dotenv()
connect_str = os.getenv("CONNECT_STR")
assert connect_str, f"please set CONNECT_STR in .env file."

engine = create_engine(connect_str)
Session = sessionmaker(bind=engine)


def create_db_session() -> Session:
    """Create a SQLAlchemy session."""
    global Session
    session = Session()
    return session
