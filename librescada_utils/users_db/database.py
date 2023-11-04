import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
import logging
from contextlib import contextmanager
from librescada_utils.logger import logger
import models

# SQLALCHEMY_DATABASE_URL = "sqlite:///./users_db/users.db"
# SQLALCHEMY_DATABASE_URL = "postgresql://user:password@postgresserver/db"
DEFAULT_DB_PATH = os.getcwd() + "/users_db/users.db"

# Make sure SQLALCHEMY_DATABASE_URL is defined before importing this module trough an environment variable
SQLALCHEMY_DATABASE_URL = os.getenv("USERSDB_DATABASE_URL", f"sqlite:///{DEFAULT_DB_PATH}")

logger.info(f"Using database at {SQLALCHEMY_DATABASE_URL}")

engine = sqlalchemy.create_engine( SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False} )
# Initialize the database
Base = declarative_base()

Base.metadata.create_all(bind=engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def get_db():
    """
    This function returns a database session that can be used to interact with the database.
    The function is used as a context manager, so it can be used as follows:
    ```
    with get_db() as db:
        # Do something with the database
        db.query(...)
    ```
    This ensures that the database session is closed after the operations are performed. And if
    an exception is raised, the session is also closed with the `finally` statement.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
