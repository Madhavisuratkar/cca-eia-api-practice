
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.connections.env_config import cs_db_host, cs_db_password, cs_db_port, cs_database, cs_db_user
from app.utils.constants import DB_PREFIX
from sqlalchemy.exc import SQLAlchemyError

Base = declarative_base()

def create_db_url():
    """
    Return the database URL.
    """
    return f'{DB_PREFIX}://{cs_db_user}:{cs_db_password}@{cs_db_host}:{cs_db_port}/{cs_database}'

def init_db():
    """Initialize the database."""
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def check_db_connection():
    try:
        db = next(get_db())
        # Run a simple query via the session's connection
        db.execute(text("SELECT 1"))
        db.commit()  # optional, SELECT doesn't require commit usually
        db.close()
        return True
    except SQLAlchemyError as e:
        print(str(e))
        return False

# Synchronous DB session
engine = create_engine(create_db_url())
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)



