from sqlalchemy import Column, BigInteger, Text, DateTime
from sqlalchemy.sql import func
from app.utils.cs_database import Base

class Application(Base):
    """
    Application database model
    """
    __tablename__ = 'applications'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(Text, nullable=False, unique=True)
    description = Column(Text, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
