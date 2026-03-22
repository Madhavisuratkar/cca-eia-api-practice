# app/models/policy_engine.py
from sqlalchemy import Column, BigInteger, String, Text, DateTime, Numeric, UniqueConstraint
from sqlalchemy.sql import func
from app.utils.cs_database import Base


class PolicyEngine(Base):
    """
    Policy Engine database model
    """
    __tablename__ = 'policy_engine'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_email = Column(Text, nullable=False)
    provider = Column(Text, nullable=False)
    instance_type = Column(Text, nullable=False)
    scalar_value = Column(Numeric(10, 4), nullable=False)
    policy_name = Column(Text, nullable=False)
    policy_type = Column(Text, nullable=False)

    # Timestamps from DB server for consistency across clients (timestamptz) 
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}  # simple serialization pattern [web:25]
