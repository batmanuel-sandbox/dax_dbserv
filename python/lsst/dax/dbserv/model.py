from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.functions import now
Base = declarative_base()


class DriverJob(Base):
    __tablename__ = 'UWSJob'
    __table_args__ = {'mysql_engine': 'InnoDB'}
    id = Column(Integer, primary_key=True)
    job_id = Column(String(128))
    driver_name = Column(String(128))
    user_id = Column(String(128), nullable=True)
    create_time = Column(DateTime, default=now())
