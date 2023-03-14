from sqlalchemy import Column, String, Float, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Wages_Cpi(Base):
    __tablename__ = "wages_cpi_table"
    index = Column(Integer, primary_key=True)
    quarter = Column(String)
    _quarter = Column(String)
    cpi = Column(Float)
    wages_increase_percent = Column(Float)
    wages = Column(Float)

class Wages(Base):
    __tablename__ = "wages_table"
    index = Column(Integer, primary_key=True)
    date = Column(String)
    CIU2020000000000A = Column(Float)

class Cpi(Base):
    __tablename__ = "cpi_table"
    index = Column(Integer, primary_key=True)
    date = Column(String)
    CUUR0000SA0 = Column(Float)