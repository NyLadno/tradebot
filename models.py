from sqlalchemy.orm import declarative_base, DeclarativeBase
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import (
    Column, Integer, String, Date, Float, Boolean,
    DateTime, CheckConstraint
)
from sqlalchemy.sql import func
from os import getenv
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = getenv('DB_IRL')
engine = create_async_engine(DATABASE_URL)
new_session = async_sessionmaker(engine, expire_on_commit=False)

async def get_session():
    async with new_session() as session:
        yield session

class Base(DeclarativeBase):
    pass


class News(Base):
    __tablename__ = 'news'

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False, index=True)
    url = Column(String, unique=True, nullable=False)
    title = Column(String)
    mood = Column(String)  # или Enum
    influence = Column(Float)
    language = Column(String(2))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        CheckConstraint("language IN ('ru', 'en', 'de')", name='check_language'),
    )


class TradingDay(Base):
    __tablename__ = 'trading_days'

    id = Column(Integer, primary_key=True)
    date = Column(Date, unique=True, nullable=False)
    is_trading = Column(Boolean, default=True)
    note = Column(String)  # 'holiday', 'weekend', etc.


class TradingHalt(Base):
    __tablename__ = 'trading_halts'

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    start_time = Column(DateTime(timezone=True), nullable=False)
    end_time = Column(DateTime(timezone=True))
    reason = Column(String)


class Trade(Base):
    __tablename__ = 'trades'

    id = Column(Integer, primary_key=True)
    pair = Column(String, nullable=False)
    direction = Column(String, nullable=False)

    entry_time = Column(DateTime(timezone=True), nullable=False)
    exit_time = Column(DateTime(timezone=True))

    entry_price = Column(Float, nullable=False)
    exit_price = Column(Float)

    z_score_entry = Column(Float)
    z_score_exit = Column(Float)

    pnl_rub = Column(Float)
    pnl_percent = Column(Float)  # добавь для наглядности

    reason = Column(String)
    mode = Column(String)  # 'backtest', 'paper', 'live'

    created_at = Column(DateTime(timezone=True), server_default=func.now())

async def setup_database():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

