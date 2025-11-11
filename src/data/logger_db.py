from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime
from sqlalchemy.sql import insert

from datetime import datetime

ENGINE = create_engine('sqlite:///trades.db', echo=False)
metadata = MetaData()

trades = Table(
    'trades', metadata,
    Column('id', Integer, primary_key=True),
    Column('timestamp', DateTime),
    Column('symbol', String),
    Column('side', String),
    Column('entry_price', Float),
    Column('risk_points', Float),
    Column('stop_loss', Float),
    Column('target_price', Float),
    Column('quanity', Integer),
    Column('balance', Float),
    Column('remarks', String)
)

metadata.create_all(ENGINE)

def log_trade(symbol, side, entry_price, risk_points, stop_loss, target_price, quantity, balance, remarks=''):
    ins = insert(trades).values(
        timestamp=datetime.now(),
        symbol=symbol,
        side=side,
        entry_price=entry_price,
        risk_points=risk_points,
        stop_loss=stop_loss,
        target_price=target_price,
        quanity=quantity,
        balance=balance,
        remarks=remarks
    )
    conn = ENGINE.connect()
    conn.execute(ins)
    conn.close()