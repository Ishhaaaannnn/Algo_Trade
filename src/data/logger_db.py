from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, update
from sqlalchemy.sql import insert

from datetime import datetime

ENGINE = create_engine('sqlite:///trades.db', echo=False)
metadata = MetaData()

trades = Table(
    'trades', metadata,
    Column('id', Integer, primary_key=True),
    Column('entry_timestamp', DateTime),
    Column('exit_timestamp', DateTime, nullable=True),
    Column('symbol', String),
    Column('side', String),
    Column('entry_price', Float),
    Column('exit_price', Float, nullable=True),
    Column('risk_points', Float),
    Column('stop_loss', Float),
    Column('target_price', Float),
    Column('quantity', Integer),
    Column('entry_balance', Float),
    Column('exit_balance', Float, nullable=True),
    Column('pnl', Float, nullable=True),
    Column('status', String),  # OPEN, CLOSED, STOPPED_OUT, TARGET_HIT
    Column('remarks', String)
)

metadata.create_all(ENGINE)

def log_trade_entry(symbol, side, entry_price, risk_points, stop_loss, target_price, quantity, balance, remarks=''):
    """Log a new trade entry (BUY signal)"""
    ins = insert(trades).values(
        entry_timestamp=datetime.now(),
        symbol=symbol,
        side=side,
        entry_price=entry_price,
        risk_points=risk_points,
        stop_loss=stop_loss,
        target_price=target_price,
        quantity=quantity,
        entry_balance=balance,
        status='OPEN',
        remarks=remarks
    )
    conn = ENGINE.connect()
    result = conn.execute(ins)
    conn.commit()
    trade_id = result.lastrowid
    conn.close()
    print(f"[LOG] Trade OPENED (ID:{trade_id}): {side} {quantity} {symbol} @ {entry_price}, SL:{stop_loss}, TP:{target_price}")
    return trade_id

def log_trade_exit(trade_id, exit_price, exit_balance, exit_status='CLOSED', remarks=''):
    """Log trade exit (SELL signal or stop loss hit)"""
    upd = update(trades).where(
        trades.c.id == trade_id
    ).values(
        exit_timestamp=datetime.now(),
        exit_price=exit_price,
        exit_balance=exit_balance,
        status=exit_status,
        remarks=remarks
    )
    conn = ENGINE.connect()
    conn.execute(upd)
    conn.commit()
    
    # Get entry details to calculate P&L
    select_stmt = trades.select().where(trades.c.id == trade_id)
    result = conn.execute(select_stmt)
    trade = result.fetchone()
    
    if trade:
        pnl = (exit_price - trade.entry_price) * trade.quantity
        # Update with P&L
        upd_pnl = update(trades).where(
            trades.c.id == trade_id
        ).values(pnl=pnl)
        conn.execute(upd_pnl)
        conn.commit()
        
        print(f"[LOG] Trade CLOSED (ID:{trade_id}): EXIT @ {exit_price}, P&L: {pnl:.2f}, Status: {exit_status}")
    
    conn.close()

def log_trade(symbol, side, entry_price, risk_points, stop_loss, target_price, quantity, balance, remarks=''):
    """Legacy function for backward compatibility - logs as complete trade"""
    log_trade_entry(symbol, side, entry_price, risk_points, stop_loss, target_price, quantity, balance, remarks)