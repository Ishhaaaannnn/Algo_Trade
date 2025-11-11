import time
import pandas as pd
from datetime import date, timedelta, datetime
from data.data_fetcher import fetch_nifty_data
from strategy.ema_crossover import EMACrossoverStrategy
from logger_db import log_trade

class PaperTrader:
    def __init__(self, symbol: str='NIFTY 50', initial_capital:float = 100000, short_window: int=10, long_window: int=20, max_risk_pts: float=40.0):
        self.symbol = symbol
        self.initial_capital = initial_capital
        self.position = 0
        self.strategy = EMACrossoverStrategy(short_window, long_window, max_risk_pts)
        self.interval = 300

    def tick(self):
        end = date.today()
        start = end - timedelta(days=7)
        data = fetch_nifty_data(start=start, end=end)

        signals = self.strategy.generate_signals(data)
        latest = signals.iloc[-1] if not signals.empty else None
        signal = latest['Signal'] if latest is not None else 0
        price = latest['Close'] if latest is not None else None
        sl_price = latest['SL_Price'] if latest is not None else None
        risk_points = latest['Calculated_Risk'] if latest is not None else None

        if signal == 1.0 and self.position == 0:
            quantity = int(self.initial_capital / price)
            self.position = quantity
            self.initial_capital -= quantity * price
            log_trade(
                symbol=self.symbol,
                side='BUY',
                entry_price=price,
                risk_points=risk_points,
                stop_loss=sl_price,
                target_price=latest['Target_Price'],
                quantity=quantity,
                balance=self.initial_capital,
                remarks='Entered long position'
            )
            print(f"[{datetime.now()}] Bought {quantity} of {self.symbol} at {price}, SL: {sl_price}, Risk: {risk_points} pts")

        elif signal == -1 and self.position > 0:
            proceeds = self.position * price
            quantity = self.position
            self.initial_capital += proceeds
            self.position = 0
            log_trade(
                symbol=self.symbol,
                side='SELL',
                entry_price=price,
                risk_points=0,
                stop_loss=0,
                target_price=0,
                quantity=quantity,
                balance=self.initial_capital,
                remarks='Exited long position'
            )
            print(f"[{datetime.now()}] Sold {quantity} of {self.symbol} at {price}, Proceeds: {proceeds}")
        else:
            pass

    def start_trading(self):
        try:
            while True:
                self.tick()
                time.sleep(self.interval)
        except KeyboardInterrupt:
            print("Paper trading stopped by user.")