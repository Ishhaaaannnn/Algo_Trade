import time
import pandas as pd
from datetime import date, timedelta, datetime
from src.data.data_fetcher import fetch_nifty_data
from src.strategy.ema_crossover import EMACrossoverStrategy
from src.data.logger_db import log_trade_entry, log_trade_exit

def get_itm_strike(spot_price, strike_gap=100, option_type='CE'):
    if spot_price is None:
        return None
    atm = int(round(spot_price / strike_gap) * strike_gap)
    if option_type.upper() == 'CE':
        return atm if atm < spot_price else max(atm - strike_gap, strike_gap)
    elif option_type.upper() == 'PE':
        return atm if atm > spot_price else atm + strike_gap
    else:
        return atm

class PaperTrader:
    def __init__(self, symbol='NIFTY 50', initial_capital=100000, short_window=10, long_window=20, max_risk_pts=40.0, mode='backtest'):
        self.symbol = symbol
        self.initial_capital = initial_capital
        self.available_balance = initial_capital
        self.position = 0
        self.entry_price = None
        self.entry_timestamp = None
        self.strategy = EMACrossoverStrategy(short_window, long_window, max_risk_pts)
        self.interval = 300
        self.open_trade_id = None
        self.lot_size = 75
        self.option_multiplier = 0.008
        self.max_lots = 4
        self.mode = mode.lower()
        end = date.today()
        start = end - timedelta(days=30)
        self.data = fetch_nifty_data(start=start, end=end)

    def simulate_option_price(self, spot_price):
        return round(spot_price * self.option_multiplier, 2)

    def calculate_delta(self):
        return 0.75

    def calculate_theta(self, days_to_expiry):
        return -1 / days_to_expiry if days_to_expiry > 0 else -1

    def calculate_time_decay(self, option_price, days_elapsed, days_to_expiry):
        theta = self.calculate_theta(days_to_expiry)
        return round(option_price + (theta * days_elapsed), 2)

    def execute_trade(self, latest):
        signal = latest['Signal']
        price = latest['Entry_Price']
        sl_price = latest['SL_Price']
        risk_points = latest['Calculated_Risk']
        option_type = 'CE' if signal == 1.0 else 'PE'
        strike = get_itm_strike(price, strike_gap=100, option_type=option_type)
        option_price = self.simulate_option_price(price)

        if signal == 1.0 and self.position == 0:
            lot_cost = option_price * self.lot_size
            affordable_lots = min(int(self.available_balance // lot_cost), self.max_lots)
            if affordable_lots <= 0:
                return
            quantity = affordable_lots * self.lot_size
            capital_used = quantity * option_price
            self.available_balance -= capital_used
            self.position = quantity
            self.entry_price = option_price
            self.entry_timestamp = datetime.now()
            delta = self.calculate_delta()
            theta = self.calculate_theta(6)
            self.open_trade_id = log_trade_entry(symbol=f'NIFTY{strike}{option_type}', side='BUY', entry_price=option_price, quantity=quantity, capital_used=capital_used, balance=self.available_balance, remarks='Opened Long Position')
            print(f'[{datetime.now()}] BUY {affordable_lots} lot(s) NIFTY{strike}{option_type} @ {option_price}, Capital Used: {capital_used:.2f}, Remaining: {self.available_balance:.2f}, Δ={delta:.2f}, Θ={theta:.4f}')

        elif signal == -1.0 and self.position > 0:
            exit_price = self.simulate_option_price(price)
            proceeds = exit_price * self.position
            pnl = (exit_price - self.entry_price) * self.position
            self.available_balance += proceeds
            days_elapsed = (datetime.now() - self.entry_timestamp).days or 1
            decayed_price = self.calculate_time_decay(exit_price, days_elapsed, 6)
            log_trade_exit(trade_id=self.open_trade_id, exit_price=exit_price, pnl=pnl, remarks='Closed Long Position')
            print(f'[{datetime.now()}] SELL {self.position // self.lot_size} lot(s) NIFTY{strike}{option_type} @ {exit_price}, P&L: {pnl:.2f}, Balance: {self.available_balance:.2f} (After Theta {decayed_price:.2f})')
            self.position = 0
            self.entry_price = None
            self.entry_timestamp = None
            self.open_trade_id = None

    def start_trading(self):
        print(f'[INIT] Paper trader started in {self.mode.upper()} mode with capital = {self.available_balance}')
        if self.mode == 'backtest':
            print('[MODE] Backtest - simulating historical data')
            signals = self.strategy.generate_signals(self.data)

            print("--- SIGNALS GENERATED (Showing first/last 5) ---")
            print(signals.head())
            print(signals.tail())
            print("-----------------------------------------------")

            for idx, row in signals.iterrows():
                signal = row['Signal']
                if signal == 1.0 and self.position == 0:
                    self.execute_trade(row)
                elif signal == -1.0 and self.position > 0:
                    self.execute_trade(row)
            print('[BACKTEST COMPLETE]')
            print(f'Final Balance: {self.available_balance:.2f}')
        elif self.mode == 'live':
            print('[MODE] Live - waiting for new 5-minute candles...')
            try:
                while True:
                    self.tick()
                    time.sleep(self.interval)
            except KeyboardInterrupt:
                print('Stopped by user.')


if __name__ == '__main__':
    trader = PaperTrader(symbol='NIFTY 50', initial_capital=100000, mode='backtest')
    trader.start_trading()
