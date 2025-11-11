import pandas as pd
import numpy as np
from typing import Tuple

class Backtester:
    def __init__(self, initial_capital = 100000, fees_per_trade = 20):
        self.initial_capital = initial_capital
        self.fees_per_trade = fees_per_trade

    def run_backtest(self, signals: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
        df = signals.copy()
        df['Position'] = df['Signal'].shift(1).fillna(0)
        df['Market_Returns'] = df['Entry_Price'].pct_change().fillna(0)
        df['Strategy_Returns'] = df['Position'] * df['Market_Returns']

        df['Cumulative_Strategy_Returns'] = (1 + df['Strategy_Returns']).cumprod()
        df['Cumulative_Market_Returns'] = (1 + df['Market_Returns']).cumprod()

        total_return = df['Cumulative_Strategy_Returns'].iloc[-1] - 1
        annualized_return = (1 + total_return) ** (252 / len(df)) - 1
        sharpe_ratio = (df['Strategy_Returns'].mean() / df['Strategy_Returns'].std()) * np.sqrt(252)
        
        metrics = {
            'Total Return': total_return,
            'Annualized Return': annualized_return,
            'Sharpe Ratio': sharpe_ratio
        }

        return df, metrics