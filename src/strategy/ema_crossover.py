import pandas as pd
import talib as tl
import numpy as np

class EMACrossoverStrategy:
    def __init__(self, short_window: int= 10, long_window: int=20, max_risk_pts: float=20.0):

        #1 EMA period lengths
        self.short =short_window
        self.long = long_window
        
        #2 Maximum allowed risk (Stop Loss in Points)
        self.max_risk_pts = max_risk_pts

    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        signals = pd.DataFrame(index=data.index)
        signals['Signal'] = 0.0
        
        #3 Calculate EMAs
        signals['EMA_10'] = tl.EMA(data['Close'].to_numpy(), timeperiod=10)
        signals['EMA_20'] = tl.EMA(data['Close'].to_numpy(), timeperiod=20)


        #4 Define Entry Conditions - EMA Crossover

        signals['BullishCrossover'] = (signals['EMA_10'] > signals['EMA_20']) & (signals['EMA_10'].shift(1) <= signals['EMA_20'].shift(1))
        signals["BearishCrossover"] = (signals['EMA_10'] < signals['EMA_20']) & (signals['EMA_10'].shift(1) >= signals['EMA_20'].shift(1))
        
        #5 Common Price Data

        signals['Entry_Price'] = data['Close']
        signals['High'] = data['High']
        signals['Low'] = data['Low']

        #6 Applying rules to filter trades
            #A. Buy side trades (Bullish Crossover -> CE)
        
        signals['SL_Price_Buy'] = signals['Low'].shift(1)
        signals['Risk_Buy'] = signals['Entry_Price'] - signals['SL_Price_Buy']
        buy_filter = (signals['BullishCrossover']) & (signals['Risk_Buy'] > 0) & (signals['Risk_Buy'] <= self.max_risk_pts)
        
        signals.loc[buy_filter, 'Signal'] = 1.0
        signals.loc[buy_filter, 'SL_Price'] = signals['SL_Price_Buy']
        signals.loc[buy_filter, 'Calculated_Risk'] = signals['Risk_Buy']
        signals.loc[buy_filter, 'Target_price'] = signals['Entry_Price'] + (signals['Calculated_Risk'] * 2)

            #B. Sell side trades (Bearish Crossover -> PE)
        signals['SL_Price_Sell'] = signals['High'].shift(1)
        signals['Risk_Sell'] = signals['SL_Price_Sell'] - signals['Entry_Price']
        sell_filter = (signals['BearishCrossover']) & (signals['Risk_Sell'] > 0) & (signals['Risk_Sell'] <= self.max_risk_pts)

        signals.loc[sell_filter, 'Signal'] = -1.0
        signals.loc[sell_filter, 'SL_Price'] = signals['SL_Price_Sell']
        signals.loc[sell_filter, 'Calculated_Risk'] = signals['Risk_Sell']
        signals.loc[sell_filter, 'Target_Price'] = signals['Entry_Price'] - (signals['Calculated_Risk'] * 2)

        signals = signals[['Signal', 'Entry_Price', 'SL_Price', 'Calculated_Risk', 'Target_Price']]
        signals.dropna(inplace=True)

        return signals

    def save_signals(self, signals: pd.DataFrame, filepath: str | None = None) -> None:
        
        import os
        
        # Default to data/backtest_results folder if filepath not provided
        if filepath is None:
            
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            filepath = os.path.join(project_root, 'data', 'backtest_results', 'signals_output.csv')
        
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        try:
            signals.to_csv(filepath)
            total_signals = len(signals)
            print(f"[SUCCESS] Signals saved to {filepath}")
            print(f"[SUMMARY] Total buy signals generated: {total_signals}")
            print(f"[DETAILS] Signal dates range: {signals.index[0]} to {signals.index[-1]}" if total_signals > 0 else "[DETAILS] No signals generated")
        except Exception as e:
            print(f"[ERROR] Failed to save signals: {e}")
    
if __name__ == "__main__":
    import sys
    import os
    
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    
    from data.data_fetcher import fetch_nifty_data
    from datetime import date, timedelta
    
    end = date.today()
    start = end - timedelta(days=59)
    data = fetch_nifty_data(start=start, end=end)
    
    strategy = EMACrossoverStrategy()
    signals_df = strategy.generate_signals(data)
    
    strategy.save_signals(signals_df)
    
    print(f"\n{signals_df}")