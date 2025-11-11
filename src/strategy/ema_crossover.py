import pandas as pd
data = pd.read_csv(r'D:\Projects\Algo_trade\cache\NIFTY_50_2025-10-12_2025-11-11_5m.csv')

class EMACrossoverStrategy:
    def __init__(self, short_window: int= 10, long_window: int=20, max_risk_pts: float=40.0):

        #1 EMA period lengths
        self.short =short_window
        self.long = long_window
        
        #2 Maximum allowed risk (Stop Loss in Points)
        self.max_risk_pts = max_risk_pts

    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        signals = pd.DataFrame(index=data.index)
        signals['Signal'] = 0.0
         # --- CORRECTION STARTS HERE ---
        # Ensure 'Close' and 'Low' columns are strictly numeric.
        #    'errors="coerce"' turns non-numeric values (like '^NSEI') into NaN.
        data['Close'] = pd.to_numeric(data['Close'], errors='coerce')
        data['Low'] = pd.to_numeric(data['Low'], errors='coerce')
        
        # Drop any rows where the price is NaN after conversion (e.g., failed fetch rows)
        data.dropna(subset=['Close', 'Low'], inplace=True)
        # --- CORRECTION ENDS HERE ---

        #3 Calculate EMAs
        signals['EMA_10'] = data['Close'].ewm(span=self.short, adjust=False).mean()
        signals['EMA_20'] = data['Close'].ewm(span=self.long, adjust=False).mean()

        #4 Define Entry Conditions 
        # Buy Signal: EMA 10 crosses above EMA 20

        signals['Crossover'] = (signals['EMA_10'] > signals['EMA_20']) & (signals['EMA_10'].shift(1) <= signals['EMA_20'].shift(1))

        #5 Calculate Risks and Targets for Buy Entries

        signals['Entry_Price'] = data['Close']
        signals['SL_Price'] = data['Low'].shift(1)
        signals['Calculated_Risk'] = signals['Entry_Price'] - signals['SL_Price']

        #6 Applying rules to filter trades

        buy_crossover = signals['Crossover'] == True

        risk_filter = signals['Calculated_Risk'] <= self.max_risk_pts

        valid_risk = signals['Calculated_Risk'] > 0

        signals.loc[buy_crossover & risk_filter & valid_risk, 'Signal'] = 1.0

        signals['Target_Price'] = signals['Entry_Price'] + (signals['Calculated_Risk'] * 2)

        return signals[signals['Signal'] == 1.0][["Signal", 'Entry_Price', 'SL_Price', 'Calculated_Risk', 'Target_Price']]
    
if __name__ == "__main__":
    strategy = EMACrossoverStrategy()
    signals_df = strategy.generate_signals(data)
    print(signals_df)