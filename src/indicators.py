import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
#--------------------------------------------------------------------------------------------------------------------------------
class Calculator:
  def __init__(self):
    self.df = pd.DataFrame()  
  #--------------------------------------------------------------------------------------------------------------------------------
  def setDataframe(self, df: pd.DataFrame) -> 'Calculator': 
    self.df = df.copy()
    return self
  #--------------------------------------------------------------------------------------------------------------------------------
  def get(self) -> pd.DataFrame: 
    return self.df
  #--------------------------------------------------------------------------------------------------------------------------------
  def addSmas(self, windows: List[int] = [10, 20, 50, 100, 200]):
    df = self.df
    if 'Close' not in df.columns: return df
    for window in windows:
      if window <= len(df):
        df[f'Sma{window}'] = df['Close'].rolling(window=window, min_periods=1).mean()
      else:
        df[f'Sma{window}'] = pd.NA
  #--------------------------------------------------------------------------------------------------------------------------------
  def addBollingerBands(self, window: int = 20, numStdDev: int = 2):
    df = self.df
    if 'Close' not in df.columns: return df
    if window <= len(df):
      df['BbMiddle'] = df['Close'].rolling(window=window, min_periods=1).mean()
      stdDev = df['Close'].rolling(window=window, min_periods=1).std()
      df['BbUpper'] = df['BbMiddle'] + (stdDev * numStdDev)
      df['BbLower'] = df['BbMiddle'] - (stdDev * numStdDev)
    else:
      df['BbMiddle'] = pd.NA
      df['BbUpper'] = pd.NA
      df['BbLower'] = pd.NA
  #--------------------------------------------------------------------------------------------------------------------------------
  def addMacd(self, shortWindow: int = 12, longWindow: int = 26, signalWindow: int = 9):
    df = self.df
    if 'Close' not in df.columns: return df
    if longWindow <= len(df):
      shortEma = df['Close'].ewm(span=shortWindow, adjust=False, min_periods=1).mean()
      longEma = df['Close'].ewm(span=longWindow, adjust=False, min_periods=1).mean()
      df['MACD'] = shortEma - longEma
      if signalWindow <= len(df['MACD'].dropna()):
        df['MacdSignal'] = df['MACD'].ewm(span=signalWindow, adjust=False, min_periods=1).mean()
        df['MacdHist'] = df['MACD'] - df['MacdSignal']
      else:
        df['MacdSignal'] = pd.NA
        df['MacdHist'] = pd.NA
    else:
      df['MACD'] = pd.NA
      df['MacdSignal'] = pd.NA
      df['MacdHist'] = pd.NA
  #--------------------------------------------------------------------------------------------------------------------------------
  def addRsi(self, window: int = 14):
    df = self.df
    if 'Close' not in df.columns: return df
    if window < len(df):
      delta = df['Close'].diff(1)
      gain = (delta.where(delta > 0, 0.0)).rolling(window=window, min_periods=1).mean()
      loss = (-delta.where(delta < 0, 0.0)).rolling(window=window, min_periods=1).mean()
      rs = gain / loss.replace(0, 1e-9)
      df['RSI'] = 100 - (100 / (1 + rs))
      df['RSI'] = df['RSI'].fillna(50)
    else:
      df['RSI'] = 50.0
  #--------------------------------------------------------------------------------------------------------------------------------
  def addStochastic(self, kWindow: int = 14, dWindow: int = 3):
    df = self.df
    if 'Close' not in df.columns or 'Low' not in df.columns or 'High' not in df.columns: return df
    if kWindow <= len(df):
      lowMin = df['Low'].rolling(window=kWindow, min_periods=1).min()
      highMax = df['High'].rolling(window=kWindow, min_periods=1).max()
      denominator = (highMax - lowMin)
      df['%K'] = 100 * ((df['Close'] - lowMin) / denominator.replace(0, 1e-9))
      df['%K'] = df['%K'].fillna(50)
      if dWindow <= len(df['%K'].dropna()):
        df['%D'] = df['%K'].rolling(window=dWindow, min_periods=1).mean()
        df['%D'] = df['%D'].fillna(50)
      else:
        df['%D'] = 50.0
    else:
      df['%K'] = 50.0
      df['%D'] = 50.0
  #----------------------------------------------------------------------------------------------------------------------  
  def addVolatility(self):
    # from https://www.learnpythonwithrune.org/calculate-the-volatility-of-historic-stock-prices-with-pandas-and-python/
    self.df['Vola'] = np.log(self.df['Close']/self.df['Close'].shift()).std()*252**.5*100
  #----------------------------------------------------------------------------------------------------------------------  
  def calculate(self)-> 'Calculator':
    if not self.df.empty: 
      self.addSmas()
      self.addBollingerBands()
      self.addMacd()
      self.addRsi()
      self.addStochastic()
      #df = self.addVolatility()
    return self
    
