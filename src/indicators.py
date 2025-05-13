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
  #----------------------------------------------------------------------------------------------------------------------  
  def addStochasticOscillator(self, kWindow:int=14, dWindow:int=3) -> pd.DataFrame:
    df = self.df.copy()
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
    return df
  #--------------------------------------------------------------------------------------------------------------------------------
  def addStochastic(self, kWindow: int = 16, dWindow: int = 3):
    #self.df = self.addStochasticOscillator(kWindow, dWindow)
    stoch = self.addStochasticOscillator(kWindow, dWindow)
    # make slow Stochastic Oscillator with standard window 3
    self.df['stochK'] = stoch["%K"].copy()
    self.df['stochD'] = stoch["%D"].copy()
    # make average of stochastic k and signal
    # 20, 5; 15, 12; 18, 14
    #df['stochKSlow'] = df['stochK'].rolling(window=12).mean()
    #df['StMaS'] = df['stochSignalK'].rolling(window=9).mean()
    stoch = self.addStochasticOscillator(44,5)
    self.df['stochKSlow'] = stoch["%K"].copy()
    self.df['stochDSlow'] = stoch["%D"].copy()
  #--------------------------------------------------------------------------------------------------------------------------------
  def addMacd(self,slow=29, fast=12, smooth=6):
    # MACD
    #   MACD calculation
    #   MACD        = EMA(close, timeperiod=12) - EMA(close, timeperiod=26)
    #   MACD signal = EMA(MACD_slow, timeperiod=9)
    #   Histogram   = MACD_slow - MACD_signal 
    #   commonly used (fast, slow, smooth) = (26, 12, 9)
    # slow, fast, smooth = 29, 10, 6
    df = self.df
    exp1   = df['Close'].ewm(span = fast, adjust = False).mean()
    exp2   = df['Close'].ewm(span = slow, adjust = False).mean()
    macd   = pd.DataFrame(exp1 - exp2).rename(columns = {'Close':'Macd'})
    signal = pd.DataFrame(macd.ewm(span = smooth, adjust = False).mean()).rename(columns = {'Macd':'MacdSignal'})
    hist   = pd.DataFrame(macd['Macd'] - signal['MacdSignal']).rename(columns = {0:'hist'})
    df['Macd']    = macd
    df['MacdSignal'] = signal
    df['MacdHist'] = hist.rename(columns = {0:'MacdHist'})
    self.df = df
  #--------------------------------------------------------------------------------------------------------------------------------
  def addRsi(self, window: int = 14):
    """Calculate RSI with Exponential Moving Average
    Adds following entries to df: RSI
    """
    df = self.df
    if 'Close' not in df.columns: return df
    if 1:
      # calculate RSI with Exponential Moving Average
      # see https://www.learnpythonwithrune.org/pandas-calculate-the-relative-strength-index-rsi-on-a-stock/
      delta     = df['Close'].diff()
      up        =    delta.clip(lower=0)
      down      = -1*delta.clip(upper=0)
      ema_up    = up  .ewm(com=10, adjust=False).mean()
      ema_down  = down.ewm(com=7, adjust=False).mean()
      rs        = ema_up/ema_down
      df['RSI'] = 100 - (100/(1 + rs))
      df['RSI'] = df['RSI'].fillna(50)

    else:
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
  def addMovingAverages(self, windows: List[int] = [5, 10, 20, 50, 100, 200]):
    """Calculate the Moving averages to df.
    Adds following entries to df: Ma#, Cma#, Ema# with # = 5,10,20,50,100,200
    """
    df = self.df
    if 'Close' not in df.columns: return df
    for window in windows:
      if window <= len(df):
        df[f'Sma{window}'] = df['Close'].rolling(window=window, min_periods=1).mean()
        df[f'Cma{window}'] = df['Close'].expanding().mean()
        df[f'Ema{window}'] = df['Close'].ewm(span=window).mean()
        #df[f'Ema{window}'] = ta.ema(df['Close'], window)
      else:
        df[f'Sma{window}'] = pd.NA
        df[f'Cma{window}'] = pd.NA
        df[f'Ema{window}'] = pd.NA
  #--------------------------------------------------------------------------------------------------------------------------------
  def addBollingerBands(self, window: int = 20, numStdDev: int = 2):
    df = self.df
    if 'Close' not in df.columns: return df
    if window <= len(df):
      df['BbMiddle'] = df['Close'].rolling(window=window, min_periods=1).mean()
      stdDev = df['Close'].rolling(window=window, min_periods=1).std()
      df['BbUpper'] = df['BbMiddle'] + (stdDev * numStdDev)
      df['BbLower'] = df['BbMiddle'] - (stdDev * numStdDev)
      df['BbSize'] = df['BbUpper'] - df['BbLower']
    else:
      df['BbMiddle'] = pd.NA
      df['BbUpper'] = pd.NA
      df['BbLower'] = pd.NA
      df['BbSize'] = pd.NA
  #----------------------------------------------------------------------------------------------------------------------  
  def addVolatility(self):
    # from https://www.learnpythonwithrune.org/calculate-the-volatility-of-historic-stock-prices-with-pandas-and-python/
    self.df['Vola'] = np.log(self.df['Close']/self.df['Close'].shift()).std()*252**.5*100
  #----------------------------------------------------------------------------------------------------------------------  
  def calculate(self)-> 'Calculator':
    if not self.df.empty: 
      self.addMovingAverages()
      self.addBollingerBands()
      self.addMacd()
      self.addRsi()
      self.addStochastic()
      self.addVolatility()
    return self
