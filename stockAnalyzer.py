
import tkinter as tk
from tkinter import ttk, messagebox, Listbox, Scrollbar, END, simpledialog
import yfinance as yf
import pandas as pd
import datetime
import mplfinance as mpf
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import matplotlib.pyplot as plt
import threading
from typing import List, Dict, Any, Optional, Tuple
import os
#--------------------------------------------------------------------------------------------------------------------------------
# This is needed to prevent 'Too Many Requests. Rate limited. Try after a while.'
# see https://github.com/ranaroussi/yfinance/issues/2422
from curl_cffi import requests
global session
session = None
#--------------------------------------------------------------------------------------------------------------------------------
class MarketDataProvider:
  #--------------------------------------------------------------------------------------------------------------------------------
  def getHistoricalData(self, tickerSymbol: str, startDate: datetime.date, endDate: datetime.date, interval: str ='1d') -> pd.DataFrame:
    raise NotImplementedError
  #--------------------------------------------------------------------------------------------------------------------------------
  def getCompanyInfo(self, tickerSymbol: str) -> Dict[str, Any]:
    raise NotImplementedError
#--------------------------------------------------------------------------------------------------------------------------------
class YFinanceProvider(MarketDataProvider):
  #--------------------------------------------------------------------------------------------------------------------------------
  def getHistoricalData(self, tickerSymbol: str, startDate: datetime.date, endDate: datetime.date, interval: str ='1d') -> pd.DataFrame:
    global session
    if session is None:
      session = requests.Session(impersonate="chrome")
    ticker = yf.Ticker(tickerSymbol, session=session)
    data = ticker.history(start=startDate, end=endDate, interval=interval, auto_adjust=True, prepost=False)
    if data.empty:
      return pd.DataFrame()
    data.rename(columns={col: col.capitalize() for col in data.columns if col in ['open', 'high', 'low', 'close', 'volume']}, inplace=True)

    if isinstance(data.index, pd.DatetimeIndex):
      if data.index.tz is not None:
        data.index = data.index.tz_convert(None)
    return data
  #--------------------------------------------------------------------------------------------------------------------------------
  def getCompanyInfo(self, tickerSymbol: str) -> Dict[str, Any]:
    ticker = yf.Ticker(tickerSymbol)
    try:
      info = ticker.info
      if not info or (info.get('regularMarketPrice') is None and \
                      info.get('previousClose') is None and \
                      not info.get('longName') and \
                      info.get('currency') is None and
                      not info.get('marketCap')):
        return {"error": f"No substantial company information found for {tickerSymbol}."}
      return info
    except Exception as e:
      return {"error": f"Could not retrieve company info for {tickerSymbol}: {str(e)}"}
#--------------------------------------------------------------------------------------------------------------------------------
class InteractiveBrokersProvider(MarketDataProvider):
  #--------------------------------------------------------------------------------------------------------------------------------
  def getHistoricalData(self, tickerSymbol: str, startDate: datetime.date, endDate: datetime.date, interval: str ='1d') -> pd.DataFrame:
    raise NotImplementedError("Interactive Brokers API not implemented yet.")
  #--------------------------------------------------------------------------------------------------------------------------------
  def getCompanyInfo(self, tickerSymbol: str) -> Dict[str, Any]:
    raise NotImplementedError("Interactive Brokers API not implemented yet.")
#--------------------------------------------------------------------------------------------------------------------------------
# --- Indicator Calculations ---
class IndicatorCalculator:
  #--------------------------------------------------------------------------------------------------------------------------------
  def addSmas(self, dataFrame: pd.DataFrame, windows: List[int] = [10, 20, 50, 100, 200]) -> pd.DataFrame:
    df = dataFrame.copy()
    if 'Close' not in df.columns: return df
    for window in windows:
      if window <= len(df):
        df[f'SMA_{window}'] = df['Close'].rolling(window=window, min_periods=1).mean()
      else:
        df[f'Sma{window}'] = pd.NA
    return df
  #--------------------------------------------------------------------------------------------------------------------------------
  def addBollingerBands(self, dataFrame: pd.DataFrame, window: int = 20, numStdDev: int = 2) -> pd.DataFrame:
    df = dataFrame.copy()
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
    return df
  #--------------------------------------------------------------------------------------------------------------------------------
  def addMacd(self, dataFrame: pd.DataFrame, shortWindow: int = 12, longWindow: int = 26, signalWindow: int = 9) -> pd.DataFrame:
    df = dataFrame.copy()
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
    return df
  #--------------------------------------------------------------------------------------------------------------------------------
  def addRsi(self, dataFrame: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    df = dataFrame.copy()
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
    return df
  #--------------------------------------------------------------------------------------------------------------------------------
  def addStochastic(self, dataFrame: pd.DataFrame, kWindow: int = 14, dWindow: int = 3) -> pd.DataFrame:
    df = dataFrame.copy()
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
class ChartingUtils:
  #--------------------------------------------------------------------------------------------------------------------------------
  def createErrorFigure(self, message: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.text(0.5, 0.5, message, ha='center', va='center', fontsize=10, wrap=True)
    try:
      fig.tight_layout(pad=0.5)
    except Exception: # NOSONAR
      pass
    return fig
  #--------------------------------------------------------------------------------------------------------------------------------
  def preparePlotData(self, dataFrame: pd.DataFrame, tickerSymbol: str, chartTimeframe: str) -> Optional[pd.DataFrame]:
    if dataFrame.empty or len(dataFrame) < 2:
      return None

    requiredOhlcCols = ['Open', 'High', 'Low', 'Close']
    if not all(col in dataFrame.columns for col in requiredOhlcCols):
      return None

    plotDf = dataFrame.copy()
    if 'Volume' not in plotDf.columns:
      plotDf['Volume'] = 0

    plotDf.dropna(subset=requiredOhlcCols, how='any', inplace=True)
    plotDf['Volume'] = plotDf['Volume'].fillna(0)

    if plotDf.empty or len(plotDf) < 2:
      return None

    if not isinstance(plotDf.index, pd.DatetimeIndex):
      try:
        plotDf.index = pd.to_datetime(plotDf.index)
      except Exception:
        return None
    return plotDf
  #--------------------------------------------------------------------------------------------------------------------------------
  def addSmaPlotsManual(self, plotDf: pd.DataFrame, addPlots: List[Dict[str, Any]], windows: List[int] = [10, 20, 50, 100, 200]):
    for window in windows:
      colName = f'Sma{window}'
      if colName in plotDf.columns and not plotDf[colName].isnull().all():
        addPlots.append(mpf.make_addplot(plotDf[colName], panel=0, width=0.7))
  #--------------------------------------------------------------------------------------------------------------------------------
  def addBollingerBandsToPlot(self, plotDf: pd.DataFrame, addPlots: List[Dict[str, Any]]):
    if 'BbUpper' in plotDf.columns and 'BbLower' in plotDf.columns:
      if not plotDf['BbUpper'].isnull().all() and not plotDf['BbLower'].isnull().all():
        addPlots.append(mpf.make_addplot(plotDf['BbUpper'], panel=0, color='darkgray', linestyle='--', width=0.7))
        addPlots.append(mpf.make_addplot(plotDf['BbLower'], panel=0, color='darkgray', linestyle='--', width=0.7))
  #--------------------------------------------------------------------------------------------------------------------------------
  def addVolumeToPlot(self, plotDf: pd.DataFrame, addPlots: List[Dict[str, Any]]):
    if 'Volume' in plotDf.columns and not plotDf['Volume'].isnull().all() and plotDf['Volume'].sum() > 0 :
      addPlots.append(mpf.make_addplot(plotDf['Volume'], panel=0, type='bar', width=0.7,
                                        ylabel='Volume', color='lightgray', alpha=0.7,
                                        secondary_y=True))
  #--------------------------------------------------------------------------------------------------------------------------------
  def addMacdToPlot(self, plotDf: pd.DataFrame, addPlots: List[Dict[str, Any]], currentPanelId: int) -> bool:
    if 'MACD' in plotDf.columns and not plotDf['MACD'].isnull().all():
      addPlots.append(mpf.make_addplot(plotDf['MACD'], panel=currentPanelId, color='dodgerblue', ylabel='MACD', width=0.8))
      if 'MacdSignal' in plotDf.columns and not plotDf['MacdSignal'].isnull().all():
        addPlots.append(mpf.make_addplot(plotDf['MacdSignal'], panel=currentPanelId, color='orangered', width=0.8))
      if 'MacdHist' in plotDf.columns and not plotDf['MacdHist'].isnull().all():
        macdHistNumeric = pd.to_numeric(plotDf['MacdHist'], errors='coerce').fillna(0)
        if not macdHistNumeric.isnull().all() and (macdHistNumeric != 0).any():
          macdHistGreen = macdHistNumeric.where(macdHistNumeric >= 0, 0)
          macdHistRed = macdHistNumeric.where(macdHistNumeric < 0, 0)
          if (macdHistGreen != 0).any():
            addPlots.append(mpf.make_addplot(macdHistGreen, type='bar', panel=currentPanelId, color='green', alpha=0.7))
          if (macdHistRed != 0).any():
            addPlots.append(mpf.make_addplot(macdHistRed, type='bar', panel=currentPanelId, color='red', alpha=0.7))
      return True
    return False
  #--------------------------------------------------------------------------------------------------------------------------------
  def addRsiToPlot(self, plotDf: pd.DataFrame, addPlots: List[Dict[str, Any]], currentPanelId: int, rsiYlabelOverride: Optional[str] = None) -> bool:
    if 'RSI' in plotDf.columns and not plotDf['RSI'].isnull().all():
      actualRsiYlabel = rsiYlabelOverride if rsiYlabelOverride else 'RSI'
      addPlots.append(mpf.make_addplot(plotDf['RSI'], panel=currentPanelId, color='purple', ylabel=actualRsiYlabel, ylim=(0,100), width=0.8))
      addPlots.append(mpf.make_addplot(pd.Series(80, index=plotDf.index), panel=currentPanelId, color='red', linestyle='dashed', width=0.7))
      addPlots.append(mpf.make_addplot(pd.Series(20, index=plotDf.index), panel=currentPanelId, color='green', linestyle='dashed', width=0.7))
      return True
    return False
  #--------------------------------------------------------------------------------------------------------------------------------
  def addStochasticToPlot(self, plotDf: pd.DataFrame, addPlots: List[Dict[str, Any]], currentPanelId: int) -> bool:
    if '%K' in plotDf.columns and not plotDf['%K'].isnull().all():
      addPlots.append(mpf.make_addplot(plotDf['%K'], panel=currentPanelId, color='darkturquoise', ylabel='Stoch', ylim=(0,100), width=0.8))
      if '%D' in plotDf.columns and not plotDf['%D'].isnull().all():
        addPlots.append(mpf.make_addplot(plotDf['%D'], panel=currentPanelId, color='deeppink', width=0.8))
      addPlots.append(mpf.make_addplot(pd.Series(70, index=plotDf.index), panel=currentPanelId, color='red', linestyle='dashed', width=0.7))
      addPlots.append(mpf.make_addplot(pd.Series(30, index=plotDf.index), panel=currentPanelId, color='green', linestyle='dashed', width=0.7))
      return True
    return False
  #--------------------------------------------------------------------------------------------------------------------------------
  def configureIndicatorPlots(self, plotDf: pd.DataFrame, addPlots: List[Dict[str, Any]], rsiYlabelOverride: Optional[str] = None) -> Tuple[List[int], int]:
    nextIndicatorPanelId = 1
    indicatorPanelRatioValues: List[int] = []
    nrOfAppends = 2
    if self.addMacdToPlot(plotDf, addPlots, nextIndicatorPanelId):
      indicatorPanelRatioValues.append(nrOfAppends)
      nextIndicatorPanelId += 1

    if self.addRsiToPlot(plotDf, addPlots, nextIndicatorPanelId, rsiYlabelOverride=rsiYlabelOverride):
      indicatorPanelRatioValues.append(nrOfAppends)
      nextIndicatorPanelId += 1

    if self.addStochasticToPlot(plotDf, addPlots, nextIndicatorPanelId):
      indicatorPanelRatioValues.append(nrOfAppends)

    return indicatorPanelRatioValues, nextIndicatorPanelId
  #--------------------------------------------------------------------------------------------------------------------------------
  def createMpfStyle(self) -> Dict:
    return mpf.make_mpf_style(base_mpf_style='yahoo',
                              rc={'axes.labelsize': 8,
                                  'xtick.labelsize': 7,
                                  'ytick.labelsize': 7,
                                  'font.size': 8,
                                  'figure.titlesize': 10,
                                  'axes.titlesize': 10,
                                  'legend.loc': 'upper left',
                                  'legend.fontsize': 7},
                              facecolor='#FDFDFD'
                              )
  #--------------------------------------------------------------------------------------------------------------------------------
  def adjustVolumeYaxisLabel(self, mainAx: plt.Axes, tickerSymbol: str, chartTimeframe: str):
    try:
      if hasattr(mainAx, 'right_ax') and mainAx.right_ax is not None:
        currentRightYlabel = mainAx.right_ax.get_ylabel()
        if currentRightYlabel == 'Volume':
          mainAx.right_ax.set_ylabel(currentRightYlabel, fontsize=8, labelpad=3)
      else:
        for twinAx in mainAx.figure.axes:
          if twinAx is not mainAx and hasattr(twinAx, 'get_shared_x_axes') and \
            mainAx in twinAx.get_shared_x_axes().get_siblings(mainAx):
            if twinAx.get_ylabel() == 'Volume':
              twinAx.set_ylabel('Volume', fontsize=8, labelpad=3)
              break
    except Exception as eTwin:
      print(f"Warning: Error adjusting volume y-axis label font for {tickerSymbol} ({chartTimeframe}): {eTwin}")
  #--------------------------------------------------------------------------------------------------------------------------------
  def adjustIndicatorAxisLabels(self, returnedAxesObject: List[plt.Axes], addPlots: List[Dict[str, Any]]):
    indicatorAxesStartObjIdx = 1
    if any(ap.get('panel') == 0 and ap.get('secondary_y') for ap in addPlots if isinstance(ap, dict)):
      indicatorAxesStartObjIdx = 2

    currentAxObjIdx = indicatorAxesStartObjIdx

    isMacdPlotted = any(isinstance(ap, dict) and ap.get('panel', -1) >= 1 and 'MACD' in ap.get('ylabel','') for ap in addPlots)
    if isMacdPlotted and len(returnedAxesObject) > currentAxObjIdx:
      ax = returnedAxesObject[currentAxObjIdx]
      ax.set_ylabel(ax.get_ylabel() or 'MACD', fontsize=8, labelpad=3)
      currentAxObjIdx +=1

    isRsiPlotted = any(isinstance(ap, dict) and ap.get('panel', -1) >= 1 and 'RSI' in ap.get('ylabel','') for ap in addPlots)
    if isRsiPlotted and len(returnedAxesObject) > currentAxObjIdx:
      ax = returnedAxesObject[currentAxObjIdx]
      ax.set_ylabel(ax.get_ylabel() or 'RSI', fontsize=8, labelpad=3)
      currentAxObjIdx +=1

    isStochPlotted = any(isinstance(ap, dict) and ap.get('panel', -1) >= 1 and 'Stoch' in ap.get('ylabel','') for ap in addPlots)
    if isStochPlotted and len(returnedAxesObject) > currentAxObjIdx:
      ax = returnedAxesObject[currentAxObjIdx]
      ax.set_ylabel(ax.get_ylabel() or 'Stoch', fontsize=8, labelpad=3)
  #--------------------------------------------------------------------------------------------------------------------------------
  def applyAxisLabelAdjustments(self, returnedAxesObject: Optional[List[plt.Axes]], tickerSymbol: str, chartTimeframe: str, addPlots: List[Dict[str, Any]]):
    if not returnedAxesObject or not isinstance(returnedAxesObject, list) or not returnedAxesObject:
      print(f"Warning: `returnedAxes` from mplfinance was not a list or empty for {tickerSymbol} ({chartTimeframe}). Type: {type(returnedAxesObject)}")
      return
    try:
      axList = returnedAxesObject[3:]
      if axList :
        axList[0].set_yticklabels([])
        axList[0].set_yticks([])

      returnedAxesObject[0].set_ylabel('Price', fontsize=8, labelpad=3)

      volumeOnSecondaryYaxis = any(
        isinstance(ap, dict) and
        ap.get('panel') == 0 and
        ap.get('secondary_y') == True and
        ap.get('ylabel') == 'Volume'
        for ap in addPlots
      )
      if volumeOnSecondaryYaxis:
        self.adjustVolumeYaxisLabel(returnedAxesObject[0], tickerSymbol, chartTimeframe)

      self.adjustIndicatorAxisLabels(returnedAxesObject, addPlots)
    except IndexError:
      pass
    except Exception as e:
      pass
  #--------------------------------------------------------------------------------------------------------------------------------
  def createStockChartFigure(self,
                              dataFrame: pd.DataFrame,
                              tickerSymbol: str,
                              chartTimeframe: str = 'Daily',
                              useManualSmas: bool = False,
                              manualSmaWindows: List[int] = [10, 20, 50, 100, 200],
                              movingAverageWindows: Optional[Tuple[int, ...]] = (20, 50, 100, 200),
                              rsiYlabelOverride: Optional[str] = None,
                              figsize: Tuple[float, float] = (9, 5.5)
                              ) -> plt.Figure:
    plotDf = self.preparePlotData(dataFrame, tickerSymbol, chartTimeframe)
    if plotDf is None:
      msg = f"Data preparation failed for\n{chartTimeframe} chart of {tickerSymbol}"
      if dataFrame.empty or len(dataFrame) < 2: msg = f"No data or not enough data for\n{chartTimeframe} chart of {tickerSymbol}"
      elif not all(col in dataFrame.columns for col in ['Open', 'High', 'Low', 'Close']): msg = f"Missing essential OHLC columns for\n{chartTimeframe} chart of {tickerSymbol}"
      elif not isinstance(dataFrame.index, pd.DatetimeIndex) and (isinstance(dataFrame.index, pd.Index) and not isinstance(pd.to_datetime(dataFrame.index, errors='coerce'), pd.DatetimeIndex)): msg = f"Invalid date index for {chartTimeframe} chart of {tickerSymbol}"
      else: msg = f"Not enough valid OHLCV data after cleaning for\n{chartTimeframe} chart of {tickerSymbol}"
      return self.createErrorFigure(msg)

    addPlots: List[Dict[str, Any]] = []
    mavParamForPlot = None
    if useManualSmas:
      self.addSmaPlotsManual(plotDf, addPlots, windows=manualSmaWindows)
    else:
      mavParamForPlot = movingAverageWindows

    self.addBollingerBandsToPlot(plotDf, addPlots)
    self.addVolumeToPlot(plotDf, addPlots)
    indicatorPanelRatioValues, _ = self.configureIndicatorPlots(plotDf, addPlots, rsiYlabelOverride=rsiYlabelOverride)

    finalPanelRatios = tuple([6] + indicatorPanelRatioValues) if indicatorPanelRatioValues else (1,)
    mpfStyle = self.createMpfStyle()
    fig: Optional[plt.Figure] = None
    returnedAxesObject: Optional[List[plt.Axes]] = None
    try:
      fig, returnedAxesObject = mpf.plot(plotDf,
                  type='candle', style=mpfStyle, title=f'{tickerSymbol} - {chartTimeframe}',
                  addplot=addPlots,
                  mav=mavParamForPlot,
                  panel_ratios=finalPanelRatios,
                  figscale=1.0, volume=False, returnfig=True, figsize=figsize,
                  update_width_config=dict(candle_linewidth=0.7, candle_width=0.6) )
    except Exception as e:
      print(f"Error during mplfinance.plot for {tickerSymbol} ({chartTimeframe}): {e}")
      import traceback; traceback.print_exc()
      return self.createErrorFigure(f"Plotting error for {tickerSymbol} ({chartTimeframe}):\n{str(e)[:100]}")

    if fig is None:
      return self.createErrorFigure(f"Figure generation failed for {tickerSymbol} ({chartTimeframe}).")

    try: fig.subplots_adjust(left=0.08, bottom=0.12, right=0.92, top=0.92, hspace=0.3)
    except Exception as e: print(f"Warning: subplots_adjust failed: {e}")

    self.applyAxisLabelAdjustments(returnedAxesObject, tickerSymbol, chartTimeframe, addPlots)
    return fig
#--------------------------------------------------------------------------------------------------------------------------------
# --- Main Application ---
class StockAnalyzerApp:
  #--------------------------------------------------------------------------------------------------------------------------------
  def __init__(self, root: tk.Tk):
    self.root = root
    self.root.title("Stock Analyzer")
    self.root.minsize(1600, 900)

    self.dataProvider = YFinanceProvider()
    self.indicatorCalc = IndicatorCalculator()
    self.chartUtils = ChartingUtils()

    self.currentTicker = tk.StringVar(value='AAPL')
    self.stockList = []
    self.stockList = self.loadStockListFromFile()

    self.dailyChartCanvas: Optional[FigureCanvasTkAgg] = None
    self.weeklyChartCanvas: Optional[FigureCanvasTkAgg] = None
    self.dailyFig: Optional[plt.Figure] = None
    self.weeklyFig: Optional[plt.Figure] = None
    self.dailyToolbar: Optional[NavigationToolbar2Tk] = None
    self.weeklyToolbar: Optional[NavigationToolbar2Tk] = None
    self.displayYearsVar = tk.IntVar(value=2)

    self.setupUserInterface()
    self.updateTickerListBox()
    if self.stockList:
      self.tickerListBox.selection_set(0)
      self.handleTickerSelect(None)
  #--------------------------------------------------------------------------------------------------------------------------------
  def loadStockListFromFile(self, filename: str = "listStocks") -> List[str]:
    scriptDir = os.path.dirname(os.path.abspath(__file__))
    filePath = os.path.join(scriptDir, filename)
    defaultStocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'VOW.DE', 'META', 'JPM', 'BTC-USD', 'ETH-USD']
    try:
      with open(filePath, 'r') as f:
        stocks = [line.strip() for line in f if line.strip()]
      if not stocks:
        print(f"Warning: Stock file '{filePath}' is empty. Using default stocks.")
        self.stockList = defaultStocks[:]
        self.saveStockListToFile(filename=filename)
        return defaultStocks
      return stocks
    except FileNotFoundError:
      print(f"Warning: Stock file '{filePath}' not found. Using default stocks and creating file.")
      self.stockList = defaultStocks[:]
      self.saveStockListToFile(filename=filename)
      return defaultStocks
    except Exception as e:
      print(f"Error reading stock file '{filePath}': {e}. Using default stocks.")
      return defaultStocks[:]
  #--------------------------------------------------------------------------------------------------------------------------------
  def saveStockListToFile(self, filename: str = "listStocks"):
    scriptDir = os.path.dirname(os.path.abspath(__file__))
    filePath = os.path.join(scriptDir, filename)
    try:
      with open(filePath, 'w') as f:
        for ticker in self.stockList:
          f.write(f"{ticker}\n")
      print(f"Stocklist saved to '{filePath}'.")
    except Exception as e:
      print(f"Error saving stocklist to '{filePath}': {e}")
  #--------------------------------------------------------------------------------------------------------------------------------
  def setupWatchlistPane(self, parentPane: ttk.PanedWindow):
    widthWatchlist = 1
    watchlistFrame = ttk.LabelFrame(parentPane, text="Watchlist", padding=2)
    watchlistFrame.columnconfigure(0, weight=1)
    watchlistFrame.rowconfigure(1, weight=1)

    addTickerFrame = ttk.Frame(watchlistFrame)
    addTickerFrame.grid(row=0, column=0, sticky="ew", pady=(0,5))
    addTickerFrame.columnconfigure(0, weight=1)

    self.newTickerEntry = ttk.Entry(addTickerFrame, width=widthWatchlist)
    self.newTickerEntry.grid(row=0, column=0, sticky="ew", padx=(0,5))
    self.newTickerEntry.bind("<Return>", self.addTickerToList)
    addTickerButton = ttk.Button(addTickerFrame, text="Add", command=self.addTickerToList, width=5)
    addTickerButton.grid(row=0, column=1, sticky="e")

    listboxFrame = ttk.Frame(watchlistFrame)
    listboxFrame.grid(row=1, column=0, sticky="nsew")
    listboxFrame.rowconfigure(0, weight=1)
    listboxFrame.columnconfigure(0, weight=1)

    self.tickerListBox = Listbox(listboxFrame, selectmode=tk.SINGLE, exportselection=False, activestyle='none')
    self.tickerListBox.grid(row=0, column=0, sticky="nsew")
    self.tickerListBox.bind("<<ListboxSelect>>", self.handleTickerSelect)

    listScrollbar = Scrollbar(listboxFrame, orient="vertical", command=self.tickerListBox.yview)
    listScrollbar.grid(row=0, column=1, sticky="ns")
    self.tickerListBox.config(yscrollcommand=listScrollbar.set)

    removeTickerButton = ttk.Button(watchlistFrame, text="Remove Selected", command=self.removeSelectedTicker)
    removeTickerButton.grid(row=2, column=0, sticky="ew", pady=(5,0))

    timePeriodFrame = ttk.Frame(watchlistFrame, padding=(0, 5, 0, 0))
    timePeriodFrame.grid(row=3, column=0, sticky="ew", pady=(10,0))
    timePeriodFrame.columnconfigure(1, weight=1)
    ttk.Label(timePeriodFrame, text="Years:").grid(row=0, column=0, sticky="w", padx=(0,2))
    self.yearsEntry = ttk.Entry(timePeriodFrame, textvariable=self.displayYearsVar, width=4)
    self.yearsEntry.grid(row=0, column=1, sticky="ew", padx=(0,5))
    updatePeriodButton = ttk.Button(timePeriodFrame, text="update", command=self.updateDisplayPeriodAndReload)
    updatePeriodButton.grid(row=0, column=2, sticky="e")

    parentPane.add(watchlistFrame, weight=widthWatchlist)
  #--------------------------------------------------------------------------------------------------------------------------------
  def setupContentAreaPanes(self, parentPane: ttk.PanedWindow):
    contentArea = ttk.PanedWindow(parentPane, orient=tk.HORIZONTAL)

    widthDaily = 8
    widthWeeklyAndInfo = 3

    self.dailyChartFrameContainer = ttk.LabelFrame(contentArea, text="Daily Chart", padding=6)
    contentArea.add(self.dailyChartFrameContainer, weight=widthDaily)

    weeklyAndInfoFrame = ttk.PanedWindow(contentArea, orient=tk.VERTICAL)
    self.weeklyChartFrameContainer = ttk.LabelFrame(weeklyAndInfoFrame, text="Weekly Chart", padding=4)
    weeklyAndInfoFrame.add(self.weeklyChartFrameContainer, weight=1)

    self.infoTextFrame = ttk.LabelFrame(weeklyAndInfoFrame, text="Company Information", padding=5)
    self.infoText = tk.Text(self.infoTextFrame, wrap=tk.WORD, height=10, relief=tk.FLAT, font=("Segoe UI", 9))
    self.infoText.pack(expand=True, fill=tk.BOTH, padx=5, pady=5)
    weeklyAndInfoFrame.add(self.infoTextFrame, weight=1)

    contentArea.add(weeklyAndInfoFrame, weight=widthWeeklyAndInfo)
    parentPane.add(contentArea, weight=widthDaily + widthWeeklyAndInfo)
  #--------------------------------------------------------------------------------------------------------------------------------
  def setupStatusBar(self):
    self.statusBar = ttk.Label(self.root, text="Ready", relief=tk.SUNKEN, anchor=tk.W)
    self.statusBar.pack(side=tk.BOTTOM, fill=tk.X)
  #--------------------------------------------------------------------------------------------------------------------------------
  def updateChartTitles(self):
    years = self.displayYearsVar.get()
    yearsText = f"({years} Year{'s' if years > 1 else ''})"
    if hasattr(self, 'dailyChartFrameContainer') and self.dailyChartFrameContainer.winfo_exists():
      self.dailyChartFrameContainer.config(text=f"Daily Chart {yearsText}")
    if hasattr(self, 'weeklyChartFrameContainer') and self.weeklyChartFrameContainer.winfo_exists():
      self.weeklyChartFrameContainer.config(text=f"Weekly Chart {yearsText}")
  #--------------------------------------------------------------------------------------------------------------------------------
  def setupUserInterface(self):
    self.rootPanedWindow = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
    self.rootPanedWindow.pack(expand=True, fill=tk.BOTH)

    self.setupWatchlistPane(self.rootPanedWindow)
    self.setupContentAreaPanes(self.rootPanedWindow)
    self.setupStatusBar()
    self.updateChartTitles()
  #--------------------------------------------------------------------------------------------------------------------------------
  def updateDisplayPeriodAndReload(self):
    try:
      years = self.displayYearsVar.get()
      if not (1 <= years <= 20):
        messagebox.showerror("Invalid input", "Enter a year period between 1 and 20.")
        self.displayYearsVar.set(max(1, min(20, years)))
        return
    except tk.TclError:
      messagebox.showerror("Invalid input", "Please enter a valid number for years.")
      self.displayYearsVar.set(2)
      return

    self.updateChartTitles()
    selectedIndices = self.tickerListBox.curselection()
    if selectedIndices:
      ticker = self.tickerListBox.get(selectedIndices[0])
      self.loadStockData(ticker)
    else:
      messagebox.showinfo("No selection", "Select a ticker from the watchlist.")
  #--------------------------------------------------------------------------------------------------------------------------------
  def addTickerToList(self, event=None):
    newTicker = self.newTickerEntry.get().strip().upper()
    if not newTicker:
      messagebox.showwarning("Empty Ticker", "Please enter a ticker symbol.")
      return
    if newTicker in self.stockList:
      messagebox.showinfo("Duplicate Ticker", f"{newTicker} is already in the watchlist.")
    else:
      self.stockList.append(newTicker)
      self.stockList.sort()
      self.updateTickerListBox()
      self.saveStockListToFile()
      try:
        idx = self.stockList.index(newTicker)
        self.tickerListBox.selection_clear(0, END)
        self.tickerListBox.selection_set(idx)
        self.tickerListBox.see(idx)
        self.handleTickerSelect(None)
      except ValueError: pass
    self.newTickerEntry.delete(0, END)
  #--------------------------------------------------------------------------------------------------------------------------------
  def removeSelectedTicker(self):
    selectedIndices = self.tickerListBox.curselection()
    if not selectedIndices:
      messagebox.showinfo("No selection", "Please select a ticker to remove.")
      return

    selectedTicker = self.tickerListBox.get(selectedIndices[0])

    if selectedTicker in self.stockList:
      self.stockList.remove(selectedTicker)
      self.updateTickerListBox()
      self.saveStockListToFile()
      if self.stockList:
        self.tickerListBox.selection_set(0)
        self.handleTickerSelect(None)
      else:
        self.clearPreviousCharts()
        self.infoText.delete('1.0', tk.END)
        self.infoText.insert(tk.END, "Watchlist is empty.")
        self.updateChartTitles()
        self.statusBar.config(text="Ticker removed. Watchlist is empty.")
  #--------------------------------------------------------------------------------------------------------------------------------
  def updateTickerListBox(self):
    self.tickerListBox.delete(0, END)
    for ticker in self.stockList:
      self.tickerListBox.insert(END, ticker)
  #--------------------------------------------------------------------------------------------------------------------------------
  def handleTickerSelect(self, event=None):
    selectedIndices = self.tickerListBox.curselection()
    if selectedIndices:
      ticker = self.tickerListBox.get(selectedIndices[0])
      if ticker != self.currentTicker.get() or not self.dailyFig or not self.weeklyFig :
        self.currentTicker.set(ticker)
        self.loadStockData(ticker)
      else:
        self.statusBar.config(text=f"Displaying data for {ticker}")
  #--------------------------------------------------------------------------------------------------------------------------------
  def clearPreviousCharts(self):
    if self.dailyChartCanvas:
      self.dailyChartCanvas.get_tk_widget().destroy()
      self.dailyChartCanvas = None
    if self.dailyToolbar:
      self.dailyToolbar.destroy()
      self.dailyToolbar = None
    if self.dailyFig:
      plt.close(self.dailyFig)
      self.dailyFig = None
    for widget in self.dailyChartFrameContainer.winfo_children():
      widget.destroy()

    if self.weeklyChartCanvas:
      self.weeklyChartCanvas.get_tk_widget().destroy()
      self.weeklyChartCanvas = None
    if self.weeklyToolbar:
      self.weeklyToolbar.destroy()
      self.weeklyToolbar = None
    if self.weeklyFig:
      plt.close(self.weeklyFig)
      self.weeklyFig = None
    for widget in self.weeklyChartFrameContainer.winfo_children():
      widget.destroy()
  #--------------------------------------------------------------------------------------------------------------------------------
  def displayError(self, message: str, ticker: str ="N/A"):
    self.clearPreviousCharts()

    errorFigDaily = self.chartUtils.createErrorFigure(f"Daily Chart Error for {ticker}:\n{message}")
    self.dailyFig, self.dailyChartCanvas, self.dailyToolbar = self.displaySingleChart(errorFigDaily, self.dailyChartFrameContainer, "Daily", ticker)

    errorFigWeekly = self.chartUtils.createErrorFigure(f"Weekly Chart Error for {ticker}:\n{message}")
    self.weeklyFig, self.weeklyChartCanvas, self.weeklyToolbar = self.displaySingleChart(errorFigWeekly, self.weeklyChartFrameContainer, "Weekly", ticker)

    self.infoText.delete('1.0', tk.END)
    self.infoText.insert(tk.END, f"Error loading data for {ticker}:\n{message}")
    self.statusBar.config(text=f"Error loading data for {ticker}")
  #--------------------------------------------------------------------------------------------------------------------------------
  def displaySingleChart(self, fig: Optional[plt.Figure], containerFrame: ttk.LabelFrame, chartType: str, ticker: str) -> Tuple[Optional[plt.Figure], Optional[FigureCanvasTkAgg], Optional[NavigationToolbar2Tk]]:
    if fig is None:
      fig = self.chartUtils.createErrorFigure(f"Figure is None for {ticker} {chartType}")

    for widget in containerFrame.winfo_children():
      widget.destroy()

    canvas = FigureCanvasTkAgg(fig, master=containerFrame)
    canvasWidget = canvas.get_tk_widget()
    canvasWidget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    toolbar = NavigationToolbar2Tk(canvas, containerFrame)
    toolbar.update()
    toolbar.pack(side=tk.BOTTOM, fill=tk.X)

    return fig, canvas, toolbar
  #--------------------------------------------------------------------------------------------------------------------------------
  def formatAndDisplayInfoValue(self, label: str, value: Any):
    textToInsert = f"{label}: "
    if isinstance(value, (int, float)):
      if abs(value) >= 1_000_000_000:
        textToInsert += f"{value / 1_000_000_000:.2f}B"
      elif abs(value) >= 1_000_000:
        textToInsert += f"{value / 1_000_000:.2f}M"
      elif abs(value) >= 1_000:
        textToInsert += f"{value / 1_000:.2f}K"
      else:
        textToInsert += f"{value:.2f}" if isinstance(value, float) else f"{value:,}"
    elif isinstance(value, str) and value.startswith("http"):
      textToInsert += value
    elif value is None or str(value).lower() == 'nan' or str(value).lower() == 'none':
      textToInsert += "N/A"
    else:
      textToInsert += str(value)

    self.infoText.insert(tk.END, textToInsert + "\n")
    try:
      if label == "Website" and value and isinstance(value, str) and value.startswith("http"):
        currentPos = self.infoText.index(f"end-2l linestart + {len(label) + 2}c")
        endPos = self.infoText.index(f"end-2l lineend")
        self.infoText.tag_add(f"link_{value}", currentPos, endPos)
        self.infoText.tag_config(f"link_{value}", foreground="blue", underline=True)
    except Exception:
      pass
  #--------------------------------------------------------------------------------------------------------------------------------
  def displayCompanyInfoDetails(self, companyInfo: Dict[str, Any], ticker: str):
    self.infoText.delete('1.0', tk.END)
    if not companyInfo or companyInfo.get("error"):
      errorMsg = companyInfo.get("error", f"No company information available for {ticker}.")
      self.infoText.insert(tk.END, errorMsg)
      return

    self.infoText.insert(tk.END, f"--- {companyInfo.get('longName', ticker)} ({ticker}) ---\n", ('header',))
    self.infoText.tag_config('header', font=('Segoe UI', 11, 'bold'), spacing1=5, spacing3=5)

    infoMap = {
      "Sector": "sector", 
      "Industry": "industry", 
      "Website": "website",
      "Currency": "currency", 
      "Market Cap": "marketCap",
      "Shares Outstanding": "sharesOutstanding", 
      "P/E Ratio": "trailingPE",
      "Forward P/E": "forwardPE", 
      "EPS (TTM)": "trailingEps",
      "Forward EPS": "forwardEps", 
      "Beta": "beta", 
      "Dividend Rate": "dividendRate",
      "Dividend Yield": "dividendYield",
      "Divident payout ration": "payoutRatio",
      "Ex-Dividend Date": "exDividendDate",
      "52 Week High": "fiftyTwoWeekHigh", 
      "52 Week Low": "fiftyTwoWeekLow",
      "Avg. Volume": "averageVolume", 
      "Current Price": "currentPrice",
      "Regular Market Price": 
      "regularMarketPrice", "Open": "open",
      "Previous Close": "previousClose", 
      "Day High": "dayHigh", 
      "Day Low": "dayLow",
      'Earnings date': 'earningsTimestampStart',
      'Recommendation': 'recommendationKey',
    }

    #print(f"Company info keys: {list(companyInfo.keys())}")
    for displayLabel, infoKey in infoMap.items():
      value = companyInfo.get(infoKey)
      if value is not None:
        if infoKey == "exDividendDate" and isinstance(value, (int, float)):
          try: value = datetime.datetime.fromtimestamp(value).strftime('%Y-%m-%d')
          except: pass
        elif infoKey == "earningsTimestampStart" and isinstance(value, list) and len(value) > 0:
          try:
            tsValue = min(v for v in value if isinstance(v, (int, float)))
            value = datetime.datetime.fromtimestamp(tsValue).strftime('%Y-%m-%d')
          except: pass
        elif infoKey == "earningsTimestampStart" and isinstance(value, (int, float)):
          try: value = datetime.datetime.fromtimestamp(value).strftime('%Y-%m-%d')
          except: pass

        self.formatAndDisplayInfoValue(displayLabel, value)

    summary = companyInfo.get('longBusinessSummary')
    if summary:
      self.infoText.insert(tk.END, "\n--- Business Summary ---\n", ('header2',))
      self.infoText.tag_config('header2', font=('Segoe UI', 10, 'bold'), spacing1=5, spacing3=3)
      self.infoText.insert(tk.END, summary)
  #--------------------------------------------------------------------------------------------------------------------------------
  def displayProcessedData(self, displayPayload: Dict[str, Any]):
    self.clearPreviousCharts()

    dailyData = displayPayload.get('daily_data')
    weeklyData = displayPayload.get('weekly_data')
    companyInfoValue = displayPayload.get('company_info')
    ticker = displayPayload.get('ticker', "N/A")
    error = displayPayload.get('error')

    if error:
      self.displayError(error, ticker)
      return

    dailyFigsize = (9, 5.5)
    weeklyFigsize = (6, 4.5)

    if dailyData is not None and not dailyData.empty:
      self.dailyFig = self.chartUtils.createStockChartFigure(dailyData, ticker, "Daily", figsize=dailyFigsize)
      self.dailyFig, self.dailyChartCanvas, self.dailyToolbar = self.displaySingleChart(self.dailyFig, self.dailyChartFrameContainer, "Daily", ticker)
    elif dailyData is None:
      errFigD = self.chartUtils.createErrorFigure(f"Could not load/process daily data for {ticker}.")
      self.dailyFig, self.dailyChartCanvas, self.dailyToolbar = self.displaySingleChart(errFigD, self.dailyChartFrameContainer, "Daily", ticker)
    else:
      errFigD = self.chartUtils.createErrorFigure(f"No daily data available for {ticker}.")
      self.dailyFig, self.dailyChartCanvas, self.dailyToolbar = self.displaySingleChart(errFigD, self.dailyChartFrameContainer, "Daily", ticker)

    if weeklyData is not None and not weeklyData.empty:
      self.weeklyFig = self.chartUtils.createStockChartFigure(weeklyData, ticker, "Weekly", figsize=weeklyFigsize)
      self.weeklyFig, self.weeklyChartCanvas, self.weeklyToolbar = self.displaySingleChart(self.weeklyFig, self.weeklyChartFrameContainer, "Weekly", ticker)
    elif weeklyData is None:
      errFigW = self.chartUtils.createErrorFigure(f"Could not load/process weekly data for {ticker}.")
      self.weeklyFig, self.weeklyChartCanvas, self.weeklyToolbar = self.displaySingleChart(errFigW, self.weeklyChartFrameContainer, "Weekly", ticker)
    else:
      errFigW = self.chartUtils.createErrorFigure(f"No weekly data available for {ticker}.")
      self.weeklyFig, self.weeklyChartCanvas, self.weeklyToolbar = self.displaySingleChart(errFigW, self.weeklyChartFrameContainer, "Weekly", ticker)

    if companyInfoValue:
      self.displayCompanyInfoDetails(companyInfoValue, ticker)
    else:
      self.infoText.delete('1.0', tk.END)
      self.infoText.insert(tk.END, f"No company information found for {ticker}.")

    self.statusBar.config(text=f"Displaying data for {ticker}")
    self.updateChartTitles()
  #--------------------------------------------------------------------------------------------------------------------------------
  def handleDataForCharting(self, dataFromThread: Dict[str, Any]):
    if dataFromThread:
      displayPayload = {
        'daily_data': dataFromThread.get('daily_data'),
        'weekly_data': dataFromThread.get('weekly_data'),
        'company_info': dataFromThread.get('company_info'),
        'ticker': dataFromThread.get('ticker'),
        'error': dataFromThread.get('error')
      }
      try:
        self.displayProcessedData(displayPayload)
      except Exception as eDisp:
        print(f"Critical error during displayProcessedData for {dataFromThread.get('ticker')}: {eDisp}")
        import traceback
        traceback.print_exc()
        self.displayError(f"Failed to display charts/info: {eDisp}", dataFromThread.get('ticker'))
    else:
      print("Received no data in handleDataForCharting.")
      self.displayError("Failed to retrieve data (empty response from background thread).")

    self.root.config(cursor="")
    self.statusBar.config(text=f"Ready. Last update for {self.currentTicker.get()}.")
  #--------------------------------------------------------------------------------------------------------------------------------
  def calculateDateRanges(self, yearsToDisplay: int) -> Tuple[datetime.date, datetime.date, pd.Timestamp]:
    if not (1 <= yearsToDisplay <= 20): yearsToDisplay = 2
    endDate = datetime.date.today()
    indicatorBufferDays = 300 + (365 if yearsToDisplay > 2 else 0)
    displayStartDate = endDate - datetime.timedelta(days=yearsToDisplay * 365)
    startDateForDataFetch = displayStartDate - datetime.timedelta(days=indicatorBufferDays)
    displayStartDateTimestamp = pd.Timestamp(displayStartDate)
    return startDateForDataFetch, endDate, displayStartDateTimestamp
  #--------------------------------------------------------------------------------------------------------------------------------
  def constructParquetFilePath(self, tickerSymbol: str, interval: str, dataDirName: str = "data") -> str:
    intervalSuffix = interval.replace('k','').replace('m','min')
    scriptDir = os.path.dirname(os.path.abspath(__file__))
    dataDirPath = os.path.join(scriptDir, dataDirName)
    os.makedirs(dataDirPath, exist_ok=True)
    return os.path.join(dataDirPath, f"{tickerSymbol}_{intervalSuffix}.parquet")
  #--------------------------------------------------------------------------------------------------------------------------------
  def loadLocalData(self, parquetFilePath: str, tickerSymbol: str, interval: str) -> Optional[pd.DataFrame]:
    if os.path.exists(parquetFilePath):
      try:
        print(f"Attempting to load {interval} data for {tickerSymbol} from {parquetFilePath}")
        localDfCandidate = pd.read_parquet(parquetFilePath)
        if not localDfCandidate.empty and isinstance(localDfCandidate.index, pd.DatetimeIndex):
          if localDfCandidate.index.tz is not None:
            localDfCandidate.index = localDfCandidate.index.tz_convert(None)
          print(f"Successfully loaded {interval} data for {tickerSymbol} from local parquet.")
          return localDfCandidate
        else:
          print(f"Local parquet file for {tickerSymbol} ({interval}) was empty or had invalid index.")
      except Exception as e:
        print(f"Error reading parquet file {parquetFilePath} for {tickerSymbol} ({interval}): {e}.")
    return None
  #--------------------------------------------------------------------------------------------------------------------------------
  def determineFetchParameters(self, localData: Optional[pd.DataFrame],
                                startDateParam: datetime.date, endDateParam: datetime.date, interval: str,
                                tickerSymbol: str) -> Tuple[Optional[datetime.date], pd.DataFrame]:
    fetchStartDate = None
    baseDfForMerge = pd.DataFrame()

    if localData is not None and not localData.empty:
      baseDfForMerge = localData.copy()
      firstTimestampInLocal = localData.index.min()
      lastTimestampInLocal = localData.index.max()

      if firstTimestampInLocal.date() > startDateParam:
        print(f"Local data for {tickerSymbol} ({interval}) starts at {firstTimestampInLocal.date()}, "
              f"but required start is {startDateParam}. Planning fetch from {startDateParam} to cover missing history.")
        fetchStartDate = startDateParam

      prospectiveFetchStartDateForNewerData = None
      if interval == '1d':
        if lastTimestampInLocal.date() == endDateParam:
          print(f"Local daily data for {tickerSymbol} includes today ({endDateParam}). Re-fetching today for latest EOD.")
          prospectiveFetchStartDateForNewerData = endDateParam
          baseDfForMerge = localData[localData.index.date < endDateParam].copy()
        elif lastTimestampInLocal.date() < endDateParam:
          prospectiveFetchStartDateForNewerData = lastTimestampInLocal.date() + datetime.timedelta(days=1)
          print(f"Local daily data for {tickerSymbol} ends at {lastTimestampInLocal.date()}. Fetching newer data from {prospectiveFetchStartDateForNewerData}.")
      elif interval == '1wk':
        todayIsoYear, todayIsoWeek, _ = datetime.date.today().isocalendar()
        lastLocalIsoYear, lastLocalIsoWeek, _ = lastTimestampInLocal.date().isocalendar()
        if lastLocalIsoYear < todayIsoYear or \
          (lastLocalIsoYear == todayIsoYear and lastLocalIsoWeek < todayIsoWeek):
          prospectiveFetchStartDateForNewerData = lastTimestampInLocal.date() + datetime.timedelta(days=1)
          print(f"Local weekly data for {tickerSymbol} (week {lastLocalIsoWeek}/{lastLocalIsoYear}) is older. Fetching newer data from {prospectiveFetchStartDateForNewerData}.")
        else:
          print(f"Local weekly data for {tickerSymbol} (week {lastLocalIsoWeek}/{lastLocalIsoYear}) is current. No fetch for newer data needed.")

      if prospectiveFetchStartDateForNewerData:
        if fetchStartDate:
          fetchStartDate = min(fetchStartDate, prospectiveFetchStartDateForNewerData)
        else:
          fetchStartDate = prospectiveFetchStartDateForNewerData
    else:
      fetchStartDate = startDateParam
      print(f"No valid local data for {tickerSymbol} ({interval}). Planning full fetch from {fetchStartDate} to {endDateParam}.")
      baseDfForMerge = pd.DataFrame()

    return fetchStartDate, baseDfForMerge
  #--------------------------------------------------------------------------------------------------------------------------------
  def fetchAndMergeData(self, tickerSymbol: str, fetchStartDate: Optional[datetime.date], endDateParam: datetime.date,
                        interval: str, baseDfForMerge: pd.DataFrame) -> pd.DataFrame:
    currentData = baseDfForMerge.copy()

    if fetchStartDate and fetchStartDate <= endDateParam:
      print(f"Fetching {interval} data for {tickerSymbol} from {fetchStartDate} to {endDateParam}")
      newData = self.dataProvider.getHistoricalData(tickerSymbol, fetchStartDate, endDateParam, interval=interval)

      if not newData.empty:
        print(f"Fetched {len(newData)} new rows for {tickerSymbol} ({interval}).")
        mergedDf = pd.concat([baseDfForMerge, newData])
        mergedDf.sort_index(inplace=True)
        currentData = mergedDf[~mergedDf.index.duplicated(keep='last')]
        print(f"Data for {tickerSymbol} ({interval}) was updated/merged.")
      else:
        print(f"No new data fetched from yfinance for {tickerSymbol} ({interval}) (tried from {fetchStartDate}).")
    elif not baseDfForMerge.empty:
      print(f"No fetch required for {tickerSymbol} ({interval}). Using existing local data provided as baseDfForMerge.")

    return currentData
  #--------------------------------------------------------------------------------------------------------------------------------
  def saveDataIfNeeded(self, dataToSave: pd.DataFrame, originalLocalData: Optional[pd.DataFrame],
                      parquetFilePath: str, tickerSymbol: str, interval: str) -> None:
    if dataToSave.empty:
      print(f"Final DataFrame for {tickerSymbol} ({interval}) is empty. Nothing to save to parquet.")
      return

    if not isinstance(dataToSave.index, pd.DatetimeIndex):
      try:
        dataToSave.index = pd.to_datetime(dataToSave.index)
      except Exception as e:
        print(f"Error converting dataToSave index to DatetimeIndex for {tickerSymbol} ({interval}): {e}. Skipping save.")
        return
    if dataToSave.index.tz is not None:
      dataToSave.index = dataToSave.index.tz_convert(None)

    shouldSave = False
    reasonForSave = []

    if originalLocalData is None or originalLocalData.empty:
      shouldSave = True
      reasonForSave.append("No valid original local data existed.")
    else:
      comparableOriginalLocalData = originalLocalData.copy()
      if not isinstance(comparableOriginalLocalData.index, pd.DatetimeIndex):
        try:
          comparableOriginalLocalData.index = pd.to_datetime(comparableOriginalLocalData.index)
        except Exception:
          shouldSave = True
          reasonForSave.append("Original local data had a non-convertible index.")
      
      if not shouldSave and comparableOriginalLocalData.index.tz is not None:
        comparableOriginalLocalData.index = comparableOriginalLocalData.index.tz_convert(None)

      if not shouldSave:
        if dataToSave.index.min() < comparableOriginalLocalData.index.min():
          shouldSave = True
          reasonForSave.append(f"Data now starts earlier ({dataToSave.index.min()} vs {comparableOriginalLocalData.index.min()}).")

        if dataToSave.index.max() > comparableOriginalLocalData.index.max():
          shouldSave = True
          reasonForSave.append(f"Data now ends later ({dataToSave.index.max()} vs {comparableOriginalLocalData.index.max()}).")

        if not shouldSave and not dataToSave.equals(comparableOriginalLocalData):
          shouldSave = True
          reasonForSave.append("Content has changed.")

    if shouldSave:
      finalReason = " ".join(reasonForSave)
      print(f"Saving data for {tickerSymbol} ({interval}). Reason(s): {finalReason}")
      try:
        dataToSave.to_parquet(parquetFilePath, engine='pyarrow', index=True)
        print(f"Saved/Updated data for {tickerSymbol} ({interval}) to {parquetFilePath}")
      except Exception as e:
        print(f"Error saving data for {tickerSymbol} ({interval}) to parquet {parquetFilePath}: {e}")
    else:
      print(f"Data for {tickerSymbol} ({interval}) is effectively unchanged compared to local data. No save needed.")
  #--------------------------------------------------------------------------------------------------------------------------------
  def applyIndicatorsAndFilterData(self, dataFrame: pd.DataFrame, displayStartDateTs: pd.Timestamp,
                                    tickerSymbol: str, interval: str) -> Optional[pd.DataFrame]:
    if dataFrame.empty:
      print(f"No data to process for indicators for {tickerSymbol} ({interval}).")
      return None

    dfWithIndicators = dataFrame.copy()
    if isinstance(dfWithIndicators.index, pd.DatetimeIndex) and dfWithIndicators.index.tz is not None:
      dfWithIndicators.index = dfWithIndicators.index.tz_convert(None)

    dfWithIndicators = self.indicatorCalc.addSmas(dfWithIndicators)
    dfWithIndicators = self.indicatorCalc.addBollingerBands(dfWithIndicators)
    dfWithIndicators = self.indicatorCalc.addMacd(dfWithIndicators)
    dfWithIndicators = self.indicatorCalc.addRsi(dfWithIndicators)
    dfWithIndicators = self.indicatorCalc.addStochastic(dfWithIndicators)

    displayStartDateTsNaive = displayStartDateTs.tz_localize(None) if dfWithIndicators.index.tz is None and displayStartDateTs.tz is not None else displayStartDateTs
    
    dfWithIndicators.sort_index(inplace=True)
    try:
      filteredDf = dfWithIndicators[dfWithIndicators.index >= displayStartDateTsNaive]
      if filteredDf.empty:
        print(f"Warning: After filtering by display start date {displayStartDateTsNaive}, DataFrame for {tickerSymbol} ({interval}) is empty.")
        return pd.DataFrame(columns=dfWithIndicators.columns, index=pd.to_datetime([]))
      return filteredDf
    except TypeError as te:
      print(f"TypeError during date filtering for {tickerSymbol} ({interval}): {te}")
      return pd.DataFrame(columns=dfWithIndicators.columns, index=pd.to_datetime([]))
    except Exception as eFilter:
      print(f"Unexpected error during date filtering for {tickerSymbol} ({interval}): {eFilter}")
      return pd.DataFrame(columns=dfWithIndicators.columns, index=pd.to_datetime([]))
  #--------------------------------------------------------------------------------------------------------------------------------
  def fetchAndProcessIntervalData(self, tickerSymbol: str, startDateParam: datetime.date, endDateParam: datetime.date, displayStartDateTs: pd.Timestamp, interval: str) -> Optional[pd.DataFrame]:
    parquetFilePath = self.constructParquetFilePath(tickerSymbol, interval)
    originalLocalDf = self.loadLocalData(parquetFilePath, tickerSymbol, interval)

    fetchStartDate, baseDfForMerge = self.determineFetchParameters(originalLocalDf, startDateParam, endDateParam, interval, tickerSymbol)
    
    finalDfToProcessAndSave = self.fetchAndMergeData(tickerSymbol, fetchStartDate, endDateParam, interval, baseDfForMerge)

    if (finalDfToProcessAndSave is None or finalDfToProcessAndSave.empty) and (startDateParam <= endDateParam):
      print(f"Performing full fetch for {interval} data: {tickerSymbol} from {startDateParam} to {endDateParam} as a fallback.")
      fallbackData = self.dataProvider.getHistoricalData(tickerSymbol, startDateParam, endDateParam, interval=interval)
      if fallbackData is not None and not fallbackData.empty:
        finalDfToProcessAndSave = fallbackData
        print(f"Full fetch successful for {tickerSymbol} ({interval}).")
      else:
        finalDfToProcessAndSave = pd.DataFrame() if finalDfToProcessAndSave is None else finalDfToProcessAndSave
        print(f"Full fetch for {tickerSymbol} ({interval}) resulted in an empty or None DataFrame.")
    elif finalDfToProcessAndSave is None:
      finalDfToProcessAndSave = pd.DataFrame()

    self.saveDataIfNeeded(finalDfToProcessAndSave, originalLocalDf, parquetFilePath, tickerSymbol, interval)

    return self.applyIndicatorsAndFilterData(finalDfToProcessAndSave, displayStartDateTs, tickerSymbol, interval)
  #--------------------------------------------------------------------------------------------------------------------------------
  def processDataInBackground(self, tickerSymbol: str, yearsToDisplay: int) -> Dict[str, Any]:
    try:
      startDate, endDate, displayStartTs = self.calculateDateRanges(yearsToDisplay)

      dailyDf = self.fetchAndProcessIntervalData(tickerSymbol, startDate, endDate, displayStartTs, '1d')
      weeklyDf = self.fetchAndProcessIntervalData(tickerSymbol, startDate, endDate, displayStartTs, '1wk')
      companyInfoValue = self.dataProvider.getCompanyInfo(tickerSymbol)

      if isinstance(companyInfoValue, dict) and companyInfoValue.get("error"):
        print(f"Company info error for {tickerSymbol}: {companyInfoValue.get('error')}")

      allDataFailed = True
      if dailyDf is not None and not dailyDf.empty: allDataFailed = False
      if weeklyDf is not None and not weeklyDf.empty: allDataFailed = False
      if isinstance(companyInfoValue, dict) and not companyInfoValue.get("error"): allDataFailed = False

      payload = {
        'daily_data': dailyDf, 'weekly_data': weeklyDf,
        'company_info': companyInfoValue, 'ticker': tickerSymbol, 'error': None
      }

      if allDataFailed:
        mainErrorMsg = "Failed to retrieve any data"
        if isinstance(companyInfoValue, dict) and companyInfoValue.get("error"):
          mainErrorMsg = companyInfoValue.get("error")
        elif dailyDf is None and weeklyDf is None :
          mainErrorMsg = f"Failed to fetch historical data for {tickerSymbol}."
        payload['error'] = mainErrorMsg + f" for {tickerSymbol}."
        print(f"Error in processDataInBackground for {tickerSymbol}: {payload['error']}")
      return payload

    except Exception as e:
      print(f"Critical error in background processing for {tickerSymbol}: {e}")
      import traceback
      traceback.print_exc()
      return {
        'daily_data': pd.DataFrame(), 'weekly_data': pd.DataFrame(),
        'company_info': {"error": f"Critical background error: {e}"},
        'ticker': tickerSymbol,
        'error': f"A critical error occurred while processing data for {tickerSymbol}: {e}"
      }
  #--------------------------------------------------------------------------------------------------------------------------------
  def updateUiForLoading(self, ticker: str):
    self.clearPreviousCharts()
    self.infoText.delete('1.0', tk.END)
    self.infoText.insert(tk.END, f"Loading data for {ticker}...")
    self.statusBar.config(text=f"Loading {ticker}...")
    self.root.config(cursor="watch")

    loadingFigDaily = self.chartUtils.createErrorFigure(f"Loading Daily Chart for {ticker}...")
    self.dailyFig, self.dailyChartCanvas, self.dailyToolbar = self.displaySingleChart(loadingFigDaily, self.dailyChartFrameContainer, "Daily", ticker)

    loadingFigWeekly = self.chartUtils.createErrorFigure(f"Loading Weekly Chart for {ticker}...")
    self.weeklyFig, self.weeklyChartCanvas, self.weeklyToolbar = self.displaySingleChart(loadingFigWeekly, self.weeklyChartFrameContainer, "Weekly", ticker)
  #--------------------------------------------------------------------------------------------------------------------------------
  def loadStockData(self, ticker: Optional[str] = None):
    if ticker is None:
      selectedIndices = self.tickerListBox.curselection()
      if not selectedIndices:
        self.statusBar.config(text="No ticker selected.")
        return
      ticker = self.tickerListBox.get(selectedIndices[0])

    self.currentTicker.set(ticker)
    self.updateUiForLoading(ticker)

    yearsToDisplayValue = self.displayYearsVar.get()
    if not (1 <= yearsToDisplayValue <= 20):
      yearsToDisplayValue = 2
      self.displayYearsVar.set(2)

    thread = threading.Thread(target=lambda: self.root.after(0, self.handleDataForCharting, self.processDataInBackground(ticker, yearsToDisplayValue)), daemon=True)
    thread.start()
#--------------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
  root = tk.Tk()
  app = StockAnalyzerApp(root)
  root.mainloop()