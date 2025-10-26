import yfinance as yf
import pandas as pd
import datetime
import requests
import os
from typing import List, Dict, Any, Optional, Tuple
#--------------------------------------------------------------------------------------------------------------------------------
# This is needed to prevent 'Too Many Requests. Rate limited. Try after a while.'
# see https://github.com/ranaroussi/yfinance/issues/2422
from curl_cffi import requests
global session
session = None
#------------------------------------------------------------------------------
import globalsSa
try:
  import IbkrTws as ib
  globalsSa.HAS_IBKR = True
except:
  globalsSa.HAS_IBKR = False
#--------------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------
class MarketDataProvider:
  #--------------------------------------------------------------------------------------------------------------------------------
  def getHistoricalData(self, tickerSymbol: str, startDate: datetime.date, endDate: datetime.date, interval: str ='1d') -> pd.DataFrame:
    raise NotImplementedError
  #--------------------------------------------------------------------------------------------------------------------------------
  def getCompanyInfo(self, tickerSymbol: str) -> Dict[str, Any]:
    raise NotImplementedError
#--------------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------
class YFinanceProvider(MarketDataProvider):
  def __init__(self):
    self.df = pd.DataFrame()
  #--------------------------------------------------------------------------------------------------------------------------------
  # class function
  @staticmethod
  def resampleMap() -> Dict[str, str]:
    return {
          'Open': 'first',
          'High': 'max',
          'Low': 'min',
          'Close': 'last',
          'Volume': 'sum'
      }
  #--------------------------------------------------------------------------------------------------------------------------------
  def isInvalid(self) -> bool:
    return self.df.empty or pd.Timestamp.now(tz='UTC').weekday() >= 5
  #--------------------------------------------------------------------------------------------------------------------------------
  def getTimeDifference(self) -> int:
    currentTime = pd.Timestamp.now(tz='UTC')
    lastDataTime = self.df.index[-1]
    lastDataTime = lastDataTime.tz_localize('UTC') if lastDataTime.tzinfo is None else lastDataTime.tz_convert('UTC')
    return (currentTime - lastDataTime)
  #--------------------------------------------------------------------------------------------------------------------------------
  def getTimeDifferenceInMinutes(self) -> int:
    return self.getTimeDifference().total_seconds() / 60
  #--------------------------------------------------------------------------------------------------------------------------------
  def getTimeDifferenceInDays(self) -> int:
    return self.getTimeDifference().days
  #--------------------------------------------------------------------------------------------------------------------------------
  def handleCurrentDay(self, ticker: yf.Ticker, interval: str) -> pd.DataFrame:
    # if empty or current day is saturday or sunday, no need to update
    if self.isInvalid():
      return
    if self.getTimeDifferenceInMinutes() > 1 and 'Close' in self.df.columns:
      dfNew = ticker.history(period='1d', interval='1m', auto_adjust=True, prepost=False)      
      dfNewD = dfNew.resample('D').agg(YFinanceProvider.resampleMap()) # resample minute to daily
      self.df = pd.concat([self.df, dfNewD])
      self.df = self.df[~self.df.index.duplicated(keep='last')]
  #--------------------------------------------------------------------------------------------------------------------------------
  def handleCurrentWeek(self, ticker: yf.Ticker) -> pd.DataFrame:
    if self.isInvalid():
      return
    # we have the days already in self.df, so just resample to weeks
    self.df = self.df.resample('W').agg(YFinanceProvider.resampleMap()) # resample daily to weekly
  #--------------------------------------------------------------------------------------------------------------------------------
  def getHistoricalData(self, tickerSymbol: str, startDate: datetime.date, endDate: datetime.date, interval: str ='1d') -> pd.DataFrame:
    global session
    if session is None:
      session = requests.Session(impersonate="chrome")
    ticker = yf.Ticker(tickerSymbol, session=session)
    self.df = ticker.history(start=startDate, end=endDate, interval=interval, auto_adjust=True, prepost=False)
    if self.df.empty:
      return pd.DataFrame()
    if "d" in interval:
      self.handleCurrentDay(ticker, interval)
    if "w" in interval:
      self.handleCurrentWeek(ticker)
    self.df.rename(columns={col: col.capitalize() for col in self.df.columns if col in ['open', 'high', 'low', 'close', 'volume']}, inplace=True)
    if isinstance(self.df.index, pd.DatetimeIndex):
      if self.df.index.tz is not None:
        self.df.index = self.df.index.tz_convert(None)
    return self.df
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
#--------------------------------------------------------------------------------------------------------------------------------
class InteractiveBrokersProvider(MarketDataProvider):
  def __init__(self):
    try:
      if not ib.isOpen():
        ib.open()
    except Exception as e:
      raise Exception("Interactive Brokers API is not enabled. Please check your configuration.")
  #--------------------------------------------------------------------------------------------------------------------------------
  def getHistoricalData(self, tickerSymbol: str, startDate: datetime.date, endDate: datetime.date, interval: str ='1d') -> pd.DataFrame:
    if ib.isOpen():
      ib.Interval.set(interval)
      # calculate period
      period = endDate - startDate
      strPeriod = f""
      if period.days < 5:
        days = max(period.days, 1)  
        strPeriod = f"{days} D"
      elif period.days < 28:
        strPeriod = f"{(period.days+6) // 7} W"
      elif period.days < 365: # less than a year
        strPeriod = f"{(period.days+29) // 30} M" 
      else: # more than a year
        strPeriod = f"{(period.days+364) // 365} Y"
      ib.Interval.setPeriod(strPeriod)  
      try:
        df = ib.get(tickerSymbol)
      except Exception:
        raise Exception("No data returned from Interactive Brokers API.")
    if df.empty:
      return pd.DataFrame()
    df.rename(columns={col: col.capitalize() for col in df.columns if col in ['open', 'high', 'low', 'close', 'volume']}, inplace=True)
    df.drop_duplicates().reset_index(drop=True) 
    if isinstance(df.index, pd.DatetimeIndex):
      if df.index.tz is not None:
        df.index = df.index.tz_convert(None)
    return df
  #--------------------------------------------------------------------------------------------------------------------------------
  def getCompanyInfo(self, tickerSymbol: str) -> Dict[str, Any]:
    #return ib.getFundamentalData(tickerSymbol)
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
def getProvider(useIbkr:bool = True) -> MarketDataProvider:
  """Returns the market data provider based on the configuration."""
  # Check if Interactive Brokers is available
  if globalsSa.HAS_IBKR and useIbkr:
    try:
      provider = InteractiveBrokersProvider()
      print("### Using Ibkr as data provider ###")
      return provider
    except Exception as e:
      globalsSa.HAS_IBKR = False
      return YFinanceProvider()
  else:
    return YFinanceProvider()    
#--------------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------
def determineFetchParameters(
    localData: Optional[pd.DataFrame],
    startDateParam: datetime.date, endDateParam: datetime.date, interval: str,
    tickerSymbol: str
  ) -> Tuple[Optional[datetime.date], pd.DataFrame]:
  """We either use the whole file or we load all data from the data provider"""
  if localData is None or localData.empty:
    print(f"No local data available for {tickerSymbol} ({interval}). Fetching from {startDateParam} to {endDateParam}.")
    return startDateParam, endDateParam
  
  #todo how to distinguish week start end end for filtering?
  fileMinDate = localData.index.min().date()
  fileMaxDate = localData.index.max().date()
  fetchStartDate = startDateParam
  fetchEndDate   = endDateParam

  if fileMinDate <= startDateParam:
    # use part from file
    if endDateParam < fileMaxDate:
      fetchStartDate, fetchEndDate =  None, None # this means all data from file
    else:
      # now only load data from internet which is still missing
      fetchStartDate = fileMaxDate # for safety load the last day in case the data is not from day end
      fetchEndDate = endDateParam

  return fetchStartDate, fetchEndDate
#------------------------------------------------------------------------------------------------------------------------------
def fetchAndProcessIntervalData(ticker: str, startDt: datetime.date, endDt: datetime.date, interval: str, useIbkr:bool) -> Optional[pd.DataFrame]:
  path = constructParquetFilePath(ticker, interval)
  dfFromFile = loadLocalData(path, ticker, interval)

  fetchStartDate, fetchEndDate = determineFetchParameters(dfFromFile, startDt, endDt, interval, ticker)
  if fetchStartDate is None:
    print(f"No fetch needed for {ticker} ({interval}). Using existing local data.")
    # filter data from file for startDt and endDt
    finalDf = dfFromFile.copy()
    finalDf = finalDf[finalDf.index.date >= startDt]
    finalDf = finalDf[finalDf.index.date <= endDt]  
  else:
    print(f"Fetch needed for {ticker} ({interval}). Using file from [{startDt}, {fetchStartDate}[. Fetching [{fetchStartDate}, {endDt}].")
    newData = getProvider(useIbkr).getHistoricalData(ticker, fetchStartDate, fetchEndDate, interval=interval)
    # now merge the data
    finalDf = pd.concat([dfFromFile, newData])
    finalDf = finalDf[~finalDf.index.duplicated(keep='last')]
    finalDf = finalDf.sort_index()      
    saveData(finalDf, path)
  return finalDf
#--------------------------------------------------------------------------------------------------------------------------------
def loadStockListFromFile(filename: str = "listStocks") -> List[str]:
  defaultStocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'VOW.DE', 'META', 'JPM', 'BTC-USD', 'ETH-USD']
  scriptDir = os.path.dirname(os.path.abspath(__file__))
  filePath = os.path.join(scriptDir, filename)
  stocks = None
  try:
    with open(filePath, 'r') as f:
      stocks = [line.strip() for line in f if line.strip()]
    if not stocks:
      print(f"Warning: Stock file '{filePath}' is empty. Using default stocks.")
  except FileNotFoundError:
    print(f"Warning: Stock file '{filePath}' not found. Using default stocks and creating file.")
  except Exception as e:
    print(f"Error reading stock file '{filePath}': {e}. Using default stocks.")
    stocks = defaultStocks
  finally:
    if not stocks:
      saveStockListToFile(defaultStocks, filename)
    return stocks if stocks else defaultStocks
#--------------------------------------------------------------------------------------------------------------------------------
# file handling
#--------------------------------------------------------------------------------------------------------------------------------
def constructParquetFilePath(tickerSymbol: str, interval: str, dataDirName: str = "data") -> str:
  intervalSuffix = interval.replace('k','').replace('m','min')
  scriptDir = os.path.dirname(os.path.abspath(__file__))
  dataDirPath = os.path.join(scriptDir, dataDirName)
  os.makedirs(dataDirPath, exist_ok=True)
  return os.path.join(dataDirPath, f"{tickerSymbol}_{intervalSuffix}.parquet")
#--------------------------------------------------------------------------------------------------------------------------------
def saveStockListToFile(tickers, filename: str = "listStocks"):
  scriptDir = os.path.dirname(os.path.abspath(__file__))
  filePath = os.path.join(scriptDir, filename)
  try:
    with open(filePath, 'w') as f:
      for ticker in tickers:
        f.write(f"{ticker}\n")
    print(f"Stocklist saved to '{filePath}'.")
  except Exception as e:
    print(f"Error saving stocklist to '{filePath}': {e}")
#--------------------------------------------------------------------------------------------------------------------------------
def loadLocalData(parquetFilePath: str, tickerSymbol: str, interval: str) -> Optional[pd.DataFrame]:
  if os.path.exists(parquetFilePath):
    try:
      print(f"Attempting to load {interval} data for {tickerSymbol} from {parquetFilePath}")
      localDfCandidate = pd.read_parquet(parquetFilePath)
      if not localDfCandidate.empty and isinstance(localDfCandidate.index, pd.DatetimeIndex):
        minDate = localDfCandidate.index.min()
        maxDate = localDfCandidate.index.max()
        if localDfCandidate.index.tz is not None:
          localDfCandidate.index = localDfCandidate.index.tz_convert(None)
        print(f"Successfully loaded from {minDate} to {maxDate} for {tickerSymbol} from local parquet.")
        return localDfCandidate
      else:
        print(f"Local parquet file for {tickerSymbol} ({interval}) was empty or had invalid index.")
    except Exception as e:
      print(f"Error reading parquet file {parquetFilePath} for {tickerSymbol} ({interval}): {e}.")
  return None
#--------------------------------------------------------------------------------------------------------------------------------
def saveData(dataToSave: pd.DataFrame, parquetFile: str) -> None:
  if dataToSave.empty:
    print(f"Final DataFrame for {parquetFile} is empty. Nothing to save to parquet.")
    return
  print(f"Saving data for {parquetFile}.")
  try:
    dataToSave.to_parquet(parquetFile, engine='pyarrow', index=True)
  except Exception as e:
    print(f"Error saving data to parquet {parquetFile}: {e}")
