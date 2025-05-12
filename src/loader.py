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
def getProvider() -> MarketDataProvider:
  """Returns the market data provider based on the configuration."""
  # Check if Interactive Brokers is available
  if False:  # Placeholder for actual condition to check if IBKR is available
    return InteractiveBrokersProvider()
  else:
    return YFinanceProvider()
#--------------------------------------------------------------------------------------------------------------------------------
def fetchAndMergeData(tickerSymbol: str, fetchStartDate: Optional[datetime.date], endDateParam: datetime.date,
                      interval: str, baseDfForMerge: pd.DataFrame) -> pd.DataFrame:
  currentData = baseDfForMerge.copy()
  if fetchStartDate and fetchStartDate <= endDateParam:
    print(f"Fetching {interval} data for {tickerSymbol} from {fetchStartDate} to {endDateParam}")
    newData = getProvider().getHistoricalData(tickerSymbol, fetchStartDate, endDateParam, interval=interval)
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
def constructParquetFilePath(tickerSymbol: str, interval: str, dataDirName: str = "data") -> str:
  intervalSuffix = interval.replace('k','').replace('m','min')
  scriptDir = os.path.dirname(os.path.abspath(__file__))
  dataDirPath = os.path.join(scriptDir, dataDirName)
  os.makedirs(dataDirPath, exist_ok=True)
  return os.path.join(dataDirPath, f"{tickerSymbol}_{intervalSuffix}.parquet")
#--------------------------------------------------------------------------------------------------------------------------------
def loadLocalData(parquetFilePath: str, tickerSymbol: str, interval: str) -> Optional[pd.DataFrame]:
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
def determineFetchParameters(localData: Optional[pd.DataFrame],
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
  