# See also https://algotrading101.com/learn/interactive-brokers-python-api-native-guide/
# https://interactivebrokers.github.io/tws-api/client_wrapper.html
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import xml.etree.ElementTree as ET  # Import für XML-Verarbeitung

import threading
import time
import pandas as pd
import sys

import config 
import globalsSa 

global app, condition_object
global gCurrentTicker
gCurrentTicker   = "" 
app              = None
ASK, OPEN, CLOSE, VOLA = 2, 14, 9, 23

#----------------------------------------------------------------------------------------------------------------------  
#--------------------------------------------------------------------------------------------------------------------------------
class Database:
  mInfo = {}
  @staticmethod
  def storeInfo(id, news, type="news"):
    if type == "account":
      data = news
      key, value = data.split(":")
      Database.mInfo[key] = value
    else:  
      Database.mInfo[id] = news

  @staticmethod
  def getInfo():
    return Database.mInfo
  
  @staticmethod
  def clear():
    Database.mInfo = {}
  
  @staticmethod
  def print():
    #print("-" * 80)
    for key, value in Database.mInfo.items():    
      #print("-" * 80)
      value = value.replace(",",";")
      print(F"{key}:{value}")  
#-----------------------------------------------------------------------------  
#-----------------------------------------------------------------------------  
class IbApi(EWrapper, EClient):
  app =  None
  ibSync = None
  REQ_ID, REQ_ID_NEWS, REQ_ID_INFO, REQ_ID_FUNDAMENTAL = 1, 2, 3, 4
  def __init__(self):
    EClient.__init__(self, self)
    self.clearData() 
    self.opened = False
    self.info = []
    self.cnt = 0
    self.portofolio = False
    self.data_received_event = threading.Event()
  #----------------------------------------------------  
  @staticmethod
  def run_loop(app):
    try:
      app.run()
    except Exception as e:
      print(f"Error in run_loop: {e}")
    finally:
      print("Event loop stopped.")
  #----------------------------------------------------  
  def __del__(self):
    self.close()
  #----------------------------------------------------
  # inherited class method overrides  
  #----------------------------------------------------  
  def error(self, reqId, errorCode: int, errorString: str, advancedOrderRejectJson = ""):
    #super().error(reqId, errorCode, errorString, advancedOrderRejectJson)
    if reqId != -1:
      global gCurrentTicker
      if advancedOrderRejectJson:
        print(f"Error:{errorCode}, Id:{reqId}, Msg:{errorString}, AdvancedOrderRejectJson:{advancedOrderRejectJson}")
      else:
        print(f"Error:{errorCode}, Id:{reqId}, ticker:{gCurrentTicker}, Msg:{errorString}")
      #self.data_received_event.set()
    else:
      #print("Error:", errorCode, "Id:", reqId, "Msg:", errorString, "AdvancedOrderRejectJson:", advancedOrderRejectJson)
      pass
  #----------------------------------------------------      
  def tickPrice(self, reqId, tickType, price, attrib):
    if tickType == ASK and reqId == IbApi.REQ_ID:
      print('The current ask price is: ', price)
  #----------------------------------------------------  
  def historicalData(self, reqId, bar):
    if reqId == IbApi.REQ_ID:
      a = [bar.date, bar.open, bar.close, bar.low, bar.high]
      self.data.append(a)
      #print(f'Time,Open,Close,Low,High: {a}')
  #----------------------------------------------------  
  def historicalDataEnd(self, reqId: int, start: str, end: str):
    if reqId == IbApi.REQ_ID:
      super().historicalDataEnd(reqId, start, end)
      self.data_received_event.set()
  #----------------------------------------------------  
  def tickNews(self, reqId: int, timeStamp: int, providerCode: str, articleId: str, headline: str, extraData: str):
    if reqId == IbApi.REQ_ID_NEWS:
      #self.news.append(f"reqId:f{reqId}; TimeStamp:{timeStamp}; ProviderCode:{providerCode}; ArticleId:{articleId}; Headline:{headline}; ExtraData:{extraData}")
      self.info.append(f"ProviderCode:{providerCode};  Headline:{headline}; {extraData}\n")
      self.data_received_event.set()
  #----------------------------------------------------  
  def accountSummary(self, reqId, account, tag, value, currency):
    if reqId == IbApi.REQ_ID_INFO:
      if len(self.info) == 0:
        self.info.append(f"Account:{account}")
        #self.info.append(f"Currency:{currency}")
      self.info.append(f"{tag}:{value}")
    #self.data_received_event.set()
  #----------------------------------------------------  
  def accountSummaryEnd(self, reqId: int):
    if reqId == IbApi.REQ_ID_INFO:
      self.data_received_event.set()
  #----------------------------------------------------  
  def updateAccountValue(self, key: str, val: str, currency: str,accountName: str):
    if len(self.info) == 0:
      x = f"Key,Value,Currency" #, AccountName:{accountName}"
      self.info.append(x)
    x = f"{key},{val},{currency}" #, AccountName:{accountName}"
    self.info.append(x)
  #----------------------------------------------------  
  def updatePortfolio(self, contract: Contract, position,marketPrice: float, marketValue: float, averageCost: float, unrealizedPNL: float, realizedPNL: float, accountName: str):  
    if not self.portofolio:
      self.portofolio = True
      x = "Symbol,SecType,Exchange,Position,MarketPrice,MarketValue,AverageCost,UnrealizedPNL,RealizedPNL"
      self.info.append(x)
    x = f"{contract.symbol},{contract.secType},{contract.exchange},{position},{marketPrice},{marketValue},{averageCost},{unrealizedPNL},{realizedPNL}"
    self.info.append(x)
  #----------------------------------------------------  
  def accountDownloadEnd(self, accountName: str):
    self.portofolio = False
    self.data_received_event.set()
  #----------------------------------------------------  
  def scannerParameters(self, xml: str):
    open('log/scanner.xml', 'w').write(xml)
    self.data_received_event.set()
  #----------------------------------------------------  
  def scannerData(self, reqId: int, rank: int, contractDetails, distance: str, benchmark: str, projection: str, legsStr: str):
    x = f"ScannerData. ReqId: {reqId}, Contract: {contractDetails.contract}, Rank: {rank}, Distance: {distance}, Benchmark: {benchmark}, Projection: {projection}, Legs: {legsStr}"
    self.info.append(x)
    self.data_received_event.set()
  #----------------------------------------------------  
  def newsProviders(self, newsProviders):
    x = f"NewsProviders: {newsProviders}"
    self.info.append(x)
    self.data_received_event.set()
  #----------------------------------------------------  
  # Inherite and overwrite fundamentalData() function in EWrapper
  if 0:
    def fundamentalData(self, reqId: int, data: str):
      if reqId == IbApi.REQ_ID_FUNDAMENTAL:
        super().fundamentalData(reqId, data)
        print("FundamentalData Returned. ReqId: {}, XML Data: {}".format(
              reqId, data))
  def fundamentalData(self, reqId: int, data: str):
    if reqId == IbApi.REQ_ID_FUNDAMENTAL:
      #print(f"FundamentalData Returned. ReqId: {reqId}, XML Data: {data}")
      self.info.append(data)  # Speichere die empfangenen Daten
      self.data_received_event.set()  # Event auslösen      
  #----------------------------------------------------  
  #----------------------------------------------------  
  def open(self):
    if self.opened == False:
      self.opened = True
      self.connect('127.0.0.1', config.port, 123)
      #Start the socket in a thread
      threading.Thread(target=IbApi.run_loop, args=(self,), daemon=True).start()
      time.sleep(1) # time for connection to server
      if not self.isConnected():
        raise globalsSa.CustomError("Ibkr connection failed.")
  #----------------------------------------------------  
  def close(self):
    if self.opened:
      print("Closing IBKR connection...")
      self.opened = False
      self.done = True  # Event-Loop stoppen
      self.disconnect()  # Verbindung trennen
      print("IBKR connection closed.") 
  #----------------------------------------------------  
  def isOpen(self):
    return self.opened
  #----------------------------------------------------  
  def clearData(self):
    self.data = []
  #----------------------------------------------------  
  def clearInfo(self):
    self.info = []
  #----------------------------------------------------  
  # thread handling    
  #----------------------------------------------------  
  def waitAndReturnInfo(self):
    if not self.data_received_event.wait(timeout=10):  # Timeout von 10 Sekunden
      print("Timeout waiting for data.")
      return None
    self.data_received_event.clear()
    ret = self.info
    self.clearInfo()
    return ret
  #----------------------------------------------------  
  def get(self, ticker, interval, period='3 Y'):
    global gCurrentTicker
    gCurrentTicker = ticker
    #Create contract object
    contract = Contract()
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    if '^' in ticker:
      contract.secType = 'IND'
      contract.exchange = 'CBOE'
      ticker = ticker[1:]
    else:
      contract.primaryExchange = "ISLAND" # for NASDAQ
    contract.symbol   = ticker
    contract.currency = 'USD'
    #Request Market Data
    self.reqHistoricalData(IbApi.REQ_ID, contract, '', period, interval, 'TRADES', 1, 2, False, [])
    #self.reqHistoricalData(IbApi.REQ_ID, contract, '3 Y', interval, 'TRADES', 1, 2, False, [])
    self.data_received_event.wait()
    self.data_received_event.clear()
    d = pd.DataFrame(self.data, columns=['DateTime', 'Open', 'Close', 'Low', 'High'])
    self.clearData()
    if d.empty:                                   raise globalsSa.CustomError("No Data")
    d['DateTime'] = pd.to_datetime(d['DateTime']) 
    d = d.set_index(['DateTime'])
    return d
  #----------------------------------------------------  
  def getAccountInfo(self):
    self.reqAccountSummary(IbApi.REQ_ID_INFO, "All","$LEDGER")
    return self.waitAndReturnInfo()
  #----------------------------------------------------  
  def getAccountUpdates(self,subscribe:bool,acctCode:str):    
    self.reqAccountUpdates(subscribe,acctCode)
    if subscribe:
      return self.waitAndReturnInfo()
    return ""
  #----------------------------------------------------  
  def getScannerParameter(self):
    self.reqScannerParameters()
    return self.waitAndReturnInfo()
  #----------------------------------------------------
  def getSubscriptionData(self,scannerSubscription,filterTagvalues):
    self.reqScannerSubscription(7002, scannerSubscription, [], filterTagvalues)
    return self.waitAndReturnInfo()
  #----------------------------------------------------
  def stopSubscriptionData(self):
    self.cancelScannerSubscription(7003)
  #----------------------------------------------------  
  def getNews(self, ticker=None, interval=None):
    contract = Contract()
    contract.symbol   = f"BRFG:BRFG_ALL" #BroadTape All News
    contract.secType  = "NEWS"
    contract.exchange = "BRFG"
    #Request Market Data
    self.reqMktData(IbApi.REQ_ID_NEWS, contract, "mdoff,292", False, False, [])
    return self.waitAndReturnInfo()
  #----------------------------------------------------  
  def getNewsProviders(self):
    self.reqNewsProviders()
    return self.waitAndReturnInfo()
  #----------------------------------------------------  
  def getFundamentalData(self, ticker=None):
    contract = Contract()
    contract.symbol   = ticker
    contract.secType  = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    #Request Market Data
    self.reqFundamentalData(IbApi.REQ_ID_FUNDAMENTAL, contract, "RESC", [])
    return self.waitAndReturnInfo()
  #----------------------------------------------------  
  def getFairValue(self, ticker=None):
    contract = Contract()
    contract.symbol   = ticker
    contract.secType  = "STK"
    contract.exchange = "SMART"
    contract.currency = "USD"
    print(f"Requesting Fair Value for {ticker}...")
    self.reqMktData(IbApi.REQ_ID, contract, "236", False, False, [])
    return self.waitAndReturnInfo()
#-----------------------------------------------------------------------------  
#-----------------------------------------------------------------------------  
def open():
  IbApi.app = IbApi()
  IbApi.app.open()
#-----------------------------------------------------------------------------  
def isOpen():
  return not IbApi.app is None and IbApi.app.isOpen()
#-----------------------------------------------------------------------------  
def close():
  if not IbApi.app is None:
    IbApi.app.close()
#-----------------------------------------------------------------------------  
class Interval:
  interval = '1d'
  period = '1 M'
  #----------------------------------------------------
  @staticmethod
  def getIbkr():
    interval = Interval.interval
    if interval == '1d':    interval = '1 day'
    elif interval == '1wk': interval = '1 week'
    return interval
  #----------------------------------------------------
  @staticmethod
  def get():
    return Interval.interval
  #----------------------------------------------------
  @staticmethod
  def set(interval):
    Interval.interval = interval  
  #----------------------------------------------------
  @staticmethod
  def getPeriod():
    return Interval.period
  #----------------------------------------------------
  @staticmethod
  def setPeriod(value):
    Interval.period = value
#-----------------------------------------------------------------------------  
def get(ticker):
  if isOpen():
    df = IbApi.app.get(ticker, Interval.getIbkr(), Interval.getPeriod())
    return df
  else: 
    raise globalsSa.CustomError("IbApi not opend")
#-----------------------------------------------------------------------------  
def getAccountInfo():
  if IbApi.app is None or not IbApi.app.isOpen():  raise globalsSa.CustomError("Ibkr not opened")
  return IbApi.app.getAccountInfo()
#-----------------------------------------------------------------------------  
def getAccountUpdate():
  if IbApi.app is None or not IbApi.app.isOpen():  raise globalsSa.CustomError("Ibkr not opened")
  return IbApi.app.getAccountUpdates(True, config.account)
#-----------------------------------------------------------------------------  
def stopAccountUpdate():
  if IbApi.app is None or not IbApi.app.isOpen():  raise globalsSa.CustomError("Ibkr not opened")
  return IbApi.app.getAccountUpdates(False, config.account)
#-----------------------------------------------------------------------------  
def getNews(ticker):
  if IbApi.app is None or not IbApi.app.isOpen():  raise globalsSa.CustomError("Ibkr not opened")
  return IbApi.app.getNews(ticker)
#-----------------------------------------------------------------------------  
def getFundamentalData(ticker):
  if IbApi.app is None or not IbApi.app.isOpen():  raise globalsSa.CustomError("Ibkr not opened")
  return IbApi.app.getFundamentalData(ticker)
#----------------------------------------------------------------------------- 
def calculateFairValue(xmlData):
  root = ET.fromstring(xmlData)
  weightedMedianSum = 0
  weightedMeanSum = 0
  totalMedianWeight = 0
  totalMeanWeight = 0

  # Suche nach Konsensschätzungen (Median und Mean)
  for consEstimate in root.findall(".//ConsEstimate"):
    estimateType = consEstimate.get("type")
    consValue = consEstimate.find(".//ConsValue[@dateType='CURR']")
    numOfEst = consEstimate.find(".//ConsValue[@dateType='NumOfEst']")  # Anzahl der Schätzungen

    if consValue is not None and consValue.text:
      try:
        value = float(consValue.text)
        weight = float(numOfEst.text) if numOfEst is not None and numOfEst.text else 1  # Standardgewichtung 1
        print(f"Extracted value: {value} (Type: {estimateType}, Weight: {weight})")  # Debugging-Ausgabe

        if estimateType == "Median":
          weightedMedianSum += value * weight
          totalMedianWeight += weight
        elif estimateType == "Mean":
          weightedMeanSum += value * weight
          totalMeanWeight += weight
      except ValueError:
        print(f"Invalid value encountered: {consValue.text}")

  # Berechnung des gewichteten Fair Value
  fairValueMedian = weightedMedianSum / totalMedianWeight if totalMedianWeight > 0 else None
  fairValueMean = weightedMeanSum / totalMeanWeight if totalMeanWeight > 0 else None

  # Rückgabe des berechneten Fair Value
  if fairValueMedian is not None and fairValueMean is not None:
    return (fairValueMedian + fairValueMean) / 2  # Durchschnitt aus Median und Mean
  elif fairValueMedian is not None:
    return fairValueMedian
  elif fairValueMean is not None:
    return fairValueMean
  else:
    return None
#------------------------------------------------------------------------------#-----------------------------------------------------------------------------  
#-----------------------------------------------------------------------------  
def addArguments(parser):
  parser.add_argument(
      "--ibkr",
      action='store_true',
      help="Load data IBKR."
  )
  parser.add_argument(
      "--news",
      action='store_true',
      help="Load news for ticker."
  )
  parser.add_argument(
      "--account",
      action='store_true',
      help="Load account info."
  )
  parser.add_argument(
      "--portofolio",
      action='store_true',
      help="Load account info."
  )
#-----------------------------------------------------------------------------  
def evaluateAndExecute(opt):
  if opt.ibkr:
    open()
  if opt.news:
    for ticker in opt.tickers:
      print(f"{ticker}")
      for cnt, item in enumerate(getNews(ticker)):   
        Database.storeInfo(cnt, item)
      Database.print()
      Database.clear()
  if opt.account:
    for cnt, item in enumerate(getAccountInfo()):   
      Database.storeInfo(cnt, item, "account")
    Database.print()
    Database.clear()
  if opt.portofolio:
    for cnt, item in enumerate(getAccountUpdate()):   
      Database.storeInfo(cnt, item)
    stopAccountUpdate()
    Database.print()
    Database.clear()
    # todo ib.IbApi.app.getNewsProviders()
#-----------------------------------------------------------------------------  
#-----------------------------------------------------------------------------  
import sys,os
def isDebugging():
  return sys.gettrace() is not None

def parseXmlFile(filePath):
  # Überprüfen, ob die Datei existiert und nicht leer ist
  if not os.path.exists(filePath):
    raise FileNotFoundError(f"Die Datei {filePath} wurde nicht gefunden.")
  if os.path.getsize(filePath) == 0:
    raise ValueError(f"Die Datei {filePath} ist leer.")

  # Versuche, die Datei zu parsen
  try:
    tree = ET.parse(filePath)
    return tree.getroot()
  except ET.ParseError as e:
    raise ValueError(f"Fehler beim Parsen der XML-Datei: {e}")

#----------------------------------------------------------------------------------------------------------------------     
if __name__ == "__main__":
  try:
    symbol = 'jnj'
    open()
    df = get(symbol)
    print(f"Ticker: {symbol}")
    print(df)
  except Exception as err:
    print(f"Unexpected {err=}, {type(err)=}")
  finally:
    close()
