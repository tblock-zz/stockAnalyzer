import tkinter as tk
from tkinter import ttk 
from typing import Optional, Dict, Any, Tuple
import datetime
#--------------------------------------------------------------------------------------------------------------------------------
class CompanyInfoDisplay:
  #------------------------------------------------------------------------------------------------------------------------------
  def __init__(self, parentPane: ttk.PanedWindow):
    self.parentPane = parentPane
    self.infoTextWidget: Optional[tk.Text] = None
    self.infoTextFrame: Optional[ttk.LabelFrame] = None
    self.setupUserInterface()
  #------------------------------------------------------------------------------------------------------------------------------
  def setupUserInterface(self):
    self.infoTextFrame = ttk.LabelFrame(self.parentPane, text="Company Information", padding=5)
    self.infoTextWidget = tk.Text(self.infoTextFrame, wrap=tk.WORD, height=10, relief=tk.FLAT, font=("Segoe UI", 9))
    self.infoTextWidget.pack(expand=True, fill=tk.BOTH, padx=5, pady=5)
    self.parentPane.add(self.infoTextFrame, weight=1)
  #------------------------------------------------------------------------------------------------------------------------------
  def clearContent(self):
    if self.infoTextWidget:
      self.infoTextWidget.delete('1.0', tk.END)
  #------------------------------------------------------------------------------------------------------------------------------
  def showMessage(self, message: str):
    self.clearContent()
    self.infoTextWidget.insert(tk.END, message)
  #------------------------------------------------------------------------------------------------------------------------------
  def showLoadingMessage(self, ticker: str):
    self.showMessage(f"Loading data for {ticker}...")
  #------------------------------------------------------------------------------------------------------------------------------
  def showError(self, errorMessage: str, ticker: Optional[str] = None):
    fullMessage = f"Error loading data for {ticker}:\n{errorMessage}" if ticker else errorMessage
    self.showMessage(fullMessage)
  #------------------------------------------------------------------------------------------------------------------------------
  def insertFormattedText(self, text: str, tags: Optional[Tuple[str, ...]] = None):
    self.infoTextWidget.insert(tk.END, text, tags)
  #------------------------------------------------------------------------------------------------------------------------------
  def applyTagConfiguration(self, tagName: str, fontSettings: Tuple[str, int, str], spacing1: int = 0, spacing3: int = 0):
    self.infoTextWidget.tag_config(tagName, font=fontSettings, spacing1=spacing1, spacing3=spacing3)
  #------------------------------------------------------------------------------------------------------------------------------
  def insertHeaderWithTag(self, text: str, tagName: str, fontSettings: Tuple[str, int, str], spacing1: int, spacing3: int):
    self.insertFormattedText(text, (tagName,))
    self.applyTagConfiguration(tagName, fontSettings, spacing1, spacing3)
  #------------------------------------------------------------------------------------------------------------------------------
  def formatAndDisplayValue(self, label: str, value: Any):
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

    self.infoTextWidget.insert(tk.END, textToInsert + "\n")
    try:
      if label == "Website" and value and isinstance(value, str) and value.startswith("http"):
        currentPos = self.infoTextWidget.index(f"end-2l linestart + {len(label) + 2}c")
        endPos = self.infoTextWidget.index(f"end-2l lineend")
        self.infoTextWidget.tag_add(f"link_{value}", currentPos, endPos)
        self.infoTextWidget.tag_config(f"link_{value}", foreground="blue", underline=True)
    except Exception: 
      pass
  #------------------------------------------------------------------------------------------------------------------------------
  def displayDetails(self, companyInfo: Dict[str, Any], ticker: str):
    self.clearContent()
    if not companyInfo or companyInfo.get("error"):
      errorMsg = companyInfo.get("error", f"No company information available for {ticker}.")
      self.showMessage(errorMsg)
      return
    self.insertHeaderWithTag(
        f"--- {companyInfo.get('longName', ticker)} ({ticker}) ---\n",
        'header',
        ('Segoe UI', 11, 'bold'),
        spacing1=5, spacing3=5
    )
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
      "Regular Market Price": "regularMarketPrice",
      "Open": "open",
      "Previous Close": "previousClose", 
      "Day High": "dayHigh", 
      "Day Low": "dayLow",
      'Earnings date': 'earningsTimestampStart',
      'Recommendation': 'recommendationKey',
    }
    for displayLabel, infoKey in infoMap.items():
      value = companyInfo.get(infoKey)
      if value is not None:
        if infoKey == "exDividendDate" and isinstance(value, (int, float)):
          try: value = datetime.datetime.fromtimestamp(value).strftime('%Y-%m-%d')
          except: pass 
        elif infoKey == "earningsTimestampStart":
          if isinstance(value, list) and len(value) > 0:
            try:
              numericValues = [v for v in value if isinstance(v, (int, float))]
              if numericValues:
                  tsValue = min(numericValues)
                  value = datetime.datetime.fromtimestamp(tsValue).strftime('%Y-%m-%d')
              else:
                  value = "N/A"
            except: value = "N/A" 
          elif isinstance(value, (int, float)):
            try: value = datetime.datetime.fromtimestamp(value).strftime('%Y-%m-%d')
            except: value = "N/A"         
        self.formatAndDisplayValue(displayLabel, value)
    summary = companyInfo.get('longBusinessSummary')
    if summary:
      self.insertHeaderWithTag(
          "\n--- Business Summary ---\n",
          'header2',
          ('Segoe UI', 10, 'bold'),
          spacing1=5, spacing3=3
      )
      if self.infoTextWidget:
        self.infoTextWidget.insert(tk.END, summary)
