import tkinter as tk
from tkinter import ttk, messagebox, Listbox, Scrollbar, END
import pandas as pd
import datetime
import mplfinance as mpf
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import matplotlib.pyplot as plt
import threading
from typing import List, Dict, Any, Optional, Tuple

import loader 
import indicators 
import infoDisplay as info 
#--------------------------------------------------------------------------------------------------------------------------------
def calculateDateRanges(yearsToDisplay: int) -> Tuple[datetime.date, datetime.date, pd.Timestamp]:
  if not (1 <= yearsToDisplay <= 20): yearsToDisplay = 2
  endDate = datetime.date.today()
  indicatorBufferDays = 300 + (365 if yearsToDisplay > 2 else 0) 
  displayStartDate = endDate - datetime.timedelta(days=yearsToDisplay * 365)
  startDateForDataFetch = displayStartDate - datetime.timedelta(days=indicatorBufferDays)
  displayStartDateTimestamp = pd.Timestamp(displayStartDate)
  return startDateForDataFetch, endDate, displayStartDateTimestamp
#--------------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------
class ChartingUtils:
  #--------------------------------------------------------------------------------------------------------------------------------
  def __init__(self):
    self.macdPanelId = -1
    self.rsiPanelId = -1
    self.stochPanelId = -1
  #--------------------------------------------------------------------------------------------------------------------------------
  def createErrorFigure(self, message: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.text(0.5, 0.5, message, ha='center', va='center', fontsize=10, wrap=True)
    try: fig.tight_layout(pad=0.5) 
    except Exception: pass 
    return fig
  #--------------------------------------------------------------------------------------------------------------------------------
  def preparePlotData(self, dataFrame: pd.DataFrame, tickerSymbol: str, chartTimeframe: str) -> Optional[pd.DataFrame]:
    if dataFrame.empty or len(dataFrame) < 2: return None
    requiredOhlcCols = ['Open', 'High', 'Low', 'Close']
    if not all(col in dataFrame.columns for col in requiredOhlcCols): return None
    plotDf = dataFrame.copy()
    if 'Volume' not in plotDf.columns: plotDf['Volume'] = 0 
    plotDf.dropna(subset=requiredOhlcCols, how='any', inplace=True) 
    plotDf['Volume'] = plotDf['Volume'].fillna(0) 
    if plotDf.empty or len(plotDf) < 2: return None
    if not isinstance(plotDf.index, pd.DatetimeIndex):
      try: plotDf.index = pd.to_datetime(plotDf.index)
      except Exception: return None
    return plotDf
  #--------------------------------------------------------------------------------------------------------------------------------
  def addBollingerBandsToPlot(self, plotDf: pd.DataFrame, addPlots: List[Dict[str, Any]]):
    if 'BbUpper' in plotDf.columns and 'BbLower' in plotDf.columns:
      if not plotDf['BbUpper'].isnull().all() and not plotDf['BbLower'].isnull().all():
        addPlots.append(mpf.make_addplot(plotDf['BbUpper'], panel=0, color='darkgray', linestyle='--', width=0.7))
        addPlots.append(mpf.make_addplot(plotDf['BbLower'], panel=0, color='darkgray', linestyle='--', width=0.7))
  #--------------------------------------------------------------------------------------------------------------------------------
  def addMacdToPlot(self, plotDf: pd.DataFrame, addPlots: List[Dict[str, Any]], currentPanelId: int) -> bool:
    if 'Macd' in plotDf.columns and not plotDf['Macd'].isnull().all():
      onRight = False
      addPlots.append(mpf.make_addplot(plotDf['Macd'], panel=currentPanelId, color='dodgerblue', ylabel='MACD', width=0.8, y_on_right=onRight)) 
      if 'MacdSignal' in plotDf.columns and not plotDf['MacdSignal'].isnull().all():
        addPlots.append(mpf.make_addplot(plotDf['MacdSignal'], panel=currentPanelId, color='orangered', width=0.8, y_on_right=onRight))
      if 'MacdHist' in plotDf.columns and not plotDf['MacdHist'].isnull().all():
        macdHistNumeric = pd.to_numeric(plotDf['MacdHist'], errors='coerce').fillna(0)
        if not macdHistNumeric.isnull().all() and (macdHistNumeric != 0).any():
          macdHistGreen = macdHistNumeric.where(macdHistNumeric >= 0, 0)
          macdHistRed = macdHistNumeric.where(macdHistNumeric < 0, 0)
          if (macdHistGreen != 0).any(): 
            addPlots.append(mpf.make_addplot(macdHistGreen, type='bar', panel=currentPanelId, color='green', alpha=0.7, y_on_right=onRight))
          if (macdHistRed != 0).any(): 
            addPlots.append(mpf.make_addplot(macdHistRed  , type='bar', panel=currentPanelId, color='red'  , alpha=0.7, y_on_right=onRight))
      return True
    return False
  #--------------------------------------------------------------------------------------------------------------------------------
  def addRsiToPlot(self, plotDf: pd.DataFrame, addPlots: List[Dict[str, Any]], currentPanelId: int, rsiYlabelOverride: Optional[str] = None) -> bool:
    if 'Rsi' in plotDf.columns and not plotDf['Rsi'].isnull().all():
      onRight = True
      addPlots.append(mpf.make_addplot(plotDf['Rsi'], panel=currentPanelId, color='purple', ylabel='RSI', ylim=(0,100), width=0.8, y_on_right=onRight)) 
      addPlots.append(mpf.make_addplot(pd.Series(80, index=plotDf.index), panel=currentPanelId, color='red'  , linestyle='dashed', width=0.7, y_on_right=onRight))
      addPlots.append(mpf.make_addplot(pd.Series(20, index=plotDf.index), panel=currentPanelId, color='green', linestyle='dashed', width=0.7, y_on_right=onRight))
      return True
    return False
  #--------------------------------------------------------------------------------------------------------------------------------
  def addStochasticToPlot(self, plotDf: pd.DataFrame, addPlots: List[Dict[str, Any]], currentPanelId: int) -> bool:
    if 'stochK' in plotDf.columns and not plotDf['stochK'].isnull().all():
      onRight = False
      addPlots.append(mpf.make_addplot(plotDf['stochK'], panel=currentPanelId, color='lightgreen', ylim=(0,100), width=0.8, y_on_right=onRight, ylabel='STOCH')) 
      addPlots.append(mpf.make_addplot(plotDf['stochKSlow'], panel=currentPanelId, color='green' , ylim=(0,100), width=0.8, y_on_right=True)) 
      if 'stochD' in plotDf.columns and not plotDf['stochD'].isnull().all():
        addPlots.append(mpf.make_addplot(plotDf['stochD'], panel=currentPanelId, color='orangered', width=0.8, y_on_right=onRight))
        addPlots.append(mpf.make_addplot(plotDf['stochDSlow'], panel=currentPanelId, color='red'  , width=0.8, y_on_right=onRight))
      addPlots.append(mpf.make_addplot(pd.Series(70, index=plotDf.index), panel=currentPanelId, color='red'  , linestyle='dashed', width=0.7, y_on_right=onRight))
      addPlots.append(mpf.make_addplot(pd.Series(30, index=plotDf.index), panel=currentPanelId, color='green', linestyle='dashed', width=0.7, y_on_right=onRight))
      return True
    return False
  #--------------------------------------------------------------------------------------------------------------------------------
  def configureIndicatorPlots(self, plotDf: pd.DataFrame, addPlots: List[Dict[str, Any]]) -> Tuple[List[int], int]:
    self.macdPanelId, self.rsiPanelId, self.stochPanelId = -1, -1, -1
    nextIndicatorPanelId = 2 # Panels für Indikatoren beginnen bei 2, da Panel 0 Preis und Panel 1 Volumen ist.
                            # Dies muss konsistent mit der `volume_panel` Einstellung in mpf.plot sein.
                            # Wenn mpf.plot(volume=True) verwendet wird, ist Panel 0 Preis, Panel 1 Volumen.
                            # Daher starten unsere Indikatoren auf Panel 2.
    indicatorPanelRatioValues: List[int] = []
    nrOfAppends = 2 
    if self.addMacdToPlot(plotDf, addPlots, nextIndicatorPanelId):
      indicatorPanelRatioValues.append(nrOfAppends)
      self.macdPanelId = nextIndicatorPanelId
      nextIndicatorPanelId += 1
    if self.addRsiToPlot(plotDf, addPlots, nextIndicatorPanelId): 
      indicatorPanelRatioValues.append(nrOfAppends)
      self.rsiPanelId = nextIndicatorPanelId
      nextIndicatorPanelId += 1
    if self.addStochasticToPlot(plotDf, addPlots, nextIndicatorPanelId):
      indicatorPanelRatioValues.append(nrOfAppends)
      self.stochPanelId = nextIndicatorPanelId
    return indicatorPanelRatioValues, nextIndicatorPanelId 
  #--------------------------------------------------------------------------------------------------------------------------------
  def createMpfStyle(self) -> Dict:
    return mpf.make_mpf_style(base_mpf_style='yahoo',
                              rc={'axes.labelsize': 8, 'xtick.labelsize': 7, 'ytick.labelsize': 7, 
                                  'font.size': 8, 'figure.titlesize': 10, 'axes.titlesize': 10,
                                  'legend.loc': 'upper left', 'legend.fontsize': 7},
                              facecolor='#FDFDFD')
  #--------------------------------------------------------------------------------------------------------------------------------
  def createStockChartFigure(self,
                              dataFrame: pd.DataFrame,
                              tickerSymbol: str,
                              chartTimeframe: str = 'Daily',
                              movingAverageWindows: Optional[Tuple[int, ...]] = (10, 20, 50, 100, 200),
                              rsiYlabelOverride: Optional[str] = None, 
                              ) -> plt.Figure:
    plotDf = self.preparePlotData(dataFrame, tickerSymbol, chartTimeframe)
    if plotDf is None:
      msg = f"Data prep failed for {tickerSymbol} ({chartTimeframe})"
      if dataFrame.empty or len(dataFrame) < 2: 
        msg = f"No data or not enough data for\n{chartTimeframe} chart of {tickerSymbol}"
      elif not all(col in dataFrame.columns for col in ['Open', 'High', 'Low', 'Close']): 
        msg = f"Missing essential OHLC columns for\n{chartTimeframe} chart of {tickerSymbol}"
      elif not isinstance(dataFrame.index, pd.DatetimeIndex) and (isinstance(dataFrame.index, pd.Index) and not isinstance(pd.to_datetime(dataFrame.index, errors='coerce'), pd.DatetimeIndex)): 
        msg = f"Invalid date index for {chartTimeframe} chart of {tickerSymbol}"
      else: 
        msg = f"Not enough valid OHLCV data after cleaning for\n{chartTimeframe} chart of {tickerSymbol}"
      return self.createErrorFigure(msg)

    addPlots: List[Dict[str, Any]] = []
    self.addBollingerBandsToPlot(plotDf, addPlots)    
    self.configureIndicatorPlots(plotDf, addPlots)
    finalPanelRatios = tuple([6, 1, 3, 2, 2])
    mpfStyle = self.createMpfStyle()
    fig: Optional[plt.Figure] = None
    returnedAxesObject: Optional[List[plt.Axes]] = None
    
    try:
      fig, returnedAxesObject = mpf.plot(
        plotDf,
        type='candle', style=mpfStyle, title=f'{tickerSymbol} - {chartTimeframe}',
        addplot=addPlots, # Enthält nur Indikatoren
        mav=movingAverageWindows, 
        panel_ratios=finalPanelRatios,
        figscale=1.0, 
        volume=True,     # Zeichnet Volumen auf Panel 1 (unter dem Preispanel) mit rechter Y-Achse
        volume_panel=1,  # Explizit Panel 1 für Volumen
        ylabel='Price',  # Label für die Preis-Achse (LINKS auf Panel 0)
        returnfig=True, 
        update_width_config=dict(candle_linewidth=0.7, candle_width=0.6)
      )
    except Exception as e:
      print(f"Error in mplfinance.plot for {tickerSymbol} ({chartTimeframe}): {e}")
      import traceback
      traceback.print_exc()
      return self.createErrorFigure(f"Plotting error for {tickerSymbol} ({chartTimeframe}):\n{str(e)[:100]}")
    
    if fig is None: 
      return self.createErrorFigure(f"Figure generation failed for {tickerSymbol} ({chartTimeframe}).")
    try: 
      fig.subplots_adjust(left=0.1, bottom=0.12, right=0.9, top=0.92, hspace=0.3) 
    except Exception as e: 
      print(f"Warning: subplots_adjust failed: {e}")
    return fig
#--------------------------------------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------------------------------
class StockAnalyzerApp:
  #--------------------------------------------------------------------------------------------------------------------------------
  def __init__(self, root: tk.Tk):
    self.root = root
    self.root.title("Stock Analyzer")
    self.root.minsize(1600, 900) 
    self.dataProvider = loader.getProvider()
    self.indicatorCalc = indicators.Calculator()
    self.chartUtils = ChartingUtils() 
    self.companyInfoDisplay: Optional[info.CompanyInfoDisplay] = None
    self.currentTicker = tk.StringVar(value='AAPL') 
    self.stockList: List[str] = loader.loadStockListFromFile()
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
  def setupWatchlistPane(self, parentPane: ttk.PanedWindow):
    widthWatchlist = 2
    row = 0
    watchlistFrame = ttk.LabelFrame(parentPane, text="Watchlist", padding=2)
    watchlistFrame.columnconfigure(0, weight=1) 
    addTickerFrame = ttk.Frame(watchlistFrame)
    addTickerFrame.grid(row=row, column=0, sticky="ew", pady=(0,5))
    addTickerFrame.columnconfigure(0, weight=1) 
    self.newTickerEntry = ttk.Entry(addTickerFrame, width=widthWatchlist) 
    self.newTickerEntry.grid(row=0, column=0, sticky="ew", padx=(0,5))
    self.newTickerEntry.bind("<Return>", self.addTickerToList) 
    addTickerButton = ttk.Button(addTickerFrame, text="Add", command=self.addTickerToList, width=5)
    addTickerButton.grid(row=0, column=1, sticky="e")
    row += 1
    removeTickerButton = ttk.Button(watchlistFrame, text="Remove Selected", command=self.removeSelectedTicker)
    removeTickerButton.grid(row=row, column=0, sticky="ew", pady=(5,0))
    row += 1
    watchlistFrame.rowconfigure(row, weight=1) 
    listboxFrame = ttk.Frame(watchlistFrame) 
    listboxFrame.grid(row=row, column=0, sticky="nsew")
    listboxFrame.rowconfigure(0, weight=1)
    listboxFrame.columnconfigure(0, weight=1)
    self.tickerListBox = Listbox(listboxFrame, selectmode=tk.SINGLE, exportselection=False, activestyle='none')
    self.tickerListBox.grid(row=0, column=0, sticky="nsew")
    self.tickerListBox.bind("<<ListboxSelect>>", self.handleTickerSelect)
    listScrollbar = Scrollbar(listboxFrame, orient="vertical", command=self.tickerListBox.yview)
    listScrollbar.grid(row=0, column=1, sticky="ns")
    self.tickerListBox.config(yscrollcommand=listScrollbar.set)
    row += 1
    timePeriodFrame = ttk.Frame(watchlistFrame, padding=(0, 5, 0, 0))
    timePeriodFrame.grid(row=row, column=0, sticky="ew", pady=(10,0))
    timePeriodFrame.columnconfigure(1, weight=1) 
    ttk.Label(timePeriodFrame, text="Years:").grid(row=0, column=0, sticky="w", padx=(0,2))
    self.yearsEntry = ttk.Entry(timePeriodFrame, textvariable=self.displayYearsVar, width=4)
    self.yearsEntry.grid(row=0, column=1, sticky="ew", padx=(0,5))
    self.yearsEntry.bind("<Return>", lambda event: self.updateDisplayPeriodAndReload()) 
    updatePeriodButton = ttk.Button(timePeriodFrame, text="Update", command=self.updateDisplayPeriodAndReload, width=7)
    updatePeriodButton.grid(row=0, column=2, sticky="e")
    parentPane.add(watchlistFrame, weight=widthWatchlist)
  #------------------------------------------------------------------------------------------------------------------------------
  def setupContentAreaPanes(self, parentPane: ttk.PanedWindow):
    widthDaily         = 8 
    widthWeeklyAndInfo = 6

    contentArea = ttk.PanedWindow(parentPane, orient=tk.HORIZONTAL)

    self.dailyChartFrameContainer = ttk.LabelFrame(contentArea, text="Daily Chart", padding=(1,2,2,1), width=widthDaily*50)
    contentArea.add(self.dailyChartFrameContainer)

    weeklyAndInfoFrame = ttk.PanedWindow(contentArea, orient=tk.VERTICAL, width=widthWeeklyAndInfo*50)
    self.weeklyChartFrameContainer = ttk.LabelFrame(weeklyAndInfoFrame, text="Weekly Chart", padding=(1,2,2,1))
    weeklyAndInfoFrame.add(self.weeklyChartFrameContainer)
    self.companyInfoDisplay = info.CompanyInfoDisplay(weeklyAndInfoFrame) 
    contentArea.add(weeklyAndInfoFrame)

    parentPane.add(contentArea, weight=widthDaily + widthWeeklyAndInfo) 
  #--------------------------------------------------------------------------------------------------------------------------------
  def setupStatusBar(self):
    self.statusBar = ttk.Label(self.root, text="Ready", relief=tk.SUNKEN, anchor=tk.W)
    self.statusBar.pack(side=tk.BOTTOM, fill=tk.X)
  #--------------------------------------------------------------------------------------------------------------------------------
  def updateChartTitles(self):
    years = self.displayYearsVar.get()
    yearsText = f"({years} Year{'s' if years != 1 else ''})"
    if hasattr(self, 'dailyChartFrameContainer') and self.dailyChartFrameContainer.winfo_exists():
      self.dailyChartFrameContainer.config(text=f"Daily Chart {yearsText}")
    if hasattr(self, 'weeklyChartFrameContainer') and self.weeklyChartFrameContainer.winfo_exists():
      self.weeklyChartFrameContainer.config(text=f"Weekly Chart {yearsText}")
  #--------------------------------------------------------------------------------------------------------------------------------
  def setupUserInterface(self):
    self.rootPanedWindow = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
    self.rootPanedWindow.pack(expand=True, fill=tk.BOTH, padx=5, pady=5)
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
      self.loadStockData(self.tickerListBox.get(selectedIndices[0])) 
    else: 
      messagebox.showinfo("No selection", "Select a ticker from the watchlist to reload.")
  #------------------------------------------------------------------------------------------------------------------------------
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
      loader.saveStockListToFile(self.stockList) 
      try:
        idx = self.stockList.index(newTicker) 
        self.tickerListBox.selection_clear(0, END)
        self.tickerListBox.selection_set(idx) 
        self.tickerListBox.see(idx)
        self.handleTickerSelect(None) 
      except ValueError: 
        pass 
    self.newTickerEntry.delete(0, END) 
  #------------------------------------------------------------------------------------------------------------------------------
  def removeSelectedTicker(self):
    selectedIndices = self.tickerListBox.curselection()
    if not selectedIndices: 
      messagebox.showinfo("No selection", "Please select a ticker to remove.")
      return
    selectedTicker = self.tickerListBox.get(selectedIndices[0])
    if not messagebox.askyesno("Confirm Removal", f"Remove {selectedTicker}?"): 
      return
    if selectedTicker in self.stockList:
      self.stockList.remove(selectedTicker)
      self.updateTickerListBox()
      loader.saveStockListToFile(self.stockList) 
      if self.stockList: 
        self.tickerListBox.selection_set(0)
        self.handleTickerSelect(None) 
      else: 
        self.clearPreviousCharts() 
        if self.companyInfoDisplay: 
          self.companyInfoDisplay.showMessage("Watchlist is empty.")
        self.updateChartTitles()
        self.statusBar.config(text="Watchlist empty.")
  #--------------------------------------------------------------------------------------------------------------------------------
  def updateTickerListBox(self):
    self.tickerListBox.delete(0, END)
    for ticker in self.stockList: self.tickerListBox.insert(END, ticker)
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
    #----------------------------------
    def destroy(canvas, fig, toolbar):
      if canvas: 
        canvas.get_tk_widget().destroy()
      if toolbar:
        toolbar.destroy()
      if fig:
        plt.close(fig)
      return None, None, None
    #----------------------------------
    self.dailyFig, self.dailyChartCanvas, self.dailyToolbar = destroy(self.dailyChartCanvas, self.dailyFig, self.dailyToolbar)
    if hasattr(self, 'dailyChartFrameContainer') and self.dailyChartFrameContainer.winfo_exists():
      for w in self.dailyChartFrameContainer.winfo_children(): 
        w.destroy()
    self.weeklyFig, self.weeklyChartCanvas, self.weeklyToolbar = destroy(self.weeklyChartCanvas, self.weeklyFig, self.weeklyToolbar)
    if hasattr(self, 'weeklyChartFrameContainer') and self.weeklyChartFrameContainer.winfo_exists():
      for w in self.weeklyChartFrameContainer.winfo_children(): 
        w.destroy()
  #------------------------------------------------------------------------------------------------------------------------------
  def displayError(self, message: str, ticker: str ="N/A"):
    self.clearPreviousCharts() 
    errFigD = self.chartUtils.createErrorFigure(f"Daily Error: {ticker}\n{message}")
    self.dailyFig, self.dailyChartCanvas, self.dailyToolbar = self.displaySingleChart(errFigD, self.dailyChartFrameContainer, "Daily", ticker)
    errFigW = self.chartUtils.createErrorFigure(f"Weekly Error: {ticker}\n{message}")
    self.weeklyFig, self.weeklyChartCanvas, self.weeklyToolbar = self.displaySingleChart(errFigW, self.weeklyChartFrameContainer, "Weekly", ticker)
    if self.companyInfoDisplay: 
      self.companyInfoDisplay.showError(message, ticker)
    self.statusBar.config(text=f"Error loading {ticker}")
  #--------------------------------------------------------------------------------------------------------------------------------
  def displaySingleChart(self, fig: Optional[plt.Figure], container: ttk.LabelFrame, type: str, ticker: str) -> Tuple[Optional[plt.Figure], Optional[FigureCanvasTkAgg], Optional[NavigationToolbar2Tk]]:
    if fig is None: fig = self.chartUtils.createErrorFigure(f"Fig is None for {ticker} {type}")
    if not container.winfo_exists(): 
        if fig: 
          plt.close(fig)
          print(f"Container for {type} chart of {ticker} destroyed.")
        return None, None, None 
    for w in container.winfo_children(): w.destroy()
    canvas = FigureCanvasTkAgg(fig, master=container)
    canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True)
    toolbar = NavigationToolbar2Tk(canvas, container)
    toolbar.update()
    toolbar.pack(side=tk.BOTTOM, fill=tk.X)
    return fig, canvas, toolbar
  #--------------------------------------------------------------------------------------------------------------------------------
  def displayProcessedData(self, payload: Dict[str, Any]):
    self.clearPreviousCharts()
    dataD, dataW, infoVal, ticker, err = payload.get('daily_data'), payload.get('weekly_data'), payload.get('company_info'), payload.get('ticker', "N/A"), payload.get('error')
    if err: 
      self.displayError(err, ticker)
      return    
    self.dailyFig = self.chartUtils.createStockChartFigure(dataD, ticker, "Daily") if dataD is not None and not dataD.empty else self.chartUtils.createErrorFigure(f"No/Bad Daily Data: {ticker}")
    self.dailyFig, self.dailyChartCanvas, self.dailyToolbar = self.displaySingleChart(self.dailyFig, self.dailyChartFrameContainer, "Daily", ticker)

    self.weeklyFig = self.chartUtils.createStockChartFigure(dataW, ticker, "Weekly") if dataW is not None and not dataW.empty else self.chartUtils.createErrorFigure(f"No/Bad Weekly Data: {ticker}")
    self.weeklyFig, self.weeklyChartCanvas, self.weeklyToolbar = self.displaySingleChart(self.weeklyFig, self.weeklyChartFrameContainer, "Weekly", ticker)

    if self.companyInfoDisplay:
        if infoVal: 
          self.companyInfoDisplay.displayDetails(infoVal, ticker)
        else: 
          self.companyInfoDisplay.showMessage(f"No company info for {ticker}.")
    self.statusBar.config(text=f"Displaying {ticker}")
    self.updateChartTitles() 
  #------------------------------------------------------------------------------------------------------------------------------
  def handleDataForCharting(self, dataFromThread: Dict[str, Any]):
    if dataFromThread:
      try: 
        self.displayProcessedData(dataFromThread)
      except Exception as eDisp: 
        print(f"Critical error in displayProcessedData for {dataFromThread.get('ticker')}: {eDisp}")
        import traceback
        traceback.print_exc()
        self.displayError(f"Display Error: {eDisp}", dataFromThread.get('ticker', 'N/A'))
    else: 
      self.displayError("Empty response from background thread.")
    self.root.config(cursor="")
    self.statusBar.config(text=f"Ready. Last update: {self.currentTicker.get()}.")
  #--------------------------------------------------------------------------------------------------------------------------------
  def applyIndicatorsAndFilterData(self, dataFrame: pd.DataFrame, displayStartDateTs: pd.Timestamp, ticker: str, interval: str) -> Optional[pd.DataFrame]:
    if dataFrame is None or dataFrame.empty: return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'], index=pd.to_datetime([]))
    df = dataFrame.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
      try: df.index = pd.to_datetime(df.index)
      except Exception as e: 
        print(f"Index conversion error {ticker} ({interval}): {e}")
        return pd.DataFrame(columns=df.columns, index=pd.to_datetime([]))          
    if df.index.tz is not None: df.index = df.index.tz_localize(None) 
    df = self.indicatorCalc.setDataframe(df).calculate().get() 
    startTsN = displayStartDateTs.tz_localize(None) if df.index.tz is None and displayStartDateTs.tz is not None else displayStartDateTs
    startTsN = startTsN.tz_convert(None) if hasattr(startTsN, 'tz') and startTsN.tz is not None else startTsN
    df.sort_index(inplace=True)
    try:
      compTs = startTsN
      if df.index.tz != getattr(startTsN, 'tz', None): 
        if df.index.tz is None and startTsN.tz is not None: compTs = startTsN.tz_localize(None)
        elif df.index.tz is not None and startTsN.tz is None: compTs = pd.Timestamp(startTsN, tz=df.index.tz)
        else: compTs = startTsN.tz_convert(df.index.tz) if df.index.tz else startTsN
      filteredDf = df[df.index >= compTs]
      if filteredDf.empty: print(f"Warning: Filtered DF empty {ticker} ({interval}) date {compTs}.")
      return filteredDf
    except Exception as eF: 
      print(f"Date filter error {ticker} ({interval}): {eF}")
      return pd.DataFrame(columns=df.columns, index=pd.to_datetime([]))
  #------------------------------------------------------------------------------------------------------------------------------
  def fetchAndProcessIntervalData(self, ticker: str, startDt: datetime.date, endDt: datetime.date, dispStartTs: pd.Timestamp, interval: str) -> Optional[pd.DataFrame]:
    finalDf = loader.fetchAndProcessIntervalData(ticker, startDt, endDt, interval)
    return self.applyIndicatorsAndFilterData(finalDf, dispStartTs, ticker, interval)
  #------------------------------------------------------------------------------------------------------------------------------
  def processDataInBackground(self, ticker: str, years: int) -> Dict[str, Any]:
    try:
      startDt, endDt, dispStartTs = calculateDateRanges(years)
      dailyDf = self.fetchAndProcessIntervalData(ticker, startDt, endDt, dispStartTs, '1d')
      weeklyDf = self.fetchAndProcessIntervalData(ticker, startDt, endDt, dispStartTs, '1wk')
      infoVal = self.dataProvider.getCompanyInfo(ticker)
      if isinstance(infoVal, dict) and infoVal.get("error"): print(f"Company info error {ticker}: {infoVal.get('error')}")
      
      payload: Dict[str, Any] = {
        'daily_data': dailyDf if dailyDf is not None else pd.DataFrame(), 
        'weekly_data': weeklyDf if weeklyDf is not None else pd.DataFrame(),
        'company_info': infoVal, 'ticker': ticker, 'error': None
      }
      if payload['daily_data'].empty and payload['weekly_data'].empty:
        errMsg = f"No chart data for {ticker}."
        if isinstance(infoVal, dict) and infoVal.get("error"): 
          errMsg = infoVal.get("error") 
        elif dailyDf is None and weeklyDf is None: 
          errMsg = f"Failed to fetch data for {ticker}."
        payload['error'] = errMsg
        print(f"Error in background {ticker}: {errMsg}")
      return payload
    except Exception as e:
      print(f"Critical background error {ticker}: {e}")
      import traceback
      traceback.print_exc()
      return {
        'daily_data': pd.DataFrame(), 
        'weekly_data': pd.DataFrame(), 
        'company_info': {"error": f"Crit BG err: {e}"}, 
        'ticker': ticker,
        'error': f"Crit err processing {ticker}: {e}"
      }
  #------------------------------------------------------------------------------------------------------------------------------
  def updateUiForLoading(self, ticker: str):
    self.clearPreviousCharts()
    if self.companyInfoDisplay: 
      self.companyInfoDisplay.showLoadingMessage(ticker)
    self.statusBar.config(text=f"Loading {ticker}...")
    self.root.config(cursor="watch")
    loadFigD = self.chartUtils.createErrorFigure(f"Loading Daily: {ticker}...")
    self.dailyFig, self.dailyChartCanvas, self.dailyToolbar = self.displaySingleChart(loadFigD, self.dailyChartFrameContainer, "Daily", ticker)
    loadFigW = self.chartUtils.createErrorFigure(f"Loading Weekly: {ticker}...")
    self.weeklyFig, self.weeklyChartCanvas, self.weeklyToolbar = self.displaySingleChart(loadFigW, self.weeklyChartFrameContainer, "Weekly", ticker)
  #--------------------------------------------------------------------------------------------------------------------------------
  def loadStockData(self, ticker: Optional[str] = None):
    if ticker is None:
      selIdx = self.tickerListBox.curselection()
      if not selIdx: 
        self.statusBar.config(text="No ticker selected.")
        return
      ticker = self.tickerListBox.get(selIdx[0])
    self.currentTicker.set(ticker)
    self.updateUiForLoading(ticker)
    yearsVal = self.displayYearsVar.get()
    if not (1 <= yearsVal <= 20): 
      yearsVal = 2
      self.displayYearsVar.set(2) 
    threading.Thread(target=lambda: self.root.after(0, self.handleDataForCharting, self.processDataInBackground(ticker, yearsVal)), daemon=True).start()
#---------------------------------------------------------------------------------------------------------------------- 
if __name__ == "__main__":
  root = tk.Tk()
  app = StockAnalyzerApp(root)
  root.mainloop()