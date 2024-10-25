#---(1)---初始化模組_開始---

# 導入必要的庫
from binance.client import Client
from binance.enums import *
from binance import ThreadedWebsocketManager
import pandas as pd
import numpy as np
import time
import json
import os
import logging
import websocket
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Tuple, Union
import threading
import uuid

# 訂單類型常量
SIDE_BUY = 'BUY'           # 買入方向
SIDE_SELL = 'SELL'         # 賣出方向
ORDER_TYPE_MARKET = 'MARKET'     # 市價單
ORDER_TYPE_LIMIT = 'LIMIT'       # 限價單
ORDER_TYPE_STOP = 'STOP'         # 止損單
ORDER_TYPE_TAKE_PROFIT = 'TAKE_PROFIT'  # 止盈單
ORDER_TYPE_STOP_MARKET = 'STOP_MARKET'  # 市價止損單
ORDER_TYPE_TAKE_PROFIT_MARKET = 'TAKE_PROFIT_MARKET'  # 市價止盈單
TIME_IN_FORCE_GTC = 'GTC'  # 訂單有效期：成交為止

# 系統配置類別
class Config:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            # K線時間週期設定
            self.KLINE_INTERVAL = ['15m', '3m', '30m']
            
            # 移動平均線週期設定
            self.MA_PERIOD = [979, 20, 726]
            
            # 交易對設定
            self.SYMBOL = ['BTCUSDT', 'BTCUSDT', 'BTCUSDT']
            
            # TWD匯率交易對
            self.TWD_SYMBOL = 'USDTTWD'
            
            # 槓桿倍數
            self.LEVERAGE = [35, 35, 35]
            
            # 止盈百分比
            self.TAKE_PROFIT_PERCENT = [7, 7, 10]
            
            # 止損百分比
            self.STOP_LOSS_PERCENT = [-80, -80, -80]
            
            # 使用餘額百分比
            self.USE_BALANCE_PERCENT = [99, 99, 99]
            
            # 交易開關
            self.TRADING_ENABLED = ['N', 'N', 'N']
            
            # 是否使用市價止盈
            self.USE_MARKET_TAKE_PROFIT = ['Y', 'Y', 'Y']
            
            # 是否在到達止盈價格時使用市價單
            self.USE_MARKET_TP_WHEN_REACHED = ['Y', 'Y', 'Y']
            
            # 是否發送交易信號檢查通知
            self.NOTIFY_SIGNAL_CHECK = ['N', 'Y', 'N']
            
            # 是否發送K線價格報告
            self.NOTIFY_KLINE_REPORT = ['Y', 'Y', 'Y']
            
            # 本金變化報告閾值（百分比）
            self.BALANCE_REPORT_THRESHOLD = 1.0
            
            # 程式名稱設定
            self.PROGRAM_NAMES = [
                f"第{i+1}組程式（{self.KLINE_INTERVAL[i]} {self.MA_PERIOD[i]}MA）"
                for i in range(3)
            ]
            
            # API設定
            self.IS_TEST_NET = os.getenv('IS_TEST_NET', 'N') == 'Y'
            self.API_KEY = os.getenv('D1_key', '')
            self.API_SECRET = os.getenv('D1_secret', '')
            
            # WebSocket重試設定
            self.WS_RETRY_LIMIT = 3
            self.WS_RETRY_DELAY = 1
            
            # REST API重試設定
            self.API_RETRY_LIMIT = 3
            self.API_RETRY_DELAY = 1
            
            # 時區設定
            self.TIMEZONE = pytz.timezone('Asia/Taipei')
            
            self._initialized = True

# 全域配置實例
Config = Config()

# 全域狀態類別
class GlobalState:
    """
    全域狀態類別
    管理所有交易程式的狀態、數據和日誌
    """
    def __init__(self):
        # 交易狀態追蹤
        self.CURRENT_STATE = [['IDLE'], ['IDLE'], ['IDLE']]
        self.CURRENT_STATE_INPUT = [[""], [""], [""]]
        
        # 上次交易方向記錄
        self.LAST_TRADE_DIRECTION = [
            [os.environ.get(f'LAST_TRADE_DIRECTION_{i+1}', 'IDLE')] 
            for i in range(3)
        ]
        self.LAST_TRADE_DIRECTION_INPUT = [[""], [""], [""]]
        
        # MA線反轉方向記錄
        self.last_ma_reversal = [[''], [''], ['']]
        self.last_ma_reversal_input = [[""], [""], [""]]
        
        # K線數據儲存
        self.klines_data = [pd.DataFrame() for _ in range(3)]
        
        # WebSocket連接狀態追蹤
        self.ws_connected = [False, False, False]
        
        # 本金監控
        self.last_balance = 0.0
        
        # 初始化日誌系統
        self._setup_logging()
    
    def _setup_logging(self):
        """設置日誌系統"""
        class CustomFormatter(logging.Formatter):
            def format(self, record):
                if record.levelno == logging.INFO:
                    return record.getMessage()
                return super().format(record)

        logging.basicConfig(
            level=logging.INFO,
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('trading_bot.log')
            ]
        )
        
        # 設置自定義格式
        custom_formatter = CustomFormatter()
        for handler in logging.getLogger().handlers:
            handler.setFormatter(custom_formatter)
            
        self.logger = logging.getLogger(__name__)

def get_twd_rate() -> float:
    """獲取USDT對TWD的即時匯率"""
    try:
        ticker = g_client.get_ticker(symbol=Config.TWD_SYMBOL)
        return float(ticker['lastPrice'])
    except Exception as e:
        logging.error(f"獲取TWD匯率失敗: {str(e)}".encode('utf-8').decode('utf-8'))
        return 31.0  # 預設匯率

def get_account_balance() -> Tuple[float, float]:
    """獲取帳戶餘額（USDT和TWD）"""
    try:
        account = g_client.futures_account()
        balance_usdt = float(account['totalWalletBalance'])
        twd_rate = get_twd_rate()
        balance_twd = balance_usdt * twd_rate
        return balance_usdt, balance_twd
    except Exception as e:
        logging.error(f"獲取帳戶餘額失敗: {str(e)}".encode('utf-8').decode('utf-8'))
        return 0.0, 0.0

def initialize_binance_client() -> Client:
    """初始化幣安客戶端"""
    try:
        if Config.IS_TEST_NET:
            client = Client(
                Config.API_KEY, 
                Config.API_SECRET,
                testnet=True
            )
            logging.info("初始化測試網客戶端成功".encode('utf-8').decode('utf-8'))
        else:
            client = Client(
                Config.API_KEY, 
                Config.API_SECRET
            )
            logging.info("初始化主網客戶端成功".encode('utf-8').decode('utf-8'))
        return client
    except Exception as e:
        logging.error(f"初始化幣安客戶端失敗: {str(e)}".encode('utf-8').decode('utf-8'))
        raise

def initialize_global_variables():
    """初始化全域變量"""
    try:
        # 初始化全域狀態
        global g_state
        g_state = GlobalState()
        
        # 初始化幣安客戶端
        global g_client
        g_client = initialize_binance_client()
        
        # 初始化WebSocket管理器
        global g_twm
        g_twm = ThreadedWebsocketManager(
            api_key=Config.API_KEY,
            api_secret=Config.API_SECRET,
            testnet=Config.IS_TEST_NET
        )
        
        # 獲取並儲存初始餘額
        balance_usdt, _ = get_account_balance()
        g_state.last_balance = balance_usdt
        
        logging.info("全域變量初始化成功".encode('utf-8').decode('utf-8'))
        return True
    except Exception as e:
        logging.error(f"全域變量初始化失敗: {str(e)}".encode('utf-8').decode('utf-8'))
        return False

def get_taiwan_time() -> str:
    """獲取台灣時間（HH:MM格式）"""
    return datetime.now(Config.TIMEZONE).strftime("%H:%M")

#---(1)---初始化模組_結束---




#---(2)---幣安交易程式_數據同步和恢復模組_開始---

async def synchronize_data():
    """同步 Binance 上的最新帳戶和 K 線數據"""
    try:
        logger.info("開始同步帳戶和 K 線數據...")

        # 為每個交易組別同步數據
        for group_index in range(3):
            # 同步 K 線數據
            if not check_api_limits():
                await asyncio.sleep(1)

            kline_data[group_index] = await asyncio.to_thread(
                client.get_klines,
                symbol=symbol[group_index],
                interval=KLINE_INTERVAL[group_index],
                limit=1000
            )
            
            # 確認 K 線數據完整性
            if not kline_data[group_index] or len(kline_data[group_index]) < 1000:
                logger.error(f"第{group_index + 1}組程式同步的 K 線數據不足，長度為 {len(kline_data[group_index])}")
                continue

            logger.info(f"第{group_index + 1}組程式同步的 K 線數據成功，長度為 {len(kline_data[group_index])}")

            # 同步帳戶數據
            if not check_api_limits():
                await asyncio.sleep(1)

            position_info = await asyncio.to_thread(
                client.futures_position_information,
                symbol=symbol[group_index]
            )

            if not position_info or len(position_info) == 0:
                logger.error(f"無法獲取第{group_index + 1}組程式的帳戶持倉信息")
                continue

            position_amt = float(position_info[0].get('positionAmt', 0))
            entry_price = float(position_info[0].get('entryPrice', 0))

            # 更新本地變數
            local_position_amt[group_index][0] = position_amt
            local_entry_price[group_index][0] = entry_price

            # 更新倉位狀態
            CURRENT_STATE[group_index] = ['IDLE'] if position_amt == 0 else ['LONG' if position_amt > 0 else 'SHORT']
            ENTRY_BALANCE[group_index][0] = client.futures_account_balance()[0]['balance']

            logger.info(
                f"第{group_index + 1}組程式 - 當前持倉量: {position_amt}, 進場價格: {entry_price}, "
                f"倉位狀態: {CURRENT_STATE[group_index][0]}"
            )

    except BinanceAPIException as e:
        logger.error(f"同步數據時 Binance API 發生錯誤: {str(e)}")
    except Exception as e:
        logger.error(f"同步數據時發生未知錯誤: {str(e)}")

#---(2)---幣安交易程式_數據同步和恢復模組_結束---


#---(3)---MA計算模組_開始---

class MACalculator:
    """MA計算與趨勢判斷類"""
    def __init__(self, program_index: int):
        self.program_index = program_index
        self.ma_period = Config.MA_PERIOD[program_index]
        self.consecutive_count = 3  # 連續上升或下降的根數要求
        
        # 檢查是否有初始MA方向
        if g_state.last_ma_reversal_input[program_index][0]:
            self.initialize_ma_direction()

    def initialize_ma_direction(self):
        """初始化MA方向"""
        initial_direction = g_state.last_ma_reversal_input[self.program_index][0]
        if initial_direction in ['UP', 'DOWN']:
            g_state.last_ma_reversal[self.program_index][0] = initial_direction

    def calculate_ma(self, df: pd.DataFrame) -> pd.DataFrame:
        """計算MA值"""
        try:
            if df.empty or len(df) < self.ma_period:
                logging.warning(f"K線數據不足以計算{self.ma_period}期MA".encode('utf-8').decode('utf-8'))
                return df
            
            # 確保數據已經按時間排序
            df = df.sort_index()
            
            # 計算MA值並四捨五入到小數點後1位
            df['MA'] = df['close'].rolling(window=self.ma_period, min_periods=self.ma_period).mean().round(1)
            
            return df
            
        except Exception as e:
            logging.error(f"計算MA失敗: {str(e)}".encode('utf-8').decode('utf-8'))
            return df

    def check_ma_trend(self, df: pd.DataFrame) -> str:
        """檢查MA趨勢"""
        try:
            if 'MA' not in df.columns or df.empty:
                return ''
            
            # 如果有初始方向且尚未更新過，返回初始方向
            if (g_state.last_ma_reversal_input[self.program_index][0] and 
                g_state.last_ma_reversal[self.program_index][0] == g_state.last_ma_reversal_input[self.program_index][0]):
                return g_state.last_ma_reversal[self.program_index][0]
            
            # 確保數據已經按時間排序
            df = df.sort_index()
            
            # 獲取最近的MA值
            recent_ma = df['MA'].dropna().tail(self.consecutive_count)
            
            if len(recent_ma) < self.consecutive_count:
                return ''
            
            # 檢查是否連續上升
            is_uptrend = all(recent_ma.iloc[i] > recent_ma.iloc[i-1] 
                           for i in range(1, len(recent_ma)))
            
            # 檢查是否連續下降
            is_downtrend = all(recent_ma.iloc[i] < recent_ma.iloc[i-1] 
                             for i in range(1, len(recent_ma)))
            
            if is_uptrend:
                return 'UP'
            elif is_downtrend:
                return 'DOWN'
            else:
                return g_state.last_ma_reversal[self.program_index][0]
                
        except Exception as e:
            logging.error(f"檢查MA趨勢失敗: {str(e)}".encode('utf-8').decode('utf-8'))
            return ''

    def update_ma_direction(self, df: pd.DataFrame) -> bool:
        """更新MA方向"""
        try:
            trend = self.check_ma_trend(df)
            if not trend:
                return False
            
            # 檢查趨勢變化
            current_direction = g_state.last_ma_reversal[self.program_index][0]
            
            # 只在方向改變時更新
            if trend != current_direction:
                g_state.last_ma_reversal[self.program_index][0] = trend
                logging.info(
                    f"{Config.PROGRAM_NAMES[self.program_index]} MA方向更新為: {trend}".encode('utf-8').decode('utf-8')
                )
            
            return True
            
        except Exception as e:
            logging.error(f"更新MA方向失敗: {str(e)}".encode('utf-8').decode('utf-8'))
            return False

    def check_price_above_ma(self, df: pd.DataFrame, lookback: int = 2) -> bool:
        """檢查價格是否在MA線上方"""
        try:
            if 'MA' not in df.columns or df.empty:
                return False
            
            # 確保數據已經按時間排序
            df = df.sort_index()
            
            recent_data = df.tail(lookback)
            if len(recent_data) < lookback:
                return False
            
            # 檢查所有指定的K線是否都在MA上方
            return all(recent_data['close'] > recent_data['MA'])
            
        except Exception as e:
            logging.error(f"檢查價格位置失敗: {str(e)}".encode('utf-8').decode('utf-8'))
            return False

    def check_price_below_ma(self, df: pd.DataFrame, lookback: int = 2) -> bool:
        """檢查價格是否在MA線下方"""
        try:
            if 'MA' not in df.columns or df.empty:
                return False
            
            # 確保數據已經按時間排序
            df = df.sort_index()
            
            recent_data = df.tail(lookback)
            if len(recent_data) < lookback:
                return False
            
            # 檢查所有指定的K線是否都在MA下方
            return all(recent_data['close'] < recent_data['MA'])
            
        except Exception as e:
            logging.error(f"檢查價格位置失敗: {str(e)}".encode('utf-8').decode('utf-8'))
            return False

    def get_current_ma_value(self, df: pd.DataFrame) -> float:
        """獲取最新的MA值"""
        try:
            if 'MA' not in df.columns or df.empty:
                return 0
            
            # 確保數據已經按時間排序
            df = df.sort_index()
            
            latest_ma = df['MA'].dropna().iloc[-1]
            return round(float(latest_ma), 1)
            
        except Exception as e:
            logging.error(f"獲取MA值失敗: {str(e)}".encode('utf-8').decode('utf-8'))
            return 0

#---(3)---MA計算模組_結束---


#---(4)---幣安交易程式_SMA 計算模組_開始---

async def calculate_sma(group_index):
    """計算移動平均線 (SMA)"""
    try:
        # 檢查 K 線數據是否足夠進行 SMA 計算
        if not kline_data[group_index] or len(kline_data[group_index]) < MA_PERIOD[group_index]:
            logger.error(f"第{group_index + 1}組程式的 K 線數據不足以計算 SMA")
            return None

        # 基於最近的 MA 週期數據計算移動平均值 (SMA)
        close_prices = [float(kline[4]) for kline in kline_data[group_index][-MA_PERIOD[group_index]:]]
        sma_value = sum(close_prices) / len(close_prices)

        logger.info(f"第{group_index + 1}組程式 - 移動平均線 (SMA): {sma_value:.2f}")
        return sma_value

    except Exception as e:
        logger.error(f"計算 SMA 時發生錯誤: {str(e)}")
        return None

async def update_sma_for_all_groups():
    """為所有組別更新 SMA 值"""
    try:
        for group_index in range(3):
            sma_value = await calculate_sma(group_index)
            if sma_value is not None:
                # 在計算完 SMA 後，更新該組的 MA 反轉狀態
                update_ma_reversal_status(group_index, sma_value)

    except Exception as e:
        logger.error(f"更新所有組別的 SMA 時發生錯誤: {str(e)}")

#---(4)---幣安交易程式_SMA 計算模組_結束---


#---(5)---幣安交易程式_交易方向和 MA 狀態管理模組_開始---

def update_trade_direction(group_index, new_direction):
    """更新交易方向"""
    try:
        if LAST_TRADE_DIRECTION[group_index][0] != new_direction:
            LAST_TRADE_DIRECTION[group_index][0] = new_direction
            logger.info(f"第{group_index + 1}組程式交易方向更新為: {new_direction}")
    except Exception as e:
        logger.error(f"更新交易方向時發生錯誤: {str(e)}")

def update_ma_reversal_status(group_index, ma_value):
    """更新 MA 線反轉狀態"""
    try:
        # 獲取最新的 K 線數據
        latest_kline = kline_data[group_index][-1]

        # 根據 MA 線與最新收盤價判斷 MA 線反轉狀態
        close_price = float(latest_kline[4])
        if close_price > ma_value:
            new_ma_reversal_status = 'UP'
        elif close_price < ma_value:
            new_ma_reversal_status = 'DOWN'
        else:
            new_ma_reversal_status = last_ma_reversal[group_index][0]  # 無變化

        # 如果 MA 線反轉狀態改變，則更新狀態
        if last_ma_reversal[group_index][0] != new_ma_reversal_status:
            last_ma_reversal[group_index][0] = new_ma_reversal_status
            logger.info(f"第{group_index + 1}組程式 MA 線反轉狀態更新為: {new_ma_reversal_status}")

    except Exception as e:
        logger.error(f"更新 MA 線反轉狀態時發生錯誤: {str(e)}")

async def manage_trade_directions():
    """管理所有組別的交易方向和 MA 狀態"""
    try:
        for group_index in range(3):
            # 計算當前 SMA 值
            sma_value = await calculate_sma(group_index)
            if sma_value is not None:
                # 更新 MA 線反轉狀態
                update_ma_reversal_status(group_index, sma_value)

                # 根據反轉狀態決定交易方向
                if last_ma_reversal[group_index][0] == 'UP':
                    update_trade_direction(group_index, 'LONG')
                elif last_ma_reversal[group_index][0] == 'DOWN':
                    update_trade_direction(group_index, 'SHORT')

    except Exception as e:
        logger.error(f"管理交易方向和 MA 狀態時發生錯誤: {str(e)}")

#---(5)---幣安交易程式_交易方向和 MA 狀態管理模組_結束---


#---(6)---報告系統模組_開始---

class ReportingSystem:
    """報告系統類"""
    def __init__(self, program_index: int):
        self.program_index = program_index
        self.program_name = Config.PROGRAM_NAMES[program_index]
        self.ma_calculator = MACalculator(program_index)
    
    def report_initialization_status(self):
        """報告初始化狀態"""
        try:
            # 獲取倉位信息
            position = self._get_position_info()
            position_status = "無持倉"
            
            if position and float(position['positionAmt']) != 0:
                side = "多倉" if float(position['positionAmt']) > 0 else "空倉"
                position_status = f"持有{side}"
            
            # 獲取交易方向
            last_trade = g_state.LAST_TRADE_DIRECTION[self.program_index][0]
            trade_direction = {
                'LONG': '買漲',
                'SHORT': '買跌',
                'IDLE': '無'
            }.get(last_trade, '無')
            
            # 獲取MA方向
            ma_direction = g_state.last_ma_reversal[self.program_index][0]
            ma_status = {
                'UP': '上升',
                'DOWN': '下降',
                '': '無'
            }.get(ma_direction, '無')
            
            # 生成報告
            report = (
                f"{self.program_name} 臺灣時間：{get_taiwan_time()}\n"
                f"倉位：{position_status}\n"
                f"上次交易方向：{trade_direction}\n"
                f"上次MA方向：{ma_status}\n"
                f"槓桿倍數：{Config.LEVERAGE[self.program_index]}\n"
                f"止盈設置：{Config.TAKE_PROFIT_PERCENT[self.program_index]}%\n"
                f"止損設置：{abs(Config.STOP_LOSS_PERCENT[self.program_index])}%"
            ).encode('utf-8').decode('utf-8')
            
            logging.info(report)
            
            # 如果是第一個程式才報告本金
            if self.program_index == 0:
                self.report_balance()
            
        except Exception as e:
            logging.error(f"生成初始化報告失敗: {str(e)}".encode('utf-8').decode('utf-8'))
    
    def report_balance(self):
        """報告帳戶本金"""
        try:
            balance_usdt, balance_twd = get_account_balance()
            
            # 檢查餘額變化是否超過閾值
            if g_state.last_balance > 0:
                change_percent = abs(balance_usdt - g_state.last_balance) / g_state.last_balance * 100
                if change_percent < Config.BALANCE_REPORT_THRESHOLD:
                    return
            
            report = (
                f"帳戶本金：{balance_usdt:.2f} USDT ({balance_twd:.0f} TWD)"
            ).encode('utf-8').decode('utf-8')
            
            logging.info(report)
            g_state.last_balance = balance_usdt
            
        except Exception as e:
            logging.error(f"生成本金報告失敗: {str(e)}".encode('utf-8').decode('utf-8'))
    
    def _get_position_info(self) -> dict:
        """獲取倉位信息"""
        try:
            positions = g_client.futures_position_information(
                symbol=Config.SYMBOL[self.program_index]
            )
            for position in positions:
                if abs(float(position['positionAmt'])) > 0:
                    return position
            return None
            
        except Exception as e:
            logging.error(f"獲取倉位信息失敗: {str(e)}".encode('utf-8').decode('utf-8'))
            return None
    
    def report_trading_disabled(self):
        """報告交易功能已禁用"""
        try:
            report = (
                f"{self.program_name} "
                f"臺灣時間：{get_taiwan_time()}，"
                f"檢測到交易信號，但交易功能已禁用"
            ).encode('utf-8').decode('utf-8')
            
            logging.info(report)
            
        except Exception as e:
            logging.error(f"生成交易禁用報告失敗: {str(e)}".encode('utf-8').decode('utf-8'))
    
    def report_system_start(self):
        """報告系統啟動"""
        try:
            report = (
                f"{self.program_name} "
                f"臺灣時間：{get_taiwan_time()}，"
                f"初始化完成"
            ).encode('utf-8').decode('utf-8')
            
            logging.info(report)
            
        except Exception as e:
            logging.error(f"生成系統啟動報告失敗: {str(e)}".encode('utf-8').decode('utf-8'))

#---(6)---報告系統模組_結束---



#---(7)---幣安交易程式_開倉和重試機制模組_開始---

async def attempt_open_position(group_index, side, max_retries=5):
    """嘗試執行開倉操作並進行重試"""
    try:
        retry_count = 0
        while retry_count < max_retries:
            if not check_api_limits():
                await asyncio.sleep(1)

            try:
                # 獲取可用餘額，用於開倉
                balance_info = await asyncio.to_thread(client.futures_account_balance)
                available_balance = float([balance['balance'] for balance in balance_info if balance['asset'] == 'USDT'][0])
                
                # 使用餘額百分比進行開倉
                use_balance = (USE_BALANCE_PERCENT[group_index] / 100) * available_balance
                quantity = use_balance / float(client.get_symbol_ticker(symbol=symbol[group_index])['price'])

                # 設置訂單參數
                order_params = {
                    'symbol': symbol[group_index],
                    'side': side,
                    'type': ORDER_TYPE_MARKET,
                    'quantity': round(quantity, 3)  # 設定開倉量
                }

                # 開倉
                order_result = await asyncio.to_thread(client.futures_create_order, **order_params)
                
                # 成功開倉後更新狀態
                CURRENT_STATE[group_index][0] = 'LONG' if side == SIDE_BUY else 'SHORT'
                LAST_TRADE_DIRECTION[group_index][0] = 'LONG' if side == SIDE_BUY else 'SHORT'
                ENTRY_BALANCE[group_index][0] = available_balance

                logger.info(f"第{group_index + 1}組程式 - 成功開倉，方向: {side}，開倉量: {quantity:.3f}，交易ID: {order_result['orderId']}")
                return True  # 成功後返回

            except BinanceAPIException as e:
                logger.error(f"第{group_index + 1}組程式 - 開倉時 Binance API 發生錯誤 (第 {retry_count + 1} 次重試): {str(e)}")
                retry_count += 1
                await asyncio.sleep(1)  # 等待 1 秒後重試

        # 如果重試次數用完仍未成功
        logger.error(f"第{group_index + 1}組程式 - 開倉失敗，已嘗試 {max_retries} 次")
        CURRENT_STATE[group_index][0] = 'IDLE'
        return False

    except Exception as e:
        logger.error(f"第{group_index + 1}組程式 - 開倉過程中發生未知錯誤: {str(e)}")
        CURRENT_STATE[group_index][0] = 'IDLE'
        return False

#---(7)---幣安交易程式_開倉和重試機制模組_結束---


#---(8)---幣安交易程式_全倉止盈止損模組_開始---

async def monitor_take_profit_and_stop_loss(group_index):
    """監控止盈和止損條件並執行相應的平倉操作"""
    try:
        if not check_api_limits():
            await asyncio.sleep(1)

        # 獲取當前持倉數據
        position_info = await asyncio.to_thread(client.futures_position_information, symbol=symbol[group_index])
        position_amt = float(position_info[0]['positionAmt'])
        entry_price = float(position_info[0]['entryPrice'])

        # 如果沒有持倉，跳過
        if position_amt == 0:
            return

        # 獲取當前市場價格
        ticker_info = await asyncio.to_thread(client.get_symbol_ticker, symbol=symbol[group_index])
        current_price = float(ticker_info['price'])

        # 計算止盈和止損價格
        if position_amt > 0:
            # 持有多單
            take_profit_price = entry_price * (1 + TAKE_PROFIT_PERCENT[group_index] / 100)
            stop_loss_price = entry_price * (1 + STOP_LOSS_PERCENT[group_index] / 100)
        else:
            # 持有空單
            take_profit_price = entry_price * (1 - TAKE_PROFIT_PERCENT[group_index] / 100)
            stop_loss_price = entry_price * (1 - STOP_LOSS_PERCENT[group_index] / 100)

        # 檢查是否達到止盈或止損條件
        if (position_amt > 0 and current_price >= take_profit_price) or (position_amt < 0 and current_price <= take_profit_price):
            # 達到止盈條件
            logger.info(f"第{group_index + 1}組程式 - 達到止盈條件，當前價格: {current_price:.2f}，止盈價格: {take_profit_price:.2f}")
            await close_position(group_index)
        elif (position_amt > 0 and current_price <= stop_loss_price) or (position_amt < 0 and current_price >= stop_loss_price):
            # 達到止損條件
            logger.info(f"第{group_index + 1}組程式 - 達到止損條件，當前價格: {current_price:.2f}，止損價格: {stop_loss_price:.2f}")
            await close_position(group_index)

    except BinanceAPIException as e:
        logger.error(f"監控止盈和止損時 Binance API 發生錯誤: {str(e)}")
    except Exception as e:
        logger.error(f"監控止盈和止損時發生未知錯誤: {str(e)}")

#---(8)---幣安交易程式_全倉止盈止損模組_結束---


#---(9)---幣安交易程式_交易報告模組_開始---

async def report_trading_signals(group_index):
    """報告交易信號和市場數據"""
    try:
        # 獲取最新的 K 線數據
        if len(kline_data[group_index]) < 5:
            logger.error(f"第{group_index + 1}組程式 K 線數據不足，無法報告交易信號")
            return

        # 報告最新的 5 個 K 線和 MA 線
        for i in range(-5, 0):
            kline = kline_data[group_index][i]
            kline_time = get_tw_time(kline[0])
            close_price = float(kline[4])

            # 計算當前 MA 值
            sma_value = await calculate_sma(group_index)
            if sma_value is None:
                logger.error(f"第{group_index + 1}組程式的 SMA 計算失敗，無法報告交易信號")
                return

            logger.info(f"第{group_index + 1}組程式（{KLINE_INTERVAL[group_index]} {MA_PERIOD[group_index]}MA）"
                        f" 臺灣時間：{kline_time.strftime('%H:%M')}，K線價格：{close_price:.1f}，MA線：{sma_value:.1f}")

    except Exception as e:
        logger.error(f"報告交易信號時發生錯誤: {str(e)}")


async def report_detailed_trade(group_index, action_type, entry_price=None, tp_price=None, sl_price=None):
    """報告詳細的交易信息"""
    try:
        # 獲取當前時間
        report_time = datetime.now() + timedelta(hours=8)
        action = "開倉" if action_type == 'OPEN' else "平倉"

        if action_type == 'OPEN':
            logger.info(f"第{group_index + 1}組程式（{KLINE_INTERVAL[group_index]} {MA_PERIOD[group_index]}MA）"
                        f" {action}時間：{report_time.strftime('%H:%M')}，開倉價格：{entry_price:.2f}，止盈價格：{tp_price:.2f}，止損價格：{sl_price:.2f}")
        elif action_type == 'CLOSE':
            logger.info(f"第{group_index + 1}組程式（{KLINE_INTERVAL[group_index]} {MA_PERIOD[group_index]}MA）"
                        f" {action}時間：{report_time.strftime('%H:%M')}，平倉價格：{entry_price:.2f}")

    except Exception as e:
        logger.error(f"報告詳細交易信息時發生錯誤: {str(e)}")


async def notify_signal_check(group_index):
    """如果 NOTIFY_SIGNAL_CHECK 為 'Y'，則報告每次新完成的 K 線價格"""
    try:
        if NOTIFY_SIGNAL_CHECK[group_index] == 'Y':
            await report_trading_signals(group_index)
    except Exception as e:
        logger.error(f"報告交易信號通知時發生錯誤: {str(e)}")

#---(9)---幣安交易程式_交易報告模組_結束---


#---(10)---幣安交易程式_錯誤恢復機制模組_開始---

async def restart_program():
    """重新啟動交易程式"""
    try:
        logger.info("重新啟動交易程式...")
        await close_global_session()
        await create_global_session()
        initialize_binance_client()
        initialize_websocket()
        logger.info("交易程式已成功重新啟動。")

    except Exception as e:
        logger.critical(f"重新啟動交易程式時發生嚴重錯誤: {str(e)}")


async def error_recovery():
    """錯誤恢復機制"""
    try:
        logger.warning("嘗試進行錯誤恢復...")
        await restart_program()

    except Exception as e:
        logger.critical(f"錯誤恢復機制中發生嚴重錯誤: {str(e)}")


async def run_main_logic():
    """主程序運行邏輯，帶有錯誤捕獲與恢復"""
    try:
        while True:
            # 管理交易方向和 MA 狀態
            await manage_trade_directions()

            # 監控止盈和止損條件
            for group_index in range(3):
                await monitor_take_profit_and_stop_loss(group_index)

            # 如果設定了交易信號報告，進行信號報告
            for group_index in range(3):
                await notify_signal_check(group_index)

            # 等待一段時間（例如 1 分鐘）後重新進行下一輪判斷
            await asyncio.sleep(60)

    except Exception as e:
        logger.error(f"主交易邏輯發生錯誤: {str(e)}")
        await error_recovery()

#---(10)---幣安交易程式_錯誤恢復機制模組_結束---


#---(11)---幣安交易程式_API 調用管理模組_開始---

api_call_counter = 0  # 記錄 API 調用次數
last_reset_time = datetime.now()

def check_api_limits():
    """檢查 Binance API 調用頻次是否符合限制"""
    global api_call_counter, last_reset_time

    # Binance 的 API 調用限制為每分鐘不超過 1200 次 (實際可根據具體賬戶要求修改)
    current_time = datetime.now()
    elapsed_seconds = (current_time - last_reset_time).total_seconds()

    if elapsed_seconds >= 60:
        # 每分鐘重置計數器
        api_call_counter = 0
        last_reset_time = current_time

    if api_call_counter < 1200:
        api_call_counter += 1
        return True
    else:
        logger.warning("API 調用頻次已達上限，暫停 1 秒")
        return False

async def track_api_call():
    """追蹤 API 調用次數並進行限流管理"""
    try:
        if not check_api_limits():
            # 等待 1 秒，防止超過調用次數限制
            await asyncio.sleep(1)
        else:
            logger.info(f"API 調用次數已增加: {api_call_counter}")
    except Exception as e:
        logger.error(f"追蹤 API 調用次數時發生錯誤: {str(e)}")

#---(11)---幣安交易程式_API 調用管理模組_結束---


#---(12)---幣安交易程式_心跳機制模組_開始---

async def start_heartbeat():
    """啟動心跳機制以保持 WebSocket 連接"""
    try:
        while True:
            # 發送心跳包，檢查 WebSocket 連接狀態
            if websocket_manager is not None and websocket_manager.ws_connection and websocket_manager.ws_connection.connected:
                # WebSocket 連接狀態正常，發送心跳包
                await websocket_manager.send_heartbeat()
                logger.info("心跳包已發送，保持 WebSocket 連接活躍")
            else:
                # 若連接中斷，重新連接
                logger.warning("WebSocket 連接失敗，嘗試重新連接...")
                await reconnect_websocket()

            # 每 30 秒發送一次心跳包
            await asyncio.sleep(30)

    except Exception as e:
        logger.error(f"心跳機制中發生錯誤: {str(e)}")
        # 若發生錯誤，嘗試恢復 WebSocket 連接
        await reconnect_websocket()

async def reconnect_websocket():
    """嘗試重新連接 WebSocket"""
    try:
        logger.info("重新連接 WebSocket 中...")
        await close_global_session()  # 關閉舊的 WebSocket 連接
        await create_global_session()  # 創建新的 WebSocket 會話
        initialize_websocket()  # 初始化 WebSocket
        logger.info("WebSocket 已成功重新連接")

    except Exception as e:
        logger.critical(f"重新連接 WebSocket 時發生嚴重錯誤: {str(e)}")

#---(12)---幣安交易程式_心跳機制模組_結束---


#---(13)---幣安交易程式_主程序模組_開始---

async def main():
    """主程序負責初始化並運行所有模組"""
    try:
        logger.info("開始初始化幣安交易程式...")

        # 初始化 Binance 客戶端及全局資源
        initialize_binance_client()
        await create_global_session()

        # 初始化交易模組
        await initialize_trading_programs()

        # 啟動交易信號監控
        logger.info("交易信號監控開始...")

        # 啟動心跳機制
        asyncio.create_task(start_heartbeat())

        # 統一報告自動交易程式開始
        logger.info("所有交易模組初始化完畢，自動交易程式開始監測交易信號")

        # 啟動主交易邏輯循環
        await run_main_logic()

    except Exception as e:
        logger.critical(f"主程序發生嚴重錯誤: {str(e)}")
        await error_recovery()

async def initialize_trading_programs():
    """初始化交易組別的資源及數據"""
    try:
        logger.info("開始初始化 3 組交易程式...")
        
        for group_index in range(3):
            if not check_api_limits():
                await asyncio.sleep(1)

            # 獲取 K 線數據
            kline_data[group_index] = await asyncio.to_thread(
                client.get_klines,
                symbol=symbol[group_index],
                interval=KLINE_INTERVAL[group_index],
                limit=1000
            )

            # 獲取帳戶資訊
            account_info = await asyncio.to_thread(client.futures_account_balance)
            available_balance = float([balance['balance'] for balance in account_info if balance['asset'] == 'USDT'][0])
            ENTRY_BALANCE[group_index][0] = available_balance

            # 設置初始交易方向與 MA 反轉狀態
            LAST_TRADE_DIRECTION[group_index][0] = os.environ.get(f'LAST_TRADE_DIRECTION_{group_index + 1}', 'IDLE')
            last_ma_reversal[group_index][0] = 'UNKNOWN'  # 初始狀態設為 UNKNOWN

            # 報告每組程式的初始化情況
            logger.info(f"第{group_index + 1}組程式（{KLINE_INTERVAL[group_index]} K 線 {MA_PERIOD[group_index]} MA）："
                        f"帳戶餘額：{available_balance:.2f} USDT，交易方向：{LAST_TRADE_DIRECTION[group_index][0]}，MA 方向：{last_ma_reversal[group_index][0]}")

            # 報告最近 5 個 K 線和 MA 值
            await report_trading_signals(group_index)

        logger.info("所有 3 組交易程式已成功初始化。")

    except Exception as e:
        logger.error(f"初始化交易程序時發生錯誤: {str(e)}")
        await error_recovery()

if __name__ == "__main__":
    try:
        # 啟動 asyncio 主循環
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("交易程式已手動停止。")

#---(13)---幣安交易程式_主程序模組_結束---
