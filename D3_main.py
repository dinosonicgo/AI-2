import os
import threading
import time
import math
import pandas as pd
import numpy as np
from binance.client import Client
from binance.exceptions import BinanceAPIException
from binance import enums
from datetime import datetime, timedelta
from queue import Queue
import json
import logging
import asyncio
import aiohttp
import websockets
import sys
import traceback
from flask import Flask

# 設置日誌
class CustomFormatter(logging.Formatter):
    def format(self, record):
        return record.getMessage()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(CustomFormatter())
logger.handlers = [handler]

# 交易對設置
symbol = ['BTCUSDT', 'BTCUSDT', 'BTCUSDT']

# 全域變數
CURRENT_STATE = [['IDLE'], ['IDLE'], ['IDLE']]  # 當前交易狀態
LAST_TRADE_DIRECTION = [
    [os.environ.get(f'LAST_TRADE_DIRECTION_{i+1}', 'IDLE')] for i in range(3)
]
LAST_TRADE_DIRECTION_INPUT = [[""], [""], [""]]  # 手動設置的交易方向
last_ma_reversal = [[''], [''], ['']]  # MA線反轉方向
ENTRY_BALANCE = [[0.0], [0.0], [0.0]]  # 入場時的餘額
time_offset = 0  # 時間偏移量
local_balance = 0.0  # 本地餘額記錄
local_position_amt = [[0.0], [0.0], [0.0]]  # 本地持倉量記錄
local_entry_price = [[0.0], [0.0], [0.0]]  # 本地入場價格記錄
last_sync_time = 0  # 最後同步時間

# 可調整參數
LEVERAGE = [35, 35, 35]  # 槓桿倍數
TAKE_PROFIT_PERCENT = [7, 7, 10]  # 止盈百分比
STOP_LOSS_PERCENT = [-80, -80, -80]  # 止損百分比
USE_BALANCE_PERCENT = [99, 99, 99]  # 使用餘額百分比
TRADING_ENABLED = ['N', 'N', 'N']  # 是否啟用交易
USE_MARKET_TAKE_PROFIT = ['Y', 'Y', 'Y']  # 是否使用市價止盈
USE_MARKET_TP_WHEN_REACHED = ['Y', 'Y', 'Y']  # 到達止盈價格時是否使用市價單
KLINE_INTERVAL = ['15m', '3m', '30m']  # K線時間間隔
MA_PERIOD = [979, 20, 726]  # MA線週期
NOTIFY_SIGNAL_CHECK = ['N', 'Y', 'N']  # 是否發送信號通知
USE_TESTNET = 'N'  # 是否使用測試網
REPORT_DETAILED_SIGNAL = ['N', 'N', 'N']  # 是否報告詳細信號

# 定義程式名稱
PROGRAM_NAMES = [
    f"第{i+1}組程式（{KLINE_INTERVAL[i]} {MA_PERIOD[i]}MA）"
    for i in range(3)
]

# 訂單類型常量
SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'
ORDER_TYPE_MARKET = 'MARKET'
ORDER_TYPE_LIMIT = 'LIMIT'
ORDER_TYPE_STOP = 'STOP'
ORDER_TYPE_TAKE_PROFIT = 'TAKE_PROFIT'
ORDER_TYPE_STOP_MARKET = 'STOP_MARKET'
ORDER_TYPE_TAKE_PROFIT_MARKET = 'TAKE_PROFIT_MARKET'
TIME_IN_FORCE_GTC = 'GTC'

# API調用管理
api_call_counter = {'second': 0, 'minute': 0}
api_call_time = {'second': time.time(), 'minute': time.time()}
API_LIMITS = {'second': 5, 'minute': 900}

def check_api_limits():
    """檢查API調用限制"""
    current_time = time.time()
    
    # 重置秒計數器
    if current_time - api_call_time['second'] >= 1:
        api_call_counter['second'] = 0
        api_call_time['second'] = current_time
    
    # 重置分鐘計數器
    if current_time - api_call_time['minute'] >= 60:
        api_call_counter['minute'] = 0
        api_call_time['minute'] = current_time
    
    # 檢查限制
    if (api_call_counter['second'] >= API_LIMITS['second'] or 
        api_call_counter['minute'] >= API_LIMITS['minute']):
        return False
    
    # 更新計數器
    api_call_counter['second'] += 1
    api_call_counter['minute'] += 1
    return True

# 使用合約交易
FUTURES_TRADING = True

# Flask應用
app = Flask(__name__)
message_queue = Queue()
all_messages = []
MAX_MESSAGES = 100

# WebSocket 全域變數
kline_data = [[], [], []]

# 從環境變數中讀取 API 密鑰
api_key = os.getenv('D3_key')
api_secret = os.getenv('D3_secret')

if not api_key or not api_secret:
    logger.error("未找到 API 密鑰。請確保環境變數 'D1_key' 和 'D1_secret' 已正確設置。")
    raise ValueError("未找到 API 密鑰。")

# 初始化 Binance 客戶端
client = Client(api_key, api_secret)

# 全局會話
global_session = None

async def create_global_session():
    """創建全局aiohttp會話"""
    global global_session
    if global_session is None or global_session.closed:
        global_session = aiohttp.ClientSession()

async def close_global_session():
    """關閉全局aiohttp會話"""
    global global_session
    if global_session and not global_session.closed:
        await global_session.close()

def get_tw_time(timestamp_ms):
    """轉換時間戳為台灣時間"""
    return datetime.fromtimestamp(timestamp_ms/1000) + timedelta(hours=8)

def format_price(price):
    """格式化價格顯示"""
    return f"{float(price):.1f}"

def get_interval_minutes(interval):
    """獲取K線間隔的分鐘數"""
    interval_map = {
        '1m': 1, '3m': 3, '5m': 5,
        '15m': 15, '30m': 30, '1h': 60
    }
    return interval_map.get(interval, 0)

async def initialize_trade_directions():
    """初始化交易方向和MA狀態"""
    try:
        for i in range(3):
            # 檢查LAST_TRADE_DIRECTION_INPUT是否有值
            if LAST_TRADE_DIRECTION_INPUT[i][0]:
                LAST_TRADE_DIRECTION[i][0] = LAST_TRADE_DIRECTION_INPUT[i][0]
            elif os.environ.get(f'LAST_TRADE_DIRECTION_{i+1}'):
                LAST_TRADE_DIRECTION[i][0] = os.environ.get(f'LAST_TRADE_DIRECTION_{i+1}')
            else:
                # 檢查API限制
                if not check_api_limits():
                    await asyncio.sleep(1)
                
                # 獲取當前倉位資訊
                position_info = await asyncio.to_thread(
                    client.futures_position_information,
                    symbol=symbol[i]
                )
                pos_amt = float(position_info[0]['positionAmt'])
                if pos_amt > 0:
                    LAST_TRADE_DIRECTION[i][0] = 'LONG'
                elif pos_amt < 0:
                    LAST_TRADE_DIRECTION[i][0] = 'SHORT'
                else:
                    LAST_TRADE_DIRECTION[i][0] = 'IDLE'

            logger.info(f"第{i+1}組程式初始化完成 - LAST_TRADE_DIRECTION: {LAST_TRADE_DIRECTION[i][0]}, " 
                       f"last_ma_reversal: {last_ma_reversal[i][0]}")

    except Exception as e:
        logger.error(f"初始化交易方向時發生錯誤：{str(e)}")

#---(1)---幣安交易程式_初始化模組_結束---


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


#---(3)---幣安交易程式_K 線資料管理模組_開始---

async def manage_kline_data():
    """管理 K 線數據的獲取、存儲和處理"""
    try:
        logger.info("開始 K 線數據管理...")

        # 獲取每個交易組別的 K 線數據
        for group_index in range(3):
            if not check_api_limits():
                await asyncio.sleep(1)

            kline_data[group_index] = await asyncio.to_thread(
                client.get_klines,
                symbol=symbol[group_index],
                interval=KLINE_INTERVAL[group_index],
                limit=1000
            )

            # 檢查 K 線數據是否足夠
            if not kline_data[group_index] or len(kline_data[group_index]) < 1000:
                logger.error(f"第{group_index + 1}組程式獲取的 K 線數據不足，長度為 {len(kline_data[group_index])}")
                continue

            logger.info(f"第{group_index + 1}組程式 K 線數據獲取成功，數據長度為 {len(kline_data[group_index])}")

            # 保存最新的 1000 條 K 線數據
            # 這裡我們將數據保存到當前列表中，並確保每次更新後最多保留 1000 條數據
            kline_data[group_index] = kline_data[group_index][-1000:]

            # 計算最新的移動平均值 (SMA)
            close_prices = [float(kline[4]) for kline in kline_data[group_index]]
            ma_value = sum(close_prices[-MA_PERIOD[group_index]:]) / len(close_prices[-MA_PERIOD[group_index]:])
            logger.info(f"第{group_index + 1}組程式 - 最新的移動平均值 (SMA): {ma_value:.2f}")

            # 更新 MA 線反轉狀態
            update_ma_reversal_status(group_index, ma_value)

    except BinanceAPIException as e:
        logger.error(f"K 線數據管理時 Binance API 發生錯誤: {str(e)}")
    except Exception as e:
        logger.error(f"K 線數據管理時發生未知錯誤: {str(e)}")


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

        if last_ma_reversal[group_index][0] != new_ma_reversal_status:
            last_ma_reversal[group_index][0] = new_ma_reversal_status
            logger.info(f"第{group_index + 1}組程式 MA 線反轉狀態更新為: {new_ma_reversal_status}")

    except Exception as e:
        logger.error(f"更新 MA 線反轉狀態時發生錯誤: {str(e)}")

#---(3)---幣安交易程式_K 線資料管理模組_結束---


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


#---(6)---幣安交易程式_交易判斷邏輯模組_開始---

async def evaluate_trade_signal(group_index):
    """基於 K 線和 MA 線數據評估交易信號"""
    try:
        # 獲取當前 SMA 值
        sma_value = await calculate_sma(group_index)
        if sma_value is None:
            logger.error(f"第{group_index + 1}組程式的 SMA 計算失敗，無法評估交易信號")
            return

        # 獲取最新的 K 線數據
        latest_kline = kline_data[group_index][-1]
        close_price = float(latest_kline[4])  # 收盤價

        # 判斷開倉信號
        if close_price > sma_value and CURRENT_STATE[group_index][0] == 'IDLE':
            # 當前無持倉且收盤價高於 SMA，判定買入
            logger.info(f"第{group_index + 1}組程式 - 生成買入信號，收盤價: {close_price:.2f}，SMA: {sma_value:.2f}")
            await open_position(group_index, SIDE_BUY)

        elif close_price < sma_value and CURRENT_STATE[group_index][0] == 'IDLE':
            # 當前無持倉且收盤價低於 SMA，判定賣出
            logger.info(f"第{group_index + 1}組程式 - 生成賣出信號，收盤價: {close_price:.2f}，SMA: {sma_value:.2f}")
            await open_position(group_index, SIDE_SELL)

        # 判斷平倉信號
        if CURRENT_STATE[group_index][0] == 'LONG' and close_price < sma_value:
            # 當前持有多單且收盤價跌破 SMA，平倉
            logger.info(f"第{group_index + 1}組程式 - 多單平倉信號觸發，收盤價: {close_price:.2f}，SMA: {sma_value:.2f}")
            await close_position(group_index)

        elif CURRENT_STATE[group_index][0] == 'SHORT' and close_price > sma_value:
            # 當前持有空單且收盤價高於 SMA，平倉
            logger.info(f"第{group_index + 1}組程式 - 空單平倉信號觸發，收盤價: {close_price:.2f}，SMA: {sma_value:.2f}")
            await close_position(group_index)

    except Exception as e:
        logger.error(f"評估交易信號時發生錯誤: {str(e)}")


async def open_position(group_index, side):
    """執行開倉操作"""
    try:
        if not check_api_limits():
            await asyncio.sleep(1)

        # 獲取可用餘額，用於開倉
        balance_info = await asyncio.to_thread(client.futures_account_balance)
        available_balance = float([balance['balance'] for balance in balance_info if balance['asset'] == 'USDT'][0])
        
        # 使用餘額百分比進行開倉
        use_balance = (USE_BALANCE_PERCENT[group_index] / 100) * available_balance
        market_price = float(client.get_symbol_ticker(symbol=symbol[group_index])['price'])
        quantity = use_balance / market_price

        # 設置訂單參數
        order_params = {
            'symbol': symbol[group_index],
            'side': side,
            'type': ORDER_TYPE_MARKET,
            'quantity': round(quantity, 3)  # 設定開倉量，精度控制
        }

        # 發送開倉訂單
        order_result = await asyncio.to_thread(client.futures_create_order, **order_params)
        
        # 成功開倉後更新狀態
        CURRENT_STATE[group_index][0] = 'LONG' if side == SIDE_BUY else 'SHORT'
        LAST_TRADE_DIRECTION[group_index][0] = 'LONG' if side == SIDE_BUY else 'SHORT'
        ENTRY_BALANCE[group_index][0] = available_balance

        logger.info(f"第{group_index + 1}組程式 - 成功執行開倉操作，方向: {CURRENT_STATE[group_index][0]}，開倉量: {quantity:.3f}，交易ID: {order_result['orderId']}")

    except BinanceAPIException as e:
        logger.error(f"第{group_index + 1}組程式 - 開倉時 Binance API 發生錯誤: {str(e)}")
        CURRENT_STATE[group_index][0] = 'IDLE'  # 開倉失敗，重設狀態

    except Exception as e:
        logger.error(f"開倉時發生未知錯誤: {str(e)}")
        CURRENT_STATE[group_index][0] = 'IDLE'  # 開倉失敗，重設狀態


async def close_position(group_index):
    """執行平倉操作"""
    try:
        if not check_api_limits():
            await asyncio.sleep(1)

        # 獲取當前持倉量
        position_info = await asyncio.to_thread(client.futures_position_information, symbol=symbol[group_index])
        position_amt = float(position_info[0]['positionAmt'])
        side = SIDE_SELL if position_amt > 0 else SIDE_BUY

        # 設置訂單參數
        order_params = {
            'symbol': symbol[group_index],
            'side': side,
            'type': ORDER_TYPE_MARKET,
            'quantity': abs(position_amt)  # 平倉量應等於目前持倉量
        }

        # 發送平倉訂單
        order_result = await asyncio.to_thread(client.futures_create_order, **order_params)

        # 成功平倉後更新狀態
        CURRENT_STATE[group_index][0] = 'IDLE'
        logger.info(f"第{group_index + 1}組程式 - 成功執行平倉操作，方向: IDLE，交易ID: {order_result['orderId']}")

    except BinanceAPIException as e:
        logger.error(f"第{group_index + 1}組程式 - 平倉時 Binance API 發生錯誤: {str(e)}")

    except Exception as e:
        logger.error(f"平倉時發生未知錯誤: {str(e)}")

#---(6)---幣安交易程式_交易判斷邏輯模組_結束---


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
