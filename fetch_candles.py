import websocket
import json
import time
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# WebSocket endpoint
socket = 'wss://data.tradingview.com/socket.io/websocket'

# Global dictionary to store all symbol data
all_symbols_data = {}

# Your symbols list (truncated for brevity - include your full list)
symbols = [
    "BTCUSDT", "ETHUSDT", "XRPUSDT", "BCHUSDT", "LTCUSDT",
    "ADAUSDT", "ETCUSDT", "LINKUSDT", "TRXUSDT", "DOTUSDT",
    # ... include all your symbols here
    "MSFTUSDT"
]

# Static inputs
resolution = "60"
limit = 5000


def create_msg(ws, fun, arg):
    """Utility to wrap and send TradingView messages"""
    msg_obj = {"m": fun, "p": arg}
    msg_str = json.dumps(msg_obj)
    framed = f"~m~{len(msg_str)}~m~{msg_str}"
    ws.send(framed)


def fetch_candle_data(symbol):
    """Fetch candle data for a single symbol"""
    logger.info(f"📡 Fetching data for {symbol}...")

    try:
        ws = websocket.create_connection(socket)
        session_id = "cs_vOn7C11YGKmD"

        # Step 1: Create chart session
        create_msg(ws, 'chart_create_session', [session_id, ""])

        # Step 2: Resolve symbol
        payload = {
            "symbol": f"BITGET:{symbol}.P",
            "adjustment": "splits",
            "session": "regular",
            "currency-id": "XTVCUSDT",
        }
        param = f'={json.dumps(payload)}'
        create_msg(ws, 'resolve_symbol', [session_id, "sds_sym_1", param])

        # Step 3: Create series
        create_msg(ws, 'create_series', [session_id, "sds_1", "s1", "sds_sym_1", resolution, limit])

        # Step 4: Receive and process data
        candle_data = []
        data_received = False
        timeout = 15
        start_time = time.time()

        while True:
            if time.time() - start_time > timeout:
                logger.warning(f"❌ Timeout reached for {symbol}.")
                break

            try:
                res = ws.recv()
            except Exception as e:
                logger.error(f"❌ WebSocket error for {symbol}: {e}")
                break

            if not res or res.startswith('~m~0~m~') or 'ping' in res.lower():
                continue

            if "error" in res.lower():
                logger.error(f"❌ Server error for {symbol}: {res[:200]}")
                break

            # Parse the message
            messages = []
            parts = res.split('~m~')
            for part in parts:
                if part and not part.isdigit():
                    try:
                        message = json.loads(part)
                        messages.append(message)
                    except:
                        continue

            for message in messages:
                if message.get('m') == 'timescale_update':
                    try:
                        series_data = message['p'][1]['sds_1']['s']
                        logger.info(f"✅ Found {len(series_data)} candles for {symbol}.")

                        for candle in series_data:
                            if 'v' in candle and len(candle['v']) >= 6:
                                timestamp, open_price, high, low, close, volume = candle['v']

                                # Convert timestamp
                                if isinstance(timestamp, (int, float)) and timestamp > 1e12:
                                    start_at = int(timestamp / 1000)
                                else:
                                    start_at = int(timestamp)

                                # Create candle in specified format
                                candle_formatted = {
                                    "symbol": symbol,
                                    "period": resolution,
                                    "start_at": start_at,
                                    "open": open_price,
                                    "high": high,
                                    "low": low,
                                    "close": close
                                }
                                candle_data.append(candle_formatted)

                        data_received = True
                    except (KeyError, IndexError, ValueError) as e:
                        logger.error(f"⚠️ Error parsing data for {symbol}: {e}")

                elif message.get('m') == 'series_completed':
                    logger.info(f"✅ Series completed for {symbol}.")
                    break

            if data_received and len(candle_data) >= limit:
                logger.info(f"✅ Received all {limit} candles for {symbol}.")
                break

        ws.close()

        # Add to global dictionary
        if candle_data:
            all_symbols_data[symbol] = candle_data
            logger.info(f"✅ Added {symbol} to data store: {len(candle_data)} candles")
            return True
        else:
            logger.warning(f"❌ No data collected for {symbol}")
            return False

    except Exception as e:
        logger.error(f"❌ Unexpected error fetching {symbol}: {e}")
        return False


def save_all_data_to_json():
    """Save all collected symbol data to a single JSON file"""
    if all_symbols_data:
        filename = "1_price_list.json"
        try:
            with open(filename, 'w') as f:
                json.dump(all_symbols_data, f, indent=4)

            logger.info(f"🎯 All data saved successfully to {filename}")
            logger.info(f"📊 Total symbols: {len(all_symbols_data)}")

            # Log summary
            for symbol, candles in all_symbols_data.items():
                logger.info(f"   {symbol}: {len(candles)} candles")

            return True

        except Exception as e:
            logger.error(f"❌ Failed to save JSON file: {e}")
            return False
    else:
        logger.error("❌ No data was collected, file not saved.")
        return False


def fetch_all_candles():
    """Fetch candles for all symbols and save to file"""
    logger.info("🚀 Starting candle data fetching for all symbols")
    logger.info(f"📊 Total symbols to fetch: {len(symbols)}")

    successful_fetches = 0
    for symbol in symbols:
        if fetch_candle_data(symbol):
            successful_fetches += 1
        time.sleep(2)  # Rate limiting

    # Save all data to single file
    success = save_all_data_to_json()

    logger.info(f"🎯 Fetching completed: {successful_fetches}/{len(symbols)} symbols successful")
    return success


if __name__ == "__main__":
    fetch_all_candles()