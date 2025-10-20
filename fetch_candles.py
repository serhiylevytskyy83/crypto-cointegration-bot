import websocket
import json
import numpy as np
import pandas as pd
from datetime import datetime
import os
import time

# WebSocket endpoint
socket = 'wss://data.tradingview.com/socket.io/websocket'

# Global dictionaries to store data in both formats
all_symbols_data = {}  # Original JSON format
all_symbols_numpy = {}  # NEW: NumPy format for fast processing


# Utility to wrap and send TradingView messages
def create_msg(ws, fun, arg):
    msg_obj = {"m": fun, "p": arg}
    msg_str = json.dumps(msg_obj)
    framed = f"~m~{len(msg_str)}~m~{msg_str}"
    ws.send(framed)


# Symbols to fetch
symbols = [
    "BTCUSDT",
  "ETHUSDT",
  "XRPUSDT",
  "BCHUSDT",
  "LTCUSDT",
  "ADAUSDT",
  "ETCUSDT",
  "LINKUSDT",
  "TRXUSDT",
  "DOTUSDT",
  "DOGEUSDT",
  "SOLUSDT",
  "BNBUSDT",
  "UNIUSDT",
  "ICPUSDT",
  "AAVEUSDT",
  "FILUSDT",
  "XLMUSDT",
  "ATOMUSDT",
  "XTZUSDT",
  "SUSHIUSDT",
  "AXSUSDT",
  "THETAUSDT",
  "AVAXUSDT",
  "SHIBUSDT",
  "MANAUSDT",
  "GALAUSDT",
  "SANDUSDT",
  "DYDXUSDT",
  "CRVUSDT",
  "NEARUSDT",
  "EGLDUSDT",
  "KSMUSDT",
  "ARUSDT",
  "PEOPLEUSDT",
  "LRCUSDT",
  "NEOUSDT",
  "ALICEUSDT",
  "WAVESUSDT",
  "ALGOUSDT",
  "IOTAUSDT",
  "ENJUSDT",
  "GMTUSDT",
  "ZILUSDT",
  "IOSTUSDT",
  "APEUSDT",
  "RUNEUSDT",
  "KNCUSDT",
  "APTUSDT",
  "CHZUSDT",
  "ROSEUSDT",
  "ZRXUSDT",
  "KAVAUSDT",
  "ENSUSDT",
  "MTLUSDT",
  "AUDIOUSDT",
  "SXPUSDT",
  "C98USDT",
  "OPUSDT",
  "RSRUSDT",
  "SNXUSDT",
  "STORJUSDT",
  "1INCHUSDT",
  "COMPUSDT",
  "IMXUSDT",
  "LUNAUSDT",
  "FLOWUSDT",
  "TRBUSDT",
  "QTUMUSDT",
  "API3USDT",
  "MASKUSDT",
  "WOOUSDT",
  "GRTUSDT",
  "BANDUSDT",
  "STGUSDT",
  "LUNCUSDT",
  "ONEUSDT",
  "JASMYUSDT",
  "BATUSDT",
  "MAGICUSDT",
  "ALPHAUSDT",
  "LDOUSDT",
  "CELOUSDT",
  "BLURUSDT",
  "MINAUSDT",
  "COREUSDT",
  "CFXUSDT",
  "ASTRUSDT",
  "GMXUSDT",
  "ANKRUSDT",
  "ACHUSDT",
  "FETUSDT",
  "FXSUSDT",
  "HOOKUSDT",
  "SSVUSDT",
  "USDCUSDT",
  "LQTYUSDT",
  "STXUSDT",
  "TRUUSDT",
  "HBARUSDT",
  "INJUSDT",
  "BELUSDT",
  "COTIUSDT",
  "VETUSDT",
  "ARBUSDT",
  "LOOKSUSDT",
  "KAIAUSDT",
  "FLMUSDT",
  "CKBUSDT",
  "IDUSDT",
  "JOEUSDT",
  "TLMUSDT",
  "HOTUSDT",
  "CHRUSDT",
  "RDNTUSDT",
  "ICXUSDT",
  "HFTUSDT",
  "ONTUSDT",
  "NKNUSDT",
  "ARPAUSDT",
  "SFPUSDT",
  "CTSIUSDT",
  "SKLUSDT",
  "RVNUSDT",
  "CELRUSDT",
  "FLOKIUSDT",
  "SPELLUSDT",
  "SUIUSDT",
  "PEPEUSDT",
  "IOTXUSDT",
  "CTKUSDT",
  "UMAUSDT",
  "TURBOUSDT",
  "BSVUSDT",
  "TONUSDT",
  "GTCUSDT",
  "DENTUSDT",
  "ZENUSDT",
  "PHBUSDT",
  "ORDIUSDT",
  "SLPUSDT",
  "1000BONKUSDT",
  "USTCUSDT",
  "RADUSDT",
  "QNTUSDT",
  "MAVUSDT",
  "MDTUSDT",
  "XVGUSDT",
  "1000XECUSDT",
  "AGLDUSDT",
  "WLDUSDT",
  "PENDLEUSDT",
  "ARKMUSDT",
  "CVXUSDT",
  "YGGUSDT",
  "OGNUSDT",
  "LPTUSDT",
  "BNTUSDT",
  "SEIUSDT",
  "CYBERUSDT",
  "BAKEUSDT",
  "BIGTIMEUSDT",
  "WAXPUSDT",
  "POLYXUSDT",
  "TIAUSDT",
  "MEMEUSDT",
  "PYTHUSDT",
  "JTOUSDT",
  "1000SATSUSDT",
  "1000RATSUSDT",
  "ACEUSDT",
  "XAIUSDT",
  "MANTAUSDT",
  "ALTUSDT",
  "JUPUSDT",
  "ZETAUSDT",
  "STRKUSDT",
  "PIXELUSDT",
  "DYMUSDT",
  "WIFUSDT",
  "AXLUSDT",
  "BEAMUSDT",
  "BOMEUSDT",
  "METISUSDT",
  "NFPUSDT",
  "VANRYUSDT",
  "AEVOUSDT",
  "ETHFIUSDT",
  "OMUSDT",
  "ONDOUSDT",
  "CAKEUSDT",
  "PORTALUSDT",
  "NTRNUSDT",
  "KASUSDT",
  "AIUSDT",
  "ENAUSDT",
  "WUSDT",
  "CVCUSDT",
  "TNSRUSDT",
  "SAGAUSDT",
  "TAOUSDT",
  "RAYUSDT",
  "ATAUSDT",
  "SUPERUSDT",
  "ONGUSDT",
  "LSKUSDT",
  "GLMUSDT",
  "REZUSDT",
  "XVSUSDT",
  "MOVRUSDT",
  "BBUSDT",
  "NOTUSDT",
  "BICOUSDT",
  "HIFIUSDT",
  "IOUSDT",
  "TAIKOUSDT",
  "BRETTUSDT",
  "ATHUSDT",
  "ZKUSDT",
  "MEWUSDT",
  "LISTAUSDT",
  "ZROUSDT",
  "BLASTUSDT",
  "DOGUSDT",
  "PAXGUSDT",
  "ZKJUSDT",
  "BGBUSDT",
  "MOCAUSDT",
  "GASUSDT",
  "UXLINKUSDT",
  "BANANAUSDT",
  "MYROUSDT",
  "POPCATUSDT",
  "PRCLUSDT",
  "AVAILUSDT",
  "RENDERUSDT",
  "RAREUSDT",
  "PONKEUSDT",
  "TUSDT",
  "1000000MOGUSDT",
  "GUSDT",
  "SYNUSDT",
  "SYSUSDT",
  "VOXELUSDT",
  "SUNUSDT",
  "DOGSUSDT",
  "ORDERUSDT",
  "SUNDOGUSDT",
  "AKTUSDT",
  "MBOXUSDT",
  "HNTUSDT",
  "CHESSUSDT",
  "FLUXUSDT",
  "POLUSDT",
  "NEIROETHUSDT",
  "RPLUSDT",
  "QUICKUSDT",
  "AERGOUSDT",
  "1MBABYDOGEUSDT",
  "1000CATUSDT",
  "KDAUSDT",
  "FIDAUSDT",
  "CATIUSDT",
  "FIOUSDT",
  "ARKUSDT",
  "GHSTUSDT",
  "VELOUSDT",
  "HMSTRUSDT",
  "AGIUSDT",
  "REIUSDT",
  "COSUSDT",
  "EIGENUSDT",
  "MOODENGUSDT",
  "DIAUSDT",
  "OGUSDT",
  "NEIROCTOUSDT",
  "ETHWUSDT",
  "DEGENUSDT",
  "KMNOUSDT",
  "POWRUSDT",
  "PYRUSDT",
  "CARVUSDT",
  "SLERFUSDT",
  "PUFFERUSDT",
  "DEEPUSDT",
  "DBRUSDT",
  "LUMIAUSDT",
  "SCRUSDT",
  "GOATUSDT",
  "XUSDT",
  "SAFEUSDT",
  "GRASSUSDT",
  "SWEATUSDT",
  "SANTOSUSDT",
  "SPXUSDT",
  "VIRTUALUSDT",
  "AEROUSDT",
  "CETUSUSDT",
  "COWUSDT",
  "SWELLUSDT",
  "DRIFTUSDT",
  "PNUTUSDT",
  "ACTUSDT",
  "CROUSDT",
  "PEAQUSDT",
  "FWOGUSDT",
  "HIPPOUSDT",
  "SNTUSDT",
  "MERLUSDT",
  "STEEMUSDT",
  "BANUSDT",
  "OLUSDT",
  "MORPHOUSDT",
  "SCRTUSDT",
  "CHILLGUYUSDT",
  "1MCHEEMSUSDT",
  "OXTUSDT",
  "ZRCUSDT",
  "THEUSDT",
  "MAJORUSDT",
  "CTCUSDT",
  "XDCUSDT",
  "XIONUSDT",
  "ORCAUSDT",
  "ACXUSDT",
  "NSUSDT",
  "MOVEUSDT",
  "KOMAUSDT",
  "MEUSDT",
  "VELODROMEUSDT",
  "AVAUSDT",
  "DEGOUSDT",
  "VANAUSDT",
  "HYPEUSDT",
  "PENGUUSDT",
  "USUALUSDT",
  "FUELUSDT",
  "CGPTUSDT",
  "AIXBTUSDT",
  "FARTCOINUSDT",
  "HIVEUSDT",
  "DEXEUSDT",
  "GIGAUSDT",
  "PHAUSDT",
  "DFUSDT",
  "AI16ZUSDT",
  "GRIFFAINUSDT",
  "ZEREBROUSDT",
  "BIOUSDT",
  "SWARMSUSDT",
  "ALCHUSDT",
  "COOKIEUSDT",
  "SONICUSDT",
  "AVAAIUSDT",
  "FLOCKUSDT",
  "SUSDT",
  "PROMUSDT",
  "DUCKUSDT",
  "BGSCUSDT",
  "SOLVUSDT",
  "ARCUSDT",
  "PIPPINUSDT",
  "TRUMPUSDT",
  "MELANIAUSDT",
  "PLUMEUSDT",
  "TESTCUSDT",
  "VTHOUSDT",
  "JUSDT",
  "VINEUSDT",
  "ANIMEUSDT",
  "XCNUSDT",
  "TOSHIUSDT",
  "VVVUSDT",
  "JELLYJELLYUSDT",
  "FORTHUSDT",
  "BERAUSDT",
  "TSTBSCUSDT",
  "10000ELONUSDT",
  "LAYERUSDT",
  "B3USDT",
  "IPUSDT",
  "RONUSDT",
  "HEIUSDT",
  "SHELLUSDT",
  "BROCCOLIUSDT",
  "AUCTIONUSDT",
  "GPSUSDT",
  "GNOUSDT",
  "AIOZUSDT",
  "PIUSDT",
  "AVLUSDT",
  "KAITOUSDT",
  "GODSUSDT",
  "ROAMUSDT",
  "REDUSDT",
  "ELXUSDT",
  "SERAPHUSDT",
  "BMTUSDT",
  "VICUSDT",
  "EPICUSDT",
  "OBTUSDT",
  "MUBARAKUSDT",
  "NMRUSDT",
  "TUTUSDT",
  "FORMUSDT",
  "RSS3USDT",
  "BIDUSDT",
  "SIRENUSDT",
  "BANANAS31USDT",
  "BRUSDT",
  "NILUSDT",
  "PARTIUSDT",
  "NAVXUSDT",
  "WALUSDT",
  "FUNUSDT",
  "MLNUSDT",
  "GUNUSDT",
  "PUMPBTCUSDT",
  "STOUSDT",
  "XAUTUSDT",
  "AMPUSDT",
  "BABYUSDT",
  "FHEUSDT",
  "PROMPTUSDT",
  "KERNELUSDT",
  "WCTUSDT",
  "10000000AIDOGEUSDT",
  "BANKUSDT",
  "EPTUSDT",
  "HYPERUSDT",
  "ZORAUSDT",
  "INITUSDT",
  "DOLOUSDT",
  "FISUSDT",
  "JSTUSDT",
  "TAIUSDT",
  "SIGNUSDT",
  "MILKUSDT",
  "HAEDALUSDT",
  "PUNDIXUSDT",
  "B2USDT",
  "GORKUSDT",
  "HOUSEUSDT",
  "ASRUSDT",
  "ALPINEUSDT",
  "MYXUSDT",
  "SYRUPUSDT",
  "OBOLUSDT",
  "SXTUSDT",
  "DOODUSDT",
  "SKYAIUSDT",
  "LAUNCHCOINUSDT",
  "NXPCUSDT",
  "BADGERUSDT",
  "AWEUSDT",
  "BLUEUSDT",
  "BUSDT",
  "TESTADLUSDT",
  "SOONUSDT",
  "ZBCNUSDT",
  "HUMAUSDT",
  "SOPHUSDT",
  "AUSDT",
  "PORT3USDT",
  "LAUSDT",
  "CUDISUSDT",
  "SKATEUSDT",
  "RESOLVUSDT",
  "HOMEUSDT",
  "IDOLUSDT",
  "SQDUSDT",
  "TAGUSDT",
  "SPKUSDT",
  "FUSDT",
  "NEWTUSDT",
  "DMCUSDT",
  "HUSDT",
  "TESTZEUSUSDT",
  "SAHARAUSDT",
  "NODEUSDT",
  "ICNTUSDT",
  "MUSDT",
  "CROSSUSDT",
  "TANSSIUSDT",
  "AINUSDT",
  "USELESSUSDT",
  "PUMPUSDT",
  "CUSDT",
  "VELVETUSDT",
  "TACUSDT",
  "ESUSDT",
  "ERAUSDT",
  "TAUSDT",
  "ASPUSDT",
  "ESPORTSUSDT",
  "IKAUSDT",
  "TREEUSDT",
  "A2ZUSDT",
  "RHEAUSDT",
  "NAORISUSDT",
  "PLAYUSDT",
  "PROVEUSDT",
  "SUPUSDT",
  "TOWNSUSDT",
  "ILVUSDT",
  "INUSDT",
  "KUSDT",
  "YALAUSDT",
  "WAIUSDT",
  "XNYUSDT",
  "AIOUSDT",
  "PUBLICUSDT",
  "DAMUSDT",
  "CRCLUSDT",
  "NVDAUSDT",
  "TSLAUSDT",
  "ALUUSDT",
  "SAPIENUSDT",
  "YZYUSDT",
  "WLFIUSDT",
  "XCXUSDT",
  "SOMIUSDT",
  "AAPLUSDT",
  "GOOGLUSDT",
  "AMZNUSDT",
  "METAUSDT",
  "MCDUSDT",
  "SDUSDT",
  "BASUSDT",
  "XPLUSDT",
  "DFDVUSDT",
  "HOODUSDT",
  "COINUSDT",
  "MSTRUSDT",
  "CAMPUSDT",
  "BTRUSDT",
  "MITOUSDT",
  "HEMIUSDT",
  "LINEAUSDT",
  "QUSDT",
  "PTBUSDT",
  "ARIAUSDT",
  "TAKEUSDT",
  "UUSDT",
  "IBMUSDT",
  "INTCUSDT",
  "BABAUSDT",
  "ASMLUSDT",
  "ARMUSDT",
  "OPENUSDT",
  "PLTRUSDT",
  "SLVUSDT",
  "ORCLUSDT",
  "GEUSDT",
  "SKYUSDT",
  "APPUSDT",
  "AVNTUSDT",
  "SWTCHUSDT",
  "GMEUSDT",
  "RIOTUSDT",
  "MRVLUSDT",
  "HOLOUSDT",
  "BGTESTMEUSDT",
  "XPINUSDT",
  "UBUSDT",
  "ZKCUSDT",
  "PORTALSUSDT",
  "0GUSDT",
  "BARDUSDT",
  "TRADOORUSDT",
  "ASTERUSDT",
  "MSFTUSDT"
]
# Static inputs
resolution = "60"
limit = 5000


def fetch_candle_data(symbol):
    print(f"\n📡 Fetching data for {symbol}...")
    ws = websocket.create_connection(socket)

    session_id = "cs_vOn7C11YGKmD"
    print(f"Session ID: {session_id}")

    # Step 1: Create chart session
    create_msg(ws, 'chart_create_session', [session_id, ""])
    print("✅ Chart session created")

    # Step 2: Resolve symbol
    payload = {
        "symbol": f"BITGET:{symbol}.P",
        "adjustment": "splits",
        "session": "regular",
        "currency-id": "XTVCUSDT",
    }
    param = f'={json.dumps(payload)}'
    create_msg(ws, 'resolve_symbol', [session_id, "sds_sym_1", param])
    print(f"✅ Symbol resolution sent for: {payload['symbol']}")

    # Step 3: Create series
    create_msg(ws, 'create_series', [session_id, "sds_1", "s1", "sds_sym_1", resolution, limit])
    print("✅ Series creation sent")

    # Step 4: Receive and process data
    candle_data = []
    data_received = False
    timeout = 5
    start_time = time.time()

    while True:
        if time.time() - start_time > timeout:
            print("❌ Timeout reached.")
            break

        try:
            res = ws.recv()
        except Exception as e:
            print(f"❌ WebSocket error: {e}")
            break

        if not res or res.startswith('~m~0~m~') or 'ping' in res.lower():
            continue

        if "error" in res.lower():
            print(f"❌ Server error: {res[:200]}")
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
                    print(f"✅ Found {len(series_data)} candles.")
                    for candle in series_data:
                        if 'v' in candle and len(candle['v']) >= 6:
                            # Unpack candle data
                            timestamp, open_price, high, low, close, volume = candle['v']

                            # Convert timestamp
                            if isinstance(timestamp, (int, float)) and timestamp > 1e12:
                                start_at = int(timestamp / 1000)  # Convert to seconds for your format
                            else:
                                start_at = int(timestamp)

                            # Create candle in your specified format
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
                    print(f"✅ Parsed candles. Total collected: {len(candle_data)}")
                except (KeyError, IndexError, ValueError) as e:
                    print(f"⚠️ Error parsing data: {e}")

            elif message.get('m') == 'series_completed':
                print("✅ Series completed.")
                break

        if data_received and len(candle_data) >= limit:
            print(f"✅ Received all {limit} candles.")
            break

    ws.close()

    # Step 5: Add to global dictionaries (don't save individual files)
    if candle_data:
        # Store in the original JSON format dictionary
        all_symbols_data[symbol] = candle_data

        # NEW: Also store in NumPy format for fast processing
        numpy_data = convert_to_numpy_format(candle_data)
        all_symbols_numpy[symbol] = numpy_data

        print(f"✅ Added {symbol} to both formats: {len(candle_data)} candles")
        return True
    else:
        print("❌ No data was collected for this symbol.")
        return False


def convert_to_numpy_format(candle_data):
    """Convert candle data to efficient NumPy array format"""
    if not candle_data:
        return np.array([])

    # Create structured NumPy array for OHLC data
    ohlc_array = np.array([
        (candle['open'], candle['high'], candle['low'], candle['close'])
        for candle in candle_data
    ], dtype=np.float64)

    return ohlc_array


def save_all_data_to_json():
    """Save all collected symbol data to a single JSON file in your specified format"""
    if all_symbols_data:
        filename = "1_price_list.json"
        try:
            with open(filename, 'w') as f:
                json.dump(all_symbols_data, f, indent=4)

            print(f"\n🎯 JSON DATA SAVED SUCCESSFULLY!")
            print(f"📁 File: {filename}")
            print(f"📊 Total symbols: {len(all_symbols_data)}")

            # Show summary
            for symbol, candles in all_symbols_data.items():
                print(f"   {symbol}: {len(candles)} candles")

            return True

        except Exception as e:
            print(f"❌ Failed to save JSON file: {e}")
            return False
    else:
        print("❌ No data was collected, file not saved.")
        return False


def save_all_data_to_numpy():
    """NEW: Save all data to NumPy format for fast processing"""
    if all_symbols_numpy:
        filename = "1_price_list_numpy.npz"
        try:
            # Save as compressed NumPy file
            np.savez_compressed(filename, **all_symbols_numpy)

            print(f"\n🎯 NUMPY DATA SAVED SUCCESSFULLY!")
            print(f"📁 File: {filename}")
            print(f"📊 Total symbols: {len(all_symbols_numpy)}")

            # Show NumPy format summary
            for symbol, numpy_data in all_symbols_numpy.items():
                print(f"   {symbol}: {numpy_data.shape} shape (OHLC data)")

            return True

        except Exception as e:
            print(f"❌ Failed to save NumPy file: {e}")
            return False
    else:
        print("❌ No NumPy data was collected.")
        return False


def load_numpy_data(filename="1_price_list_numpy.npz"):
    """NEW: Load previously saved NumPy data"""
    try:
        data = np.load(filename)
        numpy_dict = {}

        for symbol in data.files:
            numpy_dict[symbol] = data[symbol]

        print(f"✅ Loaded {len(numpy_dict)} symbols from NumPy file")
        return numpy_dict

    except Exception as e:
        print(f"❌ Error loading NumPy data: {e}")
        return {}


# Main execution
if __name__ == "__main__":
    print("🚀 Starting Candle Data Fetching (Dual Format Mode)")
    print("=" * 50)

    successful_fetches = 0
    for symbol in symbols:
        if fetch_candle_data(symbol):
            successful_fetches += 1
        time.sleep(2)  # Rate limiting

    # Save all data to both formats after fetching all symbols
    save_all_data_to_json()  # Original JSON format
    save_all_data_to_numpy()  # NEW: NumPy format

    print(f"\n🎯 Fetching completed: {successful_fetches}/{len(symbols)} symbols successful")

