# from config_strategy_api import z_score_window
from statsmodels.tsa.stattools import coint
import statsmodels.api as sm
import pandas as pd
import numpy as np
import math
import json
from tqdm import tqdm

z_score_window = 21


def calculate_zscore(spread):
    df = pd.DataFrame(spread, columns=['spread'])
    mean = df['spread'].rolling(window=z_score_window, min_periods=1).mean()
    std = df['spread'].rolling(window=z_score_window, min_periods=1).std()
    zscore = (df['spread'] - mean) / std
    zscore = zscore.fillna(0)
    return zscore.values


def calculate_spread(series_1, series_2, hedge_ratio):
    spread = pd.Series(series_1) - (pd.Series(series_2) * hedge_ratio)
    return spread


def calculate_cointegration(series_1, series_2):
    # Data validation
    series_1 = pd.Series(series_1).dropna()
    series_2 = pd.Series(series_2).dropna()

    # Ensure equal length after cleaning
    min_len = min(len(series_1), len(series_2))
    if min_len < 30:
        return (0, 1.0, 0, 0, 0, 0)

    series_1 = series_1.iloc[:min_len]
    series_2 = series_2.iloc[:min_len]

    coint_flag = 0
    coint_res = coint(series_1, series_2)
    coint_t = coint_res[0]
    p_value = coint_res[1]
    critical_value = coint_res[2][1]

    # FIXED: Proper OLS with constant
    series_2_with_const = sm.add_constant(series_2)
    model = sm.OLS(series_1, series_2_with_const).fit()
    hedge_ratio = model.params[1]  # CORRECTION: Use the SECOND parameter for slope

    spread = calculate_spread(series_1, series_2, hedge_ratio)
    zero_crossings = len(np.where(np.diff(np.sign(spread)))[0])

    if p_value < 0.05 and coint_t < critical_value:
        coint_flag = 1

    return (coint_flag, round(p_value, 4), round(coint_t, 4),
            round(critical_value, 4), round(hedge_ratio, 4), zero_crossings)


def extract_close_prices(prices):
    close_prices = []
    for price_values in prices:
        if math.isnan(price_values["close"]):
            return []
        close_prices.append(price_values["close"])
    return close_prices


def get_cointegrated_pairs_corrected(prices):
    """Corrected version with proper pair comparison logic"""
    coint_pair_list = []
    included_set = set()  # Use set for O(1) lookups

    # Convert to list for proper indexing
    symbols = list(prices.keys())
    print(f"Analyzing {len(symbols)} symbols...")

    total_pairs = len(symbols) * (len(symbols) - 1) // 2
    progress_bar = tqdm(total=total_pairs, desc="Checking pairs")

    for i, sym_1 in enumerate(symbols):
        # Get close prices once per symbol
        series_1 = extract_close_prices(prices[sym_1])
        if len(series_1) < 30:
            progress_bar.update(len(symbols) - i - 1)  # Update progress for skipped pairs
            continue

        for sym_2 in symbols[i + 1:]:  # Only compare with subsequent symbols
            progress_bar.update(1)

            # Create unique pair identifier
            unique = "".join(sorted([sym_1, sym_2]))

            # Skip if already processed
            if unique in included_set:
                continue

            # Get close prices for second symbol
            series_2 = extract_close_prices(prices[sym_2])
            if len(series_2) < 30:
                continue

            # Check cointegration
            coint_flag, p_value, t_value, c_value, hedge_ratio, zero_crossings = calculate_cointegration(
                series_1, series_2
            )

            if coint_flag == 1:
                included_set.add(unique)
                coint_pair_list.append({
                    "sym_1": sym_1,
                    "sym_2": sym_2,
                    "p_value": p_value,
                    "t_value": t_value,
                    "c_value": c_value,
                    "hedge_ratio": hedge_ratio,
                    "zero_crossings": zero_crossings
                })

    progress_bar.close()

    # Output results
    if coint_pair_list:
        df_coint = pd.DataFrame(coint_pair_list)
        df_coint = df_coint.sort_values("zero_crossings", ascending=False)
        df_coint.to_csv("2_cointegrated_pairs.csv", index=False)
        print(f"✅ Found {len(coint_pair_list)} cointegrated pairs")
    else:
        df_coint = pd.DataFrame()
        print("❌ No cointegrated pairs found")

    return df_coint


# NEW: Load NumPy data function
def load_numpy_data(filename="1_price_list_numpy.npz"):
    """Load price data from NumPy file"""
    try:
        data = np.load(filename)
        numpy_dict = {}

        for symbol in data.files:
            numpy_dict[symbol] = data[symbol]

        print(f"✅ Loaded {len(numpy_dict)} symbols from {filename}")
        return numpy_dict

    except FileNotFoundError:
        print(f"❌ Error: {filename} file not found!")
        return None
    except Exception as e:
        print(f"❌ Error loading NumPy data: {e}")
        return None


def get_cointegrated_pairs_numpy(numpy_data):
    """Cointegration analysis for NumPy data"""
    symbols = list(numpy_data.keys())
    coint_pair_list = []
    included_set = set()

    total_pairs = len(symbols) * (len(symbols) - 1) // 2
    progress_bar = tqdm(total=total_pairs, desc="Checking pairs (NumPy)")

    for i, sym_1 in enumerate(symbols):
        # Extract close prices from NumPy array (column 3)
        series_1 = numpy_data[sym_1][:, 3]
        if len(series_1) < 30:
            progress_bar.update(len(symbols) - i - 1)
            continue

        for sym_2 in symbols[i + 1:]:
            progress_bar.update(1)

            unique = "".join(sorted([sym_1, sym_2]))
            if unique in included_set:
                continue

            series_2 = numpy_data[sym_2][:, 3]
            if len(series_2) < 30:
                continue

            # Ensure same length
            min_length = min(len(series_1), len(series_2))
            series_1_trimmed = series_1[:min_length]
            series_2_trimmed = series_2[:min_length]

            coint_flag, p_value, t_value, c_value, hedge_ratio, zero_crossings = calculate_cointegration(
                series_1_trimmed, series_2_trimmed
            )

            if coint_flag == 1:
                included_set.add(unique)
                coint_pair_list.append({
                    "sym_1": sym_1, "sym_2": sym_2,
                    "p_value": p_value, "t_value": t_value,
                    "c_value": c_value, "hedge_ratio": hedge_ratio,
                    "zero_crossings": zero_crossings
                })

    progress_bar.close()

    if coint_pair_list:
        df_coint = pd.DataFrame(coint_pair_list)
        df_coint = df_coint.sort_values("zero_crossings", ascending=False)
        df_coint.to_csv("2_cointegrated_pairs_numpy.csv", index=False)
        print(f"✅ Found {len(coint_pair_list)} cointegrated pairs")
    else:
        df_coint = pd.DataFrame()
        print("❌ No cointegrated pairs found")

    return df_coint


# MAIN EXECUTION BLOCK
if __name__ == "__main__":
    print("🚀 Starting Cointegration Analysis")
    print("=" * 50)

    # Try NumPy first, then JSON
    numpy_prices = load_numpy_data("1_price_list_numpy.npz")

    if numpy_prices is not None:
        print("\nUsing NumPy data format...")
        df_con = get_cointegrated_pairs_numpy(numpy_prices)
    else:
        print("\nUsing JSON data format...")
        try:
            with open("1_price_list.json", "r") as f:
                prices_data = json.load(f)
            df_con = get_cointegrated_pairs_corrected(prices_data)
        except FileNotFoundError:
            print("❌ Error: No price data files found!")
        except Exception as e:
            print(f"❌ Error: {e}")

    if 'df_con' in locals() and not df_con.empty:
        print(f"\n🎯 ANALYSIS COMPLETE!")
        print(f"📈 Found {len(df_con)} cointegrated pairs")
        print(f"\n📊 Top pairs:")
        print(df_con.head()[['sym_1', 'sym_2', 'p_value', 'zero_crossings']])