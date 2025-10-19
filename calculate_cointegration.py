from statsmodels.tsa.stattools import coint, adfuller
import statsmodels.api as sm
import pandas as pd
import numpy as np
import math
import json
import logging

logger = logging.getLogger(__name__)

z_score_window = 21


def calculate_adf_test(spread_series):
    """Perform Augmented Dickey-Fuller test on the spread series"""
    adf_result = adfuller(spread_series.dropna())
    return {
        'adf_statistic': adf_result[0],
        'p_value': adf_result[1],
        'critical_values': adf_result[4],
        'is_stationary': adf_result[1] < 0.05
    }


def calculate_zscore(spread):
    """Calculate z-score for spread"""
    df = pd.DataFrame(spread, columns=['spread'])

    # Calculate rolling statistics
    mean = df['spread'].rolling(window=z_score_window, min_periods=1).mean()
    std = df['spread'].rolling(window=z_score_window, min_periods=1).std()

    # Calculate z-score
    zscore = (df['spread'] - mean) / std
    zscore = zscore.fillna(0)

    logger.info(f"Z-score calculation: {len(zscore)} values, range: {zscore.min():.4f} to {zscore.max():.4f}")
    return zscore.values


def calculate_spread(series_1, series_2, hedge_ratio):
    """Calculate spread between two series"""
    spread = pd.Series(series_1) - (pd.Series(series_2) * hedge_ratio)
    return spread


def calculate_cointegration(series_1, series_2):
    """Calculate cointegration between two series"""
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

    # Proper OLS with constant
    series_2_with_const = sm.add_constant(series_2)
    model = sm.OLS(series_1, series_2_with_const).fit()
    hedge_ratio = model.params[0]

    spread = calculate_spread(series_1, series_2, hedge_ratio)
    zero_crossings = len(np.where(np.diff(np.sign(spread)))[0])

    if p_value < 0.05 and coint_t < critical_value:
        coint_flag = 1

    return (coint_flag, round(p_value, 4), round(coint_t, 4), round(critical_value, 4),
            round(hedge_ratio, 4), zero_crossings)


def extract_close_prices(prices):
    """Extract close prices from price data"""
    close_prices = []
    for price_values in prices:
        if math.isnan(price_values["close"]):
            return []
        close_prices.append(price_values["close"])
    return close_prices


def extract_and_align_series(price_data, symbol_1, symbol_2):
    """Extract and align price series for two symbols"""
    series_1 = [p["close"] for p in price_data[symbol_1] if not math.isnan(p["close"])]
    series_2 = [p["close"] for p in price_data[symbol_2] if not math.isnan(p["close"])]

    logger.info(f"Original lengths - {symbol_1}: {len(series_1)}, {symbol_2}: {len(series_2)}")

    # Ensure equal length
    min_len = min(len(series_1), len(series_2))
    series_1 = series_1[:min_len]
    series_2 = series_2[:min_len]

    logger.info(f"Aligned lengths - {symbol_1}: {len(series_1)}, {symbol_2}: {len(series_2)}")
    return series_1, series_2


def get_cointegrated_pairs(prices):
    """Find cointegrated pairs from price data"""
    coint_pair_list = []
    included_list = []

    symbols = list(prices.keys())
    logger.info(f"Analyzing {len(symbols)} symbols for cointegration...")

    for i, sym_1 in enumerate(symbols):
        logger.info(f"Processing {sym_1} ({i + 1}/{len(symbols)})")

        for sym_2 in symbols:
            if sym_2 != sym_1:
                # Get unique combination id
                sorted_characters = sorted(sym_1 + sym_2)
                unique = "".join(sorted_characters)
                if unique in included_list:
                    continue

                # Get close prices
                series_1 = extract_close_prices(prices[sym_1])
                series_2 = extract_close_prices(prices[sym_2])

                # Skip if not enough data
                if len(series_1) < 30 or len(series_2) < 30:
                    continue

                # Check for cointegration
                coint_flag, p_value, t_value, c_value, hedge_ratio, zero_crossings = calculate_cointegration(series_1,
                                                                                                             series_2)
                if coint_flag == 1:
                    included_list.append(unique)
                    coint_pair_list.append({
                        "sym_1": sym_1,
                        "sym_2": sym_2,
                        "p_value": p_value,
                        "t_value": t_value,
                        "c_value": c_value,
                        "hedge_ratio": hedge_ratio,
                        "zero_crossings": zero_crossings
                    })
                    logger.info(f"✅ Found cointegrated pair: {sym_1} - {sym_2}")

    return coint_pair_list


def calculate_cointegrated_pairs():
    """Main function to calculate cointegrated pairs"""
    try:
        # Load price data
        with open("1_price_list.json", "r") as f:
            prices_data = json.load(f)

        logger.info("🔄 Finding cointegrated pairs...")
        logger.info(f"📊 Analyzing {len(prices_data)} symbols...")

        df_coint = get_cointegrated_pairs(prices_data)

        # Save results
        if len(df_coint) > 0:
            df_result = pd.DataFrame(df_coint)
            df_result = df_result.sort_values("zero_crossings", ascending=False)
            df_result.to_csv("2_cointegrated_pairs.csv", index=False)

            logger.info(f"✅ Found {len(df_coint)} cointegrated pairs")
            logger.info(f"💾 Results saved to 2_cointegrated_pairs.csv")

            # Log top pairs
            for _, row in df_result.head(10).iterrows():
                logger.info(f"   {row['sym_1']} - {row['sym_2']}: {row['zero_crossings']} crossings")

            return True
        else:
            logger.warning("❌ No cointegrated pairs found")
            return False

    except FileNotFoundError:
        logger.error("❌ Error: 1_price_list.json file not found!")
        return False
    except Exception as e:
        logger.error(f"❌ Error in cointegration calculation: {e}")
        return False


if __name__ == "__main__":
    calculate_cointegrated_pairs()