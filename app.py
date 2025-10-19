import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

import schedule
import time
import json
import os
from datetime import datetime
import logging
from fetch_candles import fetch_all_candles
from calculate_cointegration import calculate_cointegrated_pairs

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_candles_job():
    """Job to fetch candle data every 6 hours"""
    logger.info("🚀 Starting candle data fetch job...")
    try:
        success = fetch_all_candles()
        if success:
            logger.info("✅ Candle data fetch completed successfully")
        else:
            logger.error("❌ Candle data fetch failed")
    except Exception as e:
        logger.error(f"❌ Error in fetch_candles_job: {e}")


def calculate_cointegration_job():
    """Job to calculate cointegrated pairs every 6 hours"""
    logger.info("🔄 Starting cointegration calculation job...")
    try:
        success = calculate_cointegrated_pairs()
        if success:
            logger.info("✅ Cointegration calculation completed successfully")
        else:
            logger.error("❌ Cointegration calculation failed")
    except Exception as e:
        logger.error(f"❌ Error in calculate_cointegration_job: {e}")


def full_pipeline_job():
    """Complete pipeline: fetch candles then calculate cointegration"""
    logger.info("🎯 Starting full pipeline job...")
    try:
        # Fetch candles first
        if fetch_all_candles():
            # Then calculate cointegration
            calculate_cointegrated_pairs()
            logger.info("✅ Full pipeline completed successfully")
        else:
            logger.error("❌ Full pipeline failed at candle fetching stage")
    except Exception as e:
        logger.error(f"❌ Error in full_pipeline_job: {e}")


def run_scheduler():
    """Run the scheduler for automated execution"""
    logger.info("⏰ Starting scheduler...")

    # Schedule jobs to run every 6 hours
    schedule.every(6).hours.do(full_pipeline_job)

    # Also run individual jobs if needed
    schedule.every(6).hours.do(fetch_candles_job)
    schedule.every(6).hours.do(calculate_cointegration_job)

    # Run immediately on startup
    logger.info("🏃 Running initial job on startup...")
    full_pipeline_job()

    logger.info("📅 Scheduler started. Jobs will run every 6 hours.")

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    # For Heroku, we need a web process, so we'll use the scheduler
    # But also provide a way to run once
    if os.environ.get('RUN_ONCE', 'false').lower() == 'true':
        logger.info("🔄 Running single execution...")
        full_pipeline_job()
    else:
        run_scheduler()