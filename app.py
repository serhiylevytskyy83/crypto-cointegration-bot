import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import schedule
import time
import json
import os
from datetime import datetime
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd
from fetch_candles import fetch_all_candles
from calculate_cointegration import calculate_cointegrated_pairs

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Email configuration
EMAIL_CONFIG = {
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587,
    'sender_email': 'fish_s@ukr.net',
    'sender_password': 'your-app-password',  # Use Gmail App Password
    'receiver_email': 'lewika.trade@gmail.com'
}

def send_email_with_files(subject, body, files_to_attach=None):
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = EMAIL_CONFIG['receiver_email']
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        if files_to_attach:
            for file_path in files_to_attach:
                if os.path.exists(file_path):
                    with open(file_path, "rb") as attachment:
                        part = MIMEBase("application", "octet-stream")
                        part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(file_path)}")
                    msg.attach(part)
                    logger.info(f"✅ Attached file: {file_path}")
                else:
                    logger.warning(f"⚠️ File not found: {file_path}")

        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
        server.sendmail(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['receiver_email'], msg.as_string())
        server.quit()

        logger.info("✅ Email sent successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to send email: {e}")
        return False

def send_results_email():
    files_to_send = []
    if os.path.exists('1_price_list.json'):
        files_to_send.append('1_price_list.json')
    if os.path.exists('2_cointegrated_pairs.csv'):
        files_to_send.append('2_cointegrated_pairs.csv')

    if not files_to_send:
        logger.warning("⚠️ No result files found to send")
        return False

    subject = f"Crypto Bot Results - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    body = f"""
    Crypto Cointegration Bot Results

    Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    Files attached:
    {chr(10).join(f'- {file}' for file in files_to_send)}

    Total files: {len(files_to_send)}

    ---
    Automated Crypto Trading Bot
    """
    return send_email_with_files(subject, body, files_to_send)

def fetch_candles_job():
    logger.info("🚀 Starting candle data fetch job...")
    try:
        success = fetch_all_candles()
        if success:
            logger.info("✅ Candle data fetch completed successfully")
            send_results_email()
        else:
            logger.error("❌ Candle data fetch failed")
        return success
    except Exception as e:
        logger.error(f"❌ Error in fetch_candles_job: {e}")
        return False

def calculate_cointegration_job():
    logger.info("🔄 Starting cointegration calculation job...")
    try:
        success = calculate_cointegrated_pairs()
        if success:
            logger.info("✅ Cointegration calculation completed successfully")
            send_results_email()
        else:
            logger.error("❌ Cointegration calculation failed")
        return success
    except Exception as e:
        logger.error(f"❌ Error in calculate_cointegration_job: {e}")
        return False

def full_pipeline_job():
    logger.info("🎯 Starting full pipeline job...")
    try:
        if fetch_all_candles():
            calculate_cointegrated_pairs()
            logger.info("✅ Full pipeline completed successfully")
            send_results_email()
            return True
        else:
            logger.error("❌ Full pipeline failed at candle fetching stage")
            return False
    except Exception as e:
        logger.error(f"❌ Error in full_pipeline_job: {e}")
        return False

def run_scheduler():
    logger.info("⏰ Starting scheduler...")
    schedule.every(6).hours.do(full_pipeline_job)
    logger.info("📅 Scheduler started. Jobs will run every 6 hours.")
    while True:
        schedule.run_pending()
        time.sleep(60)

# Web server with Flask
try:
    from flask import Flask, jsonify, send_from_directory

    app = Flask(__name__)

    @app.route('/')
    def home():
        return "Crypto Bot is running! Use /send-results to email files or /download/<filename> to download."

    @app.route('/send-results')
    def send_results_endpoint():
        success = send_results_email()
        return jsonify({
            "status": "success" if success else "error",
            "message": "Results sent to email" if success else "Failed to send email"
        })

    @app.route('/download/<path:filename>')
    def download_file(filename):
        directory = os.path.abspath('.')  # or use './results' if files are stored there
        file_path = os.path.join(directory, filename)
        if os.path.exists(file_path):
            return send_from_directory(directory, filename, as_attachment=True)
        else:
            return jsonify({
                "status": "error",
                "message": f"File not found: {filename}"
            }), 404

    def start_web_server():
        port = int(os.environ.get('PORT', 5000))
        app.run(host='0.0.0.0', port=port)

except ImportError:
    def start_web_server():
        logger.info("🌐 Web server not available (Flask not installed)")

if __name__ == "__main__":
    if os.environ.get('PROCESS_TYPE') == 'worker':
        logger.info("👷 Starting as worker process with scheduler...")
        full_pipeline_job()
        run_scheduler()
    else:
        logger.info("🌐 Starting as web process...")
        start_web_server()
