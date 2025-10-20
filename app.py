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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Email configuration
EMAIL_CONFIG = {
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587,
    'sender_email': 'your-bot-email@gmail.com',  # Change this to your bot's email
    'sender_password': 'your-app-password',      # Gmail App Password
    'receiver_email': 'lewika.trade@gmail.com'
}

def send_email_with_files(subject, body, files_to_attach=None):
    """Send email with attached files"""
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = EMAIL_CONFIG['receiver_email']
        msg['Subject'] = subject

        # Add body to email
        msg.attach(MIMEText(body, 'plain'))

        # Attach files
        if files_to_attach:
            for file_path in files_to_attach:
                if os.path.exists(file_path):
                    # Open the file in binary mode
                    with open(file_path, "rb") as attachment:
                        # Add file as application/octet-stream
                        part = MIMEBase("application", "octet-stream")
                        part.set_payload(attachment.read())
                    
                    # Encode file in ASCII characters to send by email    
                    encoders.encode_base64(part)
                    
                    # Add header as key/value pair to attachment part
                    part.add_header(
                        "Content-Disposition",
                        f"attachment; filename= {os.path.basename(file_path)}",
                    )
                    
                    # Add attachment to message
                    msg.attach(part)
                    logger.info(f"✅ Attached file: {file_path}")
                else:
                    logger.warning(f"⚠️ File not found: {file_path}")

        # Create SMTP session
        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()  # Enable security
        server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
        
        # Convert message to string and send
        text = msg.as_string()
        server.sendmail(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['receiver_email'], text)
        server.quit()
        
        logger.info("✅ Email sent successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to send email: {e}")
        return False

def send_results_email():
    """Send all results files via email"""
    files_to_send = []
    
    # Check which files exist
    if os.path.exists('1_price_list.json'):
        files_to_send.append('1_price_list.json')
    
    if os.path.exists('2_cointegrated_pairs.csv'):
        files_to_send.append('2_cointegrated_pairs.csv')
    
    if not files_to_send:
        logger.warning("⚠️ No result files found to send")
        return False
    
    # Create email content
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
    
    # Send email
    return send_email_with_files(subject, body, files_to_send)

def fetch_candles_job():
    """Job to fetch candle data every 6 hours and send email"""
    logger.info("🚀 Starting candle data fetch job...")
    try:
        success = fetch_all_candles()
        if success:
            logger.info("✅ Candle data fetch completed successfully")
            # Send email after successful fetch
            send_results_email()
        else:
            logger.error("❌ Candle data fetch failed")
        return success
    except Exception as e:
        logger.error(f"❌ Error in fetch_candles_job: {e}")
        return False

def calculate_cointegration_job():
    """Job to calculate cointegrated pairs every 6 hours and send email"""
    logger.info("🔄 Starting cointegration calculation job...")
    try:
        success = calculate_cointegrated_pairs()
        if success:
            logger.info("✅ Cointegration calculation completed successfully")
            # Send email after successful calculation
            send_results_email()
        else:
            logger.error("❌ Cointegration calculation failed")
        return success
    except Exception as e:
        logger.error(f"❌ Error in calculate_cointegration_job: {e}")
        return False

def full_pipeline_job():
    """Complete pipeline: fetch candles then calculate cointegration then send email"""
    logger.info("🎯 Starting full pipeline job...")
    try:
        # Fetch candles first
        if fetch_all_candles():
            # Then calculate cointegration
            calculate_cointegrated_pairs()
            logger.info("✅ Full pipeline completed successfully")
            # Send final email with all results
            send_results_email()
            return True
        else:
            logger.error("❌ Full pipeline failed at candle fetching stage")
            return False
    except Exception as e:
        logger.error(f"❌ Error in full_pipeline_job: {e}")
        return False

def run_scheduler():
    """Run the scheduler for automated execution"""
    logger.info("⏰ Starting scheduler...")
    
    # Schedule jobs to run every 6 hours
    schedule.every(6).hours.do(full_pipeline_job)
    
    logger.info("📅 Scheduler started. Jobs will run every 6 hours.")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

# Simple web endpoint to manually trigger email
try:
    from flask import Flask, jsonify
    app = Flask(__name__)
    
    @app.route('/')
    def home():
        return "Crypto Bot is running! Use /send-results to email files."
    
    @app.route('/send-results')
    def send_results_endpoint():
        """Manual trigger to send results via email"""
        success = send_results_email()
        return jsonify({
            "status": "success" if success else "error",
            "message": "Results sent to email" if success else "Failed to send email"
        })
    
    def start_web_server():
        port = int(os.environ.get('PORT', 5000))
        app.run(host='0.0.0.0', port=port)
        
except ImportError:
    def start_web_server():
        logger.info("🌐 Web server not available (Flask not installed)")

if __name__ == "__main__":
    # For Heroku/Railway, determine if we're running as web or worker
    if os.environ.get('PROCESS_TYPE') == 'worker':
        logger.info("👷 Starting as worker process with scheduler...")
        # Run initial job
        full_pipeline_job()
        # Start scheduler
        run_scheduler()
    else:
        logger.info("🌐 Starting as web process...")
        # Start web server
        start_web_server()
