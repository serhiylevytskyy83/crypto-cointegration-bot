import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import schedule
import time
import json
import os
from datetime import datetime
import logging
import http.server
import socketserver
import threading

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import your modules
from fetch_candles import fetch_all_candles
from calculate_cointegration import calculate_cointegrated_pairs

class FileHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # Route handling
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(self.create_dashboard().encode())
        
        elif self.path == '/download/price-data':
            self.serve_file('1_price_list.json', 'price_data.json')
        
        elif self.path == '/download/cointegrated-pairs':
            self.serve_file('2_cointegrated_pairs.csv', 'cointegrated_pairs.csv')
        
        elif self.path == '/files':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(self.list_files()).encode())
        
        elif self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "healthy",
                "timestamp": datetime.now().isoformat()
            }).encode())
        
        else:
            super().do_GET()
    
    def serve_file(self, filename, download_name):
        if os.path.exists(filename):
            self.send_response(200)
            self.send_header('Content-type', 'application/octet-stream')
            self.send_header('Content-Disposition', f'attachment; filename="{download_name}"')
            self.end_headers()
            
            with open(filename, 'rb') as f:
                self.wfile.write(f.read())
        else:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "File not found"}).encode())
    
    def list_files(self):
        files = []
        for filename in os.listdir('.'):
            if os.path.isfile(filename):
                files.append({
                    'name': filename,
                    'size': os.path.getsize(filename),
                    'modified': datetime.fromtimestamp(os.path.getmtime(filename)).isoformat()
                })
        return files
    
    def create_dashboard(self):
        price_data_exists = os.path.exists('1_price_list.json')
        pairs_exists = os.path.exists('2_cointegrated_pairs.csv')
        
        html = f"""
        <html>
        <head>
            <title>Crypto Bot Dashboard</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .card {{ border: 1px solid #ddd; padding: 20px; margin: 10px; }}
                .success {{ background-color: #d4edda; }}
                .error {{ background-color: #f8d7da; }}
                .btn {{ display: inline-block; padding: 10px; margin: 5px; background: #007bff; color: white; text-decoration: none; }}
            </style>
        </head>
        <body>
            <h1>Crypto Cointegration Bot</h1>
            <p>Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <div class="card {'success' if price_data_exists else 'error'}">
                <h2>Price Data</h2>
                {'<p>File available</p><a href="/download/price-data" class="btn">Download</a>' if price_data_exists else '<p>File not found</p>'}
            </div>
            
            <div class="card {'success' if pairs_exists else 'error'}">
                <h2>Cointegrated Pairs</h2>
                {'<p>File available</p><a href="/download/cointegrated-pairs" class="btn">Download</a>' if pairs_exists else '<p>File not found</p>'}
            </div>
            
            <div class="card">
                <h2>Tools</h2>
                <a href="/files" class="btn">List Files</a>
                <a href="/health" class="btn">Health Check</a>
            </div>
        </body>
        </html>
        """
        return html

def fetch_candles_job():
    """Job to fetch candle data every 6 hours"""
    logger.info("🚀 Starting candle data fetch job...")
    try:
        success = fetch_all_candles()
        if success:
            logger.info("✅ Candle data fetch completed successfully")
        else:
            logger.error("❌ Candle data fetch failed")
        return success
    except Exception as e:
        logger.error(f"❌ Error in fetch_candles_job: {e}")
        return False

def calculate_cointegration_job():
    """Job to calculate cointegrated pairs every 6 hours"""
    logger.info("🔄 Starting cointegration calculation job...")
    try:
        success = calculate_cointegrated_pairs()
        if success:
            logger.info("✅ Cointegration calculation completed successfully")
        else:
            logger.error("❌ Cointegration calculation failed")
        return success
    except Exception as e:
        logger.error(f"❌ Error in calculate_cointegration_job: {e}")
        return False

def full_pipeline_job():
    """Complete pipeline: fetch candles then calculate cointegration"""
    logger.info("🎯 Starting full pipeline job...")
    try:
        # Fetch candles first
        if fetch_all_candles():
            # Then calculate cointegration
            calculate_cointegrated_pairs()
            logger.info("✅ Full pipeline completed successfully")
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
        time.sleep(60)

def start_web_server():
    """Start the web server"""
    port = int(os.environ.get('PORT', 5000))
    with socketserver.TCPServer(("", port), FileHandler) as httpd:
        logger.info(f"🌐 Web server running on port {port}")
        httpd.serve_forever()

if __name__ == "__main__":
    # Determine if we're running as web or worker
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
