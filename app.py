import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

import schedule
import time
import json
import os
from datetime import datetime
import logging
from flask import Flask, send_file, jsonify, render_template_string
import pandas as pd
from fetch_candles import fetch_all_candles
from calculate_cointegration import calculate_cointegrated_pairs

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# HTML template for dashboard
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Crypto Cointegration Bot - Results Dashboard</title>
    <style>
        body { 
            font-family: Arial, sans-serif; 
            margin: 40px; 
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        h1 { 
            color: #2c3e50; 
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }
        .card { 
            border: 1px solid #ddd; 
            padding: 20px; 
            margin: 20px 0; 
            border-radius: 8px;
            background: #fafafa;
        }
        .success { 
            border-left: 5px solid #27ae60; 
        }
        .error { 
            border-left: 5px solid #e74c3c; 
        }
        .warning { 
            border-left: 5px solid #f39c12; 
        }
        .btn {
            display: inline-block;
            padding: 10px 20px;
            margin: 5px;
            background: #3498db;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            border: none;
            cursor: pointer;
        }
        .btn-download {
            background: #27ae60;
        }
        .btn-view {
            background: #3498db;
        }
        .file-list {
            background: white;
            padding: 15px;
            border-radius: 5px;
            margin: 10px 0;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #f8f9fa;
        }
        tr:hover {
            background-color: #f5f5f5;
        }
        .last-updated {
            color: #7f8c8d;
            font-style: italic;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Crypto Cointegration Bot - Results Dashboard</h1>
        <p class="last-updated">Last updated: {{ last_updated }}</p>

        <div class="card success">
            <h2>💰 Price Data</h2>
            {% if price_data_exists %}
            <p><strong>Symbols Collected:</strong> {{ price_data_stats.symbols_count }}</p>
            <p><strong>Sample Symbols:</strong> {{ price_data_stats.sample_symbols }}</p>
            <div>
                <a href="/download/price-data" class="btn btn-download">📥 Download JSON</a>
                <a href="/view/price-data" class="btn btn-view">👁️ View in Browser</a>
                <a href="/view/price-data-summary" class="btn btn-view">📊 View Summary</a>
            </div>
            {% else %}
            <p style="color: #e74c3c;">❌ Price data file not found</p>
            {% endif %}
        </div>

        <div class="card {% if cointegrated_pairs_exists %}success{% else %}warning{% endif %}">
            <h2>📈 Cointegrated Pairs</h2>
            {% if cointegrated_pairs_exists %}
            <p><strong>Pairs Found:</strong> {{ cointegrated_pairs_stats.pairs_count }}</p>
            <p><strong>Last Calculation:</strong> {{ cointegrated_pairs_stats.last_calculated }}</p>
            <div>
                <a href="/download/cointegrated-pairs" class="btn btn-download">📥 Download CSV</a>
                <a href="/view/cointegrated-pairs" class="btn btn-view">👁️ View in Browser</a>
                <a href="/view/cointegrated-pairs-table" class="btn btn-view">📋 View as Table</a>
            </div>

            {% if cointegrated_pairs_stats.top_pairs %}
            <h3>Top Pairs (by Zero Crossings):</h3>
            <table>
                <thead>
                    <tr>
                        <th>Symbol 1</th>
                        <th>Symbol 2</th>
                        <th>Zero Crossings</th>
                        <th>P-Value</th>
                        <th>Hedge Ratio</th>
                    </tr>
                </thead>
                <tbody>
                    {% for pair in cointegrated_pairs_stats.top_pairs %}
                    <tr>
                        <td>{{ pair.sym_1 }}</td>
                        <td>{{ pair.sym_2 }}</td>
                        <td>{{ pair.zero_crossings }}</td>
                        <td>{{ pair.p_value }}</td>
                        <td>{{ pair.hedge_ratio }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% endif %}

            {% else %}
            <p style="color: #f39c12;">⚠️ Cointegrated pairs file not found or being calculated</p>
            {% endif %}
        </div>

        <div class="card">
            <h2>🔧 Tools & Utilities</h2>
            <div>
                <a href="/files" class="btn">📁 List All Files</a>
                <a href="/health" class="btn">❤️ Health Check</a>
                <a href="/run-pipeline" class="btn" onclick="return confirm('This will run the full pipeline. Continue?')">🔄 Run Pipeline Now</a>
            </div>
        </div>

        <div class="card">
            <h3>📋 Quick File Access</h3>
            <div class="file-list">
                <p><strong>Available Files:</strong></p>
                <ul>
                    {% for file in available_files %}
                    <li>{{ file.name }} ({{ file.size }} bytes) - <a href="/download-file/{{ file.name }}">Download</a></li>
                    {% endfor %}
                </ul>
            </div>
        </div>
    </div>
</body>
</html>
"""


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
        time.sleep(60)  # Check every minute


# ==================== WEB ENDPOINTS ====================

@app.route('/')
def home():
    """Main dashboard page"""
    return render_template_string(DASHBOARD_HTML, **get_dashboard_data())


@app.route('/dashboard')
def dashboard():
    """Dashboard page"""
    return render_template_string(DASHBOARD_HTML, **get_dashboard_data())


def get_dashboard_data():
    """Get data for dashboard"""
    # Get file information
    available_files = []
    for filename in ['1_price_list.json', '2_cointegrated_pairs.csv', 'requirements.txt', 'Procfile', 'runtime.txt']:
        if os.path.exists(filename):
            available_files.append({
                'name': filename,
                'size': os.path.getsize(filename),
                'modified': datetime.fromtimestamp(os.path.getmtime(filename)).strftime('%Y-%m-%d %H:%M:%S')
            })

    # Price data stats
    price_data_exists = os.path.exists('1_price_list.json')
    price_data_stats = {}
    if price_data_exists:
        try:
            with open('1_price_list.json', 'r') as f:
                price_data = json.load(f)
            price_data_stats = {
                'symbols_count': len(price_data),
                'sample_symbols': ', '.join(list(price_data.keys())[:5])
            }
        except:
            price_data_stats = {'symbols_count': 0, 'sample_symbols': 'Error reading file'}

    # Cointegrated pairs stats
    cointegrated_pairs_exists = os.path.exists('2_cointegrated_pairs.csv')
    cointegrated_pairs_stats = {}
    if cointegrated_pairs_exists:
        try:
            df = pd.read_csv('2_cointegrated_pairs.csv')
            cointegrated_pairs_stats = {
                'pairs_count': len(df),
                'last_calculated': datetime.fromtimestamp(os.path.getmtime('2_cointegrated_pairs.csv')).strftime(
                    '%Y-%m-%d %H:%M:%S'),
                'top_pairs': df.head(10).to_dict('records')
            }
        except:
            cointegrated_pairs_stats = {'pairs_count': 0, 'last_calculated': 'Error reading file', 'top_pairs': []}

    return {
        'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'price_data_exists': price_data_exists,
        'price_data_stats': price_data_stats,
        'cointegrated_pairs_exists': cointegrated_pairs_exists,
        'cointegrated_pairs_stats': cointegrated_pairs_stats,
        'available_files': available_files
    }


# ==================== DOWNLOAD ENDPOINTS ====================

@app.route('/download/price-data')
def download_price_data():
    """Download the price list JSON file"""
    try:
        return send_file('1_price_list.json',
                         as_attachment=True,
                         download_name=f'price_data_{datetime.now().strftime("%Y%m%d_%H%M")}.json')
    except FileNotFoundError:
        return jsonify({"error": "Price data file not found"}), 404


@app.route('/download/cointegrated-pairs')
def download_cointegrated_pairs():
    """Download the cointegrated pairs CSV file"""
    try:
        return send_file('2_cointegrated_pairs.csv',
                         as_attachment=True,
                         download_name=f'cointegrated_pairs_{datetime.now().strftime("%Y%m%d_%H%M")}.csv')
    except FileNotFoundError:
        return jsonify({"error": "Cointegrated pairs file not found"}), 404


@app.route('/download-file/<filename>')
def download_file(filename):
    """Download any file by name"""
    try:
        if os.path.exists(filename):
            return send_file(filename, as_attachment=True)
        else:
            return jsonify({"error": f"File {filename} not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ==================== VIEW ENDPOINTS ====================

@app.route('/view/price-data')
def view_price_data():
    """View price data in browser as JSON"""
    try:
        with open('1_price_list.json', 'r') as f:
            data = json.load(f)
        return jsonify(data)
    except FileNotFoundError:
        return jsonify({"error": "Price data file not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/view/price-data-summary')
def view_price_data_summary():
    """View price data summary in browser"""
    try:
        with open('1_price_list.json', 'r') as f:
            data = json.load(f)

        summary = {
            "total_symbols": len(data),
            "symbols": list(data.keys()),
            "sample_data": {}
        }

        # Add sample data for first 3 symbols
        for symbol in list(data.keys())[:3]:
            if data[symbol]:
                summary["sample_data"][symbol] = {
                    "candle_count": len(data[symbol]),
                    "first_candle": data[symbol][0] if data[symbol] else None,
                    "last_candle": data[symbol][-1] if data[symbol] else None
                }

        return jsonify(summary)
    except FileNotFoundError:
        return jsonify({"error": "Price data file not found"}), 404


@app.route('/view/cointegrated-pairs')
def view_cointegrated_pairs():
    """View cointegrated pairs in browser as JSON"""
    try:
        df = pd.read_csv('2_cointegrated_pairs.csv')
        return jsonify(df.to_dict('records'))
    except FileNotFoundError:
        return jsonify({"error": "Cointegrated pairs file not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/view/cointegrated-pairs-table')
def view_cointegrated_pairs_table():
    """View cointegrated pairs as HTML table"""
    try:
        df = pd.read_csv('2_cointegrated_pairs.csv')
        return df.to_html(classes='table table-striped', index=False)
    except FileNotFoundError:
        return "<h2>Cointegrated pairs file not found</h2>", 404


# ==================== UTILITY ENDPOINTS ====================

@app.route('/files')
def list_files():
    """List all files in current directory"""
    files = []
    for filename in os.listdir('.'):
        if os.path.isfile(filename):
            files.append({
                'name': filename,
                'size': os.path.getsize(filename),
                'modified': datetime.fromtimestamp(os.path.getmtime(filename)).strftime('%Y-%m-%d %H:%M:%S'),
                'download_url': f'/download-file/{filename}'
            })

    return jsonify(files)


@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Crypto Cointegration Bot",
        "files": {
            "price_data": os.path.exists('1_price_list.json'),
            "cointegrated_pairs": os.path.exists('2_cointegrated_pairs.csv')
        }
    })


@app.route('/run-pipeline')
def run_pipeline():
    """Manual trigger for the pipeline"""
    try:
        success = full_pipeline_job()
        return jsonify({
            "status": "success" if success else "error",
            "message": "Pipeline executed successfully" if success else "Pipeline execution failed",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== MAIN EXECUTION ====================

if __name__ == "__main__":
    # For Heroku/Railway, we need to determine if we're running as web or worker
    if os.environ.get('PROCESS_TYPE') == 'worker':
        logger.info("👷 Starting as worker process with scheduler...")
        # Run initial job
        full_pipeline_job()
        # Start scheduler
        run_scheduler()
    else:
        logger.info("🌐 Starting as web process...")
        # Run web server
        port = int(os.environ.get('PORT', 5000))
        app.run(host='0.0.0.0', port=port)