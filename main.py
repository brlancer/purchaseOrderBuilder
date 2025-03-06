from flask import Flask, request, jsonify, render_template
from sync_shiphero import sync_shiphero
from prepare_replenishment import prepare_replenishment
from populate_production import populate_production
import threading

app = Flask(__name__)

@app.route('/webhook/prepare_replenishment', methods=['GET', 'POST'])
def webhook_prepare_replenishment():
    use_cache_stock_levels = request.args.get('use_cache_stock_levels', 'false').lower() == 'true'
    use_cache_sales = request.args.get('use_cache_sales', 'false').lower() == 'true'
    threading.Thread(target=prepare_replenishment, args=(use_cache_stock_levels, use_cache_sales)).start()
    return jsonify({"status": "Task prepare_replenishment started"}), 200

@app.route('/webhook/populate_production', methods=['GET', 'POST'])
def webhook_populate_production():
    threading.Thread(target=populate_production).start()
    return jsonify({"status": "Task populate_production started"}), 200

@app.route('/webhook/sync_shiphero', methods=['GET', 'POST'])
def webhook_sync_shiphero():
    threading.Thread(target=sync_shiphero).start()
    return jsonify({"status": "Task sync_shiphero started"}), 200

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)