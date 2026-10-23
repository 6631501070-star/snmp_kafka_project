import os
import json
import random
import time
import datetime
from threading import Thread
from flask import Flask, send_from_directory
from flask_socketio import SocketIO

app = Flask(__name__, static_folder='.')
app.config['SECRET_KEY'] = 'secret-snmp-kafka'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

latest_data = {}
traffic_counters = {'R1': {'in': 2500000, 'out': 1200000}, 'R2': {'in': 1800000, 'out': 950000}}

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@socketio.on('connect')
def handle_connect():
    print("[Socket.io] Client connected")
    if latest_data:
        socketio.emit('snmp_data', latest_data)

def background_pipeline():
    """
    Simulation loop representing:
    Cisco Router (SNMP) -> collector.py (Producer) -> Kafka -> worker.py (Consumer) -> Socket.io
    """
    routers = ['R1', 'R2']
    while True:
        try:
            for r in routers:
                traffic_counters[r]['in'] += random.randint(15000, 85000)
                traffic_counters[r]['out'] += random.randint(10000, 50000)
                
                payload = {
                    'timestamp': datetime.datetime.now().strftime('%H:%M:%S'),
                    'router': r,
                    'host': '192.168.222.130' if r == 'R1' else '192.168.222.131',
                    'sysDescr': f'Cisco 7200 Router ({r}) IOS Software',
                    'sysUpTime': f"{random.randint(120, 150)} hrs",
                    'sysName': r,
                    'ifInOctets': str(traffic_counters[r]['in']),
                    'ifOutOctets': str(traffic_counters[r]['out']),
                    'cpuLoad': str(random.randint(10, 55)),
                }
                latest_data[r] = payload
            socketio.emit('snmp_data', latest_data)
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(2)

if __name__ == '__main__':
    t = Thread(target=background_pipeline, daemon=True)
    t.start()
    print("=" * 60)
    print(" Web Dashboard running at: http://localhost:5000")
    print("=" * 60)
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
