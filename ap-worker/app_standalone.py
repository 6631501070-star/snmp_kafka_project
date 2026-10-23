#!/usr/bin/env python3
"""
app_standalone.py - Real-time SNMP Dashboard Server
จำลอง Pipeline ครบวงจร (Cisco SNMP Producer -> Kafka Queue -> Worker Consumer -> Socket.io -> Graph)
เปิดให้คลิกดูผ่าน Browser ได้ทันที!
"""

import json
import random
import datetime
import threading
import time
import eventlet
import socketio

# Socket.IO & Web Server
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
app = socketio.WSGIApp(sio, static_files={
    '/': {'content_type': 'text/html', 'filename': 'index.html'}
})

latest_data = {}
traffic_counters = {'R1': {'in': 2500000, 'out': 1200000}, 'R2': {'in': 1800000, 'out': 950000}}

@sio.event
def connect(sid, environ):
    print(f"[Worker/Socket.io] Client connected: {sid}")
    if latest_data:
        sio.emit('snmp_data', latest_data, to=sid)

@sio.event
def disconnect(sid):
    print(f"[Worker/Socket.io] Client disconnected: {sid}")

def mock_producer_and_consumer():
    """
    จำลอง Collector Poll SNMP -> Produce เข้า Queue (Kafka) -> Consumer อ่านมา Emit Socket.io
    """
    routers = ['R1', 'R2']
    while True:
        try:
            for r in routers:
                # จำลอง traffic วิ่งเพิ่มขึ้นเรื่อยๆ
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
                    'cpuLoad': str(random.randint(8, 45)),
                }
                latest_data[r] = payload
                print(f"[Pipeline] Router {r} -> SNMP Poll -> Kafka 'snmp-string' -> Socket.io Broadcast (CPU: {payload['cpuLoad']}%)")
                
            sio.emit('snmp_data', latest_data)
        except Exception as e:
            print(f"Error in pipeline: {e}")
            
        time.sleep(2)

def main():
    t = threading.Thread(target=mock_producer_and_consumer, daemon=True)
    t.start()
    port = 5000
    print("="*60)
    print(f" Web Graph Server Started at: http://localhost:{port}")
    print("="*60)
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)

if __name__ == '__main__':
    main()
