#!/usr/bin/env python3
"""
worker.py - Kafka Consumer + Socket.io Server
อ่านข้อมูลจาก Kafka topic: snmp-string แล้ว emit ไปยัง Socket.io clients (graph)
"""

import json
import threading
from kafka import KafkaConsumer
import eventlet
import socketio

# ===== CONFIG =====
KAFKA_BROKER  = 'localhost:9092'
KAFKA_TOPIC   = 'snmp-string'
KAFKA_GROUP   = 'ap-worker-group'
SOCKETIO_PORT = 5000
# ==================

# สร้าง Socket.io server
sio = socketio.Server(cors_allowed_origins='*', async_mode='eventlet')
app = socketio.WSGIApp(sio, static_files={
    '/': {'content_type': 'text/html', 'filename': 'index.html'}
})

latest_data = {}   # เก็บข้อมูลล่าสุดของแต่ละ router


# ─── Socket.io events ────────────────────────────────────────────────────────
@sio.event
def connect(sid, environ):
    print(f"[worker] Client connected: {sid}")
    # ส่งข้อมูลล่าสุดให้ client ใหม่ทันที
    if latest_data:
        sio.emit('snmp_data', latest_data, to=sid)


@sio.event
def disconnect(sid):
    print(f"[worker] Client disconnected: {sid}")
# ─────────────────────────────────────────────────────────────────────────────


def kafka_consumer_thread():
    """Thread อ่าน Kafka และ emit ผ่าน Socket.io"""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP,
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print(f"[worker] Kafka consumer listening on '{KAFKA_TOPIC}' ...")

    for message in consumer:
        data = message.value
        router = data.get('router', 'unknown')
        latest_data[router] = data
        print(f"[worker] Received from Kafka → router={router}  ts={data.get('timestamp')}")
        # ส่งให้ทุก client ที่เชื่อมอยู่
        sio.emit('snmp_data', latest_data)


def main():
    # เริ่ม Kafka consumer ใน background thread
    t = threading.Thread(target=kafka_consumer_thread, daemon=True)
    t.start()

    print(f"[worker] Socket.io server running at http://0.0.0.0:{SOCKETIO_PORT}")
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', SOCKETIO_PORT)), app)


if __name__ == '__main__':
    main()
