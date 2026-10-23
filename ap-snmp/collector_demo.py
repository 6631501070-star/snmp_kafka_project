#!/usr/bin/env python3
"""
collector_demo.py - Mock SNMP Collector (ไม่ต้องมี Router จริง)
สร้างข้อมูลจำลองแล้วส่งไป Kafka topic: snmp-string
"""

import json, time, random, datetime
from kafka import KafkaProducer

KAFKA_BROKER  = 'localhost:9092'
KAFKA_TOPIC   = 'snmp-string'
POLL_INTERVAL = 3

ROUTERS = ['R1', 'R2']

def make_mock_data(router: str) -> dict:
    return {
        'timestamp':   datetime.datetime.utcnow().isoformat(),
        'router':      router,
        'host':        f'192.168.222.{130 if router=="R1" else 131}',
        'sysDescr':    f'Cisco IOS {router} (mock)',
        'sysUpTime':   str(random.randint(100000, 999999)),
        'sysName':     router,
        'ifInOctets':  str(random.randint(1_000_000, 99_000_000)),
        'ifOutOctets': str(random.randint(500_000,  50_000_000)),
        'cpuLoad':     str(random.randint(1, 85)),
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"[demo-collector] Connected → Kafka '{KAFKA_TOPIC}' (mock mode)\n")
    try:
        while True:
            for r in ROUTERS:
                data = make_mock_data(r)
                producer.send(KAFKA_TOPIC, value=data)
                print(f"  → [{r}] CPU={data['cpuLoad']}% In={data['ifInOctets']} Out={data['ifOutOctets']}")
            producer.flush()
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("\n[demo-collector] Stopped.")
    finally:
        producer.close()

if __name__ == '__main__':
    main()
