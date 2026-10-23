#!/usr/bin/env python3
"""
collector.py - SNMP Collector / Kafka Producer
ดึงข้อมูล SNMP จาก Cisco Router แล้วส่งไปยัง Kafka topic: snmp-string
"""

import json
import time
import datetime
from pysnmp.hlapi import *
from kafka import KafkaProducer

# ===== CONFIG =====
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC   = 'snmp-string'

# รายการ Router ที่จะ poll SNMP (IP ใน GNS3)
ROUTERS = [
    {'host': '192.168.222.130', 'community': 'public', 'name': 'R1'},
    {'host': '192.168.222.131', 'community': 'public', 'name': 'R2'},
]

POLL_INTERVAL = 5  # วินาที

# OID ที่สนใจ
OID_MAP = {
    'sysDescr':    '1.3.6.1.2.1.1.1.0',
    'sysUpTime':   '1.3.6.1.2.1.1.3.0',
    'sysName':     '1.3.6.1.2.1.1.5.0',
    'ifInOctets':  '1.3.6.1.2.1.2.2.1.10.1',   # interface 1 in
    'ifOutOctets': '1.3.6.1.2.1.2.2.1.16.1',   # interface 1 out
    'cpuLoad':     '1.3.6.1.4.1.9.2.1.58.0',   # Cisco CPU (5 sec avg)
}
# ==================


def snmp_get(host: str, community: str, oid: str):
    """ดึงค่า SNMP GET จาก host"""
    error_indication, error_status, error_index, var_binds = next(
        getCmd(
            SnmpEngine(),
            CommunityData(community, mpModel=0),
            UdpTransportTarget((host, 161), timeout=2, retries=1),
            ContextData(),
            ObjectType(ObjectIdentity(oid))
        )
    )
    if error_indication or error_status:
        return None
    return str(var_binds[0][1])


def collect_router(router: dict) -> dict:
    """รวบรวมข้อมูลทุก OID จาก router หนึ่งตัว"""
    result = {
        'timestamp': datetime.datetime.utcnow().isoformat(),
        'router': router['name'],
        'host': router['host'],
    }
    for name, oid in OID_MAP.items():
        val = snmp_get(router['host'], router['community'], oid)
        result[name] = val
        print(f"  [{router['name']}] {name} = {val}")
    return result


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"[collector] Connected to Kafka at {KAFKA_BROKER}")
    print(f"[collector] Publishing to topic '{KAFKA_TOPIC}' every {POLL_INTERVAL}s\n")

    try:
        while True:
            for router in ROUTERS:
                print(f"[collector] Polling {router['name']} ({router['host']}) ...")
                data = collect_router(router)
                producer.send(KAFKA_TOPIC, value=data)
                print(f"  → Sent to Kafka: {data}\n")
            producer.flush()
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("\n[collector] Stopped.")
    finally:
        producer.close()


if __name__ == '__main__':
    main()
