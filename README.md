# 📡 SNMP Kafka Real-time Monitoring Project (Enterprise UI Redesign)

โปรเจคติดตามและแสดงผลข้อมูล Cisco Router แบบ Real-time ผ่าน SNMP, Apache Kafka, และ Socket.io พร้อม **หน้าเว็บ Clean Enterprise Minimal Dashboard**

---

## 🏗️ Architecture

```
Cisco Router 7200 (SNMP) 
        ↓
ap-snmp/collector.py (Producer) 
        ↓ (produce)
Apache Kafka (Topic: snmp-string) 
        ↓ (consume)
ap-worker/worker.py (Consumer) 
        ↓
Socket.io / Real-time Web Server 
        ↓
NetPulse Enterprise Dashboard (Modern Clean UI)
```

---

## ✨ การปรับปรุงดีไซน์หน้าเว็บ (Clean Enterprise Minimal UI)
- **Modern Dashboard Layout:** ออกแบบด้วย Tailwind CSS และ Inter Font สไตล์ SaaS / Enterprise Minimal
- **Executive Summary / KPI Overview:** สรุปจำนวน Router ที่ออนไลน์, ค่าเฉลี่ย CPU Load, และปริมาณ Traffic Ingress/Egress รวมแบบ Real-time
- **Unit Formatting อัตโนมัติ:** แปลงหน่วย Octets เป็น B, KB, MB, GB ให้อ่านง่าย
- **Interactive Charts:** กราฟเส้น Chart.js แบบ Area Gradient ดูลื่นไหล สบายตา
- **Dark / Light Mode:** รองรับการสลับโหมดสว่าง-มืดได้ตามต้องการ
- **Live Stream Logs:** หน้าต่างแสดง Audit log แบบ Real-time พร้อมปุ่ม Clear

---

## 📁 โครงสร้างโปรเจค

```
snmp-kafka-project-redesign/
├── docker-compose.yml          # Zookeeper, Kafka, Kafka-UI สำหรับรันบน Docker
├── cisco-snmp-config.txt       # คำสั่งคอนฟิก SNMP สำหรับ Cisco Router ใน GNS3
├── README.md
├── ap-snmp/                    # ฝั่ง Producer (ดึง SNMP ส่งเข้า Kafka)
│   ├── collector.py            # ดึงค่า SNMP จริงจาก Router
│   ├── collector_demo.py       # จำลองส่งข้อมูลทดสอบเข้า Kafka
│   └── requirements.txt
└── ap-worker/                  # ฝั่ง Consumer & Web Graph (รับจาก Kafka สตรีมขึ้นหน้าเว็บ)
    ├── worker.py               # Kafka Consumer -> Socket.io (พอร์ต 5000)
    ├── app_standalone.py       # สคริปต์ Demo ในตัวเดียว (รันดูหน้าเว็บได้ทันทีโดยไม่ต้องเปิด Kafka)
    ├── server.py               # Flask-SocketIO Server
    ├── server_node.js          # Node.js Server ทางเลือก (SSE stream)
    ├── index.html              # หน้าเว็บ NetPulse Clean Enterprise UI (Socket.io)
    ├── index_live.html         # หน้าเว็บ NetPulse Clean Enterprise UI (SSE)
    └── requirements.txt
```

---

## ⚡ วิธีทดสอบหน้าเว็บทันที (Standalone Mode - ไม่ต้องใช้ Kafka / Docker)

หากต้องการเปิดดูหน้าตา Dashboard สวยๆ ทันที:
```bash
cd ap-worker
pip install -r requirements.txt
python app_standalone.py
```
> เปิด Browser ไปที่: **`http://localhost:5000`**

---

## 🚀 วิธีติดตั้งและรันแบบเต็มระบบ (Production / Kafka Mode)

### 1. รัน Kafka ด้วย Docker Compose
```bash
docker-compose up -d
```
> ดูสถานะ Kafka ได้ที่ Web UI: `http://localhost:8080`

### 2. สร้าง Kafka Topic
```bash
docker exec -it kafka kafka-topics --create \
  --topic snmp-string \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### 3. รัน Consumer & Web Server
```bash
cd ap-worker
pip install -r requirements.txt
python worker.py
```
> เปิด Browser ไปที่ `http://localhost:5000`

### 4. รัน Collector (Producer)
```bash
cd ap-snmp
pip install -r requirements.txt
python collector.py
# หรือหากไม่มี Router จริง สามารถรันตัว mock: python collector_demo.py
```
