# 📡 SNMP Kafka Real-time Monitoring Project (Teal & Cyan Edition)

โปรเจคติดตามและแสดงผลข้อมูล Cisco Router แบบ Real-time ผ่าน SNMP, Apache Kafka, และ Socket.io พร้อม **หน้าเว็บ Clean Enterprise Minimal Dashboard ในธีมสีเขียวหัวเป็ด-ไซแอน (Teal & Cyan)**

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
NetPulse Enterprise Dashboard (Teal & Cyan Theme)
```

---

## 🎨 รายละเอียดธีมสี (Teal & Cyan Color Palette)
- **Primary Brand:** โทนสีเขียวหัวเป็ด (`Teal #0d9488` / `Teal 600`) สดชื่น สะอาดตา สไตล์ Cloudflare & Stripe
- **Secondary Accent:** โทนสีฟ้าไซแอน (`Cyan #06b6d4` / `Cyan 500`) ใช้กับทราฟฟิกขาเข้า (Rx Inbound)
- **Traffic Outbound (Tx):** ใช้เส้นกราฟและตัวเลขโทนสีเขียวหัวเป็ด Teal
- **Executive Summary / KPI Overview:** แดชบอร์ดสรุปโหนดออนไลน์, CPU เฉลี่ยรวม และ Ingress/Egress รวมแบบ Real-time
- **Dark / Light Mode:** รองรับการสลับโหมดสว่าง-มืดพร้อมปรับสีกราฟ Chart.js อัตโนมัติ

---

## ⚡ วิธีทดสอบหน้าเว็บทันที (Standalone Mode - ไม่ต้องใช้ Kafka / Docker)

```bash
cd ap-worker
python app_standalone.py
```
> เปิด Browser ไปที่: **`http://localhost:5000`**

---

## 🚀 วิธีติดตั้งและรันแบบเต็มระบบ (Production / Kafka Mode)

### 1. รัน Kafka ด้วย Docker Compose
```bash
docker-compose up -d
```

### 2. รัน Consumer & Web Server
```bash
cd ap-worker
python worker.py
```
> เปิด Browser ไปที่: `http://localhost:5000`

### 3. รัน Collector (Producer)
```bash
cd ap-snmp
python collector.py        # สำหรับ Router จริง
# หรือ python collector_demo.py  # สำหรับข้อมูล Mock
```
