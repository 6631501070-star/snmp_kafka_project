const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = 5000;
const HTML_FILE = path.join(__dirname, 'index_live.html');

let traffic = {
  R1: { in: 2500000, out: 1200000 },
  R2: { in: 1800000, out: 950000 }
};

let latestData = {};

function updateData() {
  ['R1', 'R2'].forEach(r => {
    traffic[r].in += Math.floor(Math.random() * 70000) + 15000;
    traffic[r].out += Math.floor(Math.random() * 40000) + 10000;
    latestData[r] = {
      timestamp: new Date().toTimeString().split(' ')[0],
      router: r,
      host: r === 'R1' ? '192.168.222.130' : '192.168.222.131',
      sysDescr: `Cisco 7200 Router (${r}) IOS`,
      sysUpTime: `${Math.floor(Math.random() * 30) + 120} hrs`,
      sysName: r,
      ifInOctets: traffic[r].in.toString(),
      ifOutOctets: traffic[r].out.toString(),
      cpuLoad: (Math.floor(Math.random() * 45) + 10).toString()
    };
  });
}

updateData();
setInterval(updateData, 2000);

const clients = [];

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    fs.readFile(HTML_FILE, (err, data) => {
      if (err) {
        res.writeHead(500);
        res.end('Error loading dashboard');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
  } else if (req.url === '/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*'
    });
    res.write(`data: ${JSON.stringify(latestData)}\n\n`);
    clients.push(res);
    req.on('close', () => {
      const idx = clients.indexOf(res);
      if (idx !== -1) clients.splice(idx, 1);
    });
  } else {
    res.writeHead(404);
    res.end();
  }
});

setInterval(() => {
  const payload = `data: ${JSON.stringify(latestData)}\n\n`;
  clients.forEach(c => c.write(payload));
}, 2000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});
