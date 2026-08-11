require('dotenv').config();
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const mysql = require('mysql2/promise');
const path = require('path');
const crypto = require('crypto');
const multer = require('multer');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

// Tự động tạo thư mục public/uploads nếu chưa có
const uploadsDir = path.join(__dirname, 'public', 'uploads');
if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}

// Cấu hình Multer lưu file
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadsDir),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname);
    const name = crypto.randomUUID() + ext;
    cb(null, name);
  }
});
const upload = multer({ 
  storage, 
  limits: { fileSize: 50 * 1024 * 1024 } // Giới hạn 50MB
});

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ---------- USERS ----------
const USERS = {
  dani: { username: 'dani', displayName: 'Dani', password: process.env.DANI_PASSWORD || 'dani123' },
  prmgvyt: { username: 'prmgvyt', displayName: 'prmgvyt', password: process.env.PRMGVYT_PASSWORD || 'prmgvyt123' }
};

// ---------- DB POOL ----------
let pool;

async function initDb() {
  pool = mysql.createPool({
    uri: process.env.DATABASE_URL,
    waitForConnections: true,
    connectionLimit: 5,
    queueLimit: 0,
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : undefined
  });

  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id VARCHAR(36) PRIMARY KEY,
      sender VARCHAR(20) NOT NULL,
      recipient VARCHAR(20) NOT NULL,
      content TEXT NOT NULL,
      status ENUM('sent','delivered','read') NOT NULL DEFAULT 'sent',
      created_at BIGINT NOT NULL,
      INDEX idx_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS reactions (
      id VARCHAR(36) PRIMARY KEY,
      message_id VARCHAR(36) NOT NULL,
      username VARCHAR(20) NOT NULL,
      emoji VARCHAR(16) NOT NULL,
      UNIQUE KEY uniq_react (message_id, username),
      FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  console.log('[DB] Ready.');
}

function otherUser(username) {
  return username === 'dani' ? 'prmgvyt' : 'dani';
}

// ---------- AUTH ----------
const sessions = new Map();

app.post('/api/login', (req, res) => {
  const { username, password } = req.body;
  const u = USERS[username];
  if (!u || u.password !== password) {
    return res.status(401).json({ error: 'Invalid username or password' });
  }
  const token = crypto.randomBytes(24).toString('hex');
  sessions.set(token, username);
  res.json({ token, username, displayName: u.displayName });
});

function authFromToken(token) {
  return sessions.get(token) || null;
}

// ---------- API UPLOAD FILE ----------
app.post('/api/upload', upload.single('file'), (req, res) => {
  const token = req.headers.authorization || req.body.token || req.query.token;
  const username = authFromToken(token);
  if (!username) return res.status(401).json({ error: 'Unauthorized' });
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

  res.json({
    fileUrl: `/uploads/${req.file.filename}`,
    fileName: req.file.originalname,
    fileType: req.file.mimetype
  });
});

app.get('/api/history', async (req, res) => {
  try {
    const username = authFromToken(req.query.token);
    if (!username) return res.status(401).json({ error: 'Unauthorized' });

    const other = otherUser(username);
    const [rows] = await pool.query(
      `SELECT m.*,
         (SELECT JSON_ARRAYAGG(JSON_OBJECT('username', r.username, 'emoji', r.emoji))
          FROM reactions r WHERE r.message_id = m.id) as reactions
       FROM messages m
       WHERE (sender = ? AND recipient = ?) OR (sender = ? AND recipient = ?)
       ORDER BY created_at ASC
       LIMIT 200`,
      [username, other, other, username]
    );

    const messages = rows.map(r => {
      let parsedReactions = [];
      if (r.reactions) {
        if (typeof r.reactions === 'object') {
          parsedReactions = r.reactions;
        } else if (typeof r.reactions === 'string') {
          try { parsedReactions = JSON.parse(r.reactions); } catch { parsedReactions = []; }
        }
      }
      return {
        ...r,
        reactions: Array.isArray(parsedReactions) ? parsedReactions : []
      };
    });

    await pool.query(
      `UPDATE messages SET status = 'read' WHERE sender = ? AND recipient = ? AND status != 'read'`,
      [other, username]
    );

    res.json({ messages, me: username, other });
    broadcastReadReceipt(username, other);
  } catch (err) {
    console.error('[API Error] /api/history:', err);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

// ---------- WEBSOCKET ----------
const clients = new Map();

function send(ws, data) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(data));
}

function sendToUser(username, data) {
  const set = clients.get(username);
  if (!set) return;
  for (const ws of set) send(ws, data);
}

function broadcastReadReceipt(reader, otherUsername) {
  sendToUser(otherUsername, { type: 'read_receipt', by: reader });
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const token = url.searchParams.get('token');
  const username = authFromToken(token);

  if (!username) {
    ws.close(4001, 'Unauthorized');
    return;
  }

  if (!clients.has(username)) clients.set(username, new Set());
  clients.get(username).add(ws);

  const other = otherUser(username);
  sendToUser(other, { type: 'presence', username, online: true });
  send(ws, { type: 'presence', username: other, online: (clients.get(other) || new Set()).size > 0 });

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    try {
      if (msg.type === 'typing') {
        sendToUser(other, { type: 'typing', from: username, isTyping: !!msg.isTyping });
        return;
      }

      if (msg.type === 'message') {
        const id = crypto.randomUUID();
        const created_at = Date.now();
        const content = String(msg.content || '').slice(0, 10000).trim();
        if (!content) return;

        await pool.query(
          `INSERT INTO messages (id, sender, recipient, content, status, created_at) VALUES (?, ?, ?, ?, 'sent', ?)`,
          [id, username, other, content, created_at]
        );

        const payload = { type: 'message', id, sender: username, recipient: other, content, status: 'sent', created_at, reactions: [] };

        send(ws, { ...payload, self: true });

        const otherOnline = (clients.get(other) || new Set()).size > 0;
        if (otherOnline) {
          sendToUser(other, payload);
          await pool.query(`UPDATE messages SET status = 'delivered' WHERE id = ?`, [id]);
          send(ws, { type: 'status_update', id, status: 'delivered' });
        }
        return;
      }

      if (msg.type === 'read_ack') {
        await pool.query(
          `UPDATE messages SET status = 'read' WHERE sender = ? AND recipient = ? AND status != 'read'`,
          [other, username]
        );
        sendToUser(other, { type: 'read_receipt', by: username });
        return;
      }

      if (msg.type === 'reaction') {
        const { messageId, emoji } = msg;
        if (!messageId) return;

        if (emoji === null) {
          await pool.query(`DELETE FROM reactions WHERE message_id = ? AND username = ?`, [messageId, username]);
        } else {
          const rid = crypto.randomUUID();
          await pool.query(
            `INSERT INTO reactions (id, message_id, username, emoji) VALUES (?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE emoji = VALUES(emoji)`,
            [rid, messageId, username, emoji]
          );
        }
        const payload = { type: 'reaction', messageId, username, emoji };
        send(ws, payload);
        sendToUser(other, payload);
        return;
      }
    } catch (err) {
      console.error('[WS Message Error]:', err);
    }
  });

  ws.on('close', () => {
    const set = clients.get(username);
    if (set) {
      set.delete(ws);
      if (set.size === 0) {
        clients.delete(username);
        sendToUser(other, { type: 'presence', username, online: false });
      }
    }
  });
});

const PORT = process.env.PORT || 3000;

initDb().then(() => {
  server.listen(PORT, () => console.log(`[Server] Listening on ${PORT}`));
}).catch(err => {
  console.error('[DB] Failed to init:', err);
  process.exit(1);
});
