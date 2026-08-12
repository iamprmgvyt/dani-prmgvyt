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

const uploadsDir = path.join(__dirname, 'public', 'uploads');
if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, uploadsDir),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname);
    const name = crypto.randomUUID() + ext;
    cb(null, name);
  }
});

// Giới hạn Upload 100MB
const upload = multer({ 
  storage, 
  limits: { fileSize: 100 * 1024 * 1024 } 
});

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

const USERS = {
  dani: { username: 'dani', displayName: 'Dani', password: process.env.DANI_PASSWORD || 'dani123' },
  prmgvyt: { username: 'prmgvyt', displayName: 'prmgvyt', password: process.env.PRMGVYT_PASSWORD || 'prmgvyt123' }
};

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
      reply_to_id VARCHAR(36) NULL,
      status ENUM('sent','delivered','read') NOT NULL DEFAULT 'sent',
      created_at BIGINT NOT NULL,
      INDEX idx_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  try {
    await pool.query(`ALTER TABLE messages ADD COLUMN reply_to_id VARCHAR(36) NULL`);
  } catch (e) { /* Cột đã tồn tại */ }

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

const sessions = new Map();
const activeConnections = new Map(); // Quản lý Single Session

function isUserLoggedIn(username) {
  return activeConnections.has(username);
}

app.post('/api/login', (req, res) => {
  const { username, password } = req.body;
  const u = USERS[username];
  if (!u || u.password !== password) {
    return res.status(401).json({ error: 'Invalid username or password' });
  }

  // Chặn đăng nhập nếu tài khoản đang được dùng trên thiết bị khác
  if (isUserLoggedIn(username)) {
    return res.status(403).json({ error: 'Account already logged in on another device' });
  }

  const token = crypto.randomBytes(24).toString('hex');
  sessions.set(token, username);
  res.json({ token, username, displayName: u.displayName });
});

app.post('/api/logout', (req, res) => {
  const token = req.headers.authorization || req.body.token || req.query.token;
  if (token && sessions.has(token)) {
    const username = sessions.get(token);
    sessions.delete(token);
    if (activeConnections.has(username)) {
      const ws = activeConnections.get(username);
      ws.close(1000, 'Logged out');
      activeConnections.delete(username);
    }
  }
  res.json({ success: true });
});

function authFromToken(token) {
  return sessions.get(token) || null;
}

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
         r_msg.sender as reply_sender,
         r_msg.content as reply_content,
         (SELECT JSON_ARRAYAGG(JSON_OBJECT('username', r.username, 'emoji', r.emoji))
          FROM reactions r WHERE r.message_id = m.id) as reactions
       FROM messages m
       LEFT JOIN messages r_msg ON m.reply_to_id = r_msg.id
       WHERE (m.sender = ? AND m.recipient = ?) OR (m.sender = ? AND m.recipient = ?)
       ORDER BY m.created_at ASC
       LIMIT 200`,
      [username, other, other, username]
    );

    const messages = rows.map(r => {
      let parsedReactions = [];
      if (r.reactions) {
        if (typeof r.reactions === 'object') parsedReactions = r.reactions;
        else if (typeof r.reactions === 'string') {
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

function send(ws, data) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(data));
}

function sendToUser(username, data) {
  const ws = activeConnections.get(username);
  if (ws) send(ws, data);
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

  // Nếu kết nối trùng session thì đóng WebSocket mới lại
  if (activeConnections.has(username) && activeConnections.get(username) !== ws) {
    ws.close(4002, 'Already logged in elsewhere');
    return;
  }

  activeConnections.set(username, ws);

  const other = otherUser(username);
  sendToUser(other, { type: 'presence', username, online: true });
  send(ws, { type: 'presence', username: other, online: activeConnections.has(other) });

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
        const reply_to_id = msg.reply_to_id || null;
        if (!content) return;

        let reply_sender = null;
        let reply_content = null;

        if (reply_to_id) {
          const [rRows] = await pool.query(`SELECT sender, content FROM messages WHERE id = ?`, [reply_to_id]);
          if (rRows.length) {
            reply_sender = rRows[0].sender;
            reply_content = rRows[0].content;
          }
        }

        await pool.query(
          `INSERT INTO messages (id, sender, recipient, content, reply_to_id, status, created_at) VALUES (?, ?, ?, ?, ?, 'sent', ?)`,
          [id, username, other, content, reply_to_id, created_at]
        );

        const payload = { 
          type: 'message', id, sender: username, recipient: other, content, 
          reply_to_id, reply_sender, reply_content,
          status: 'sent', created_at, reactions: [] 
        };

        send(ws, { ...payload, self: true });

        const otherOnline = activeConnections.has(other);
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
    if (activeConnections.get(username) === ws) {
      activeConnections.delete(username);
      sendToUser(other, { type: 'presence', username, online: false });
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
