require("dotenv").config();

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const { Readable } = require("node:stream");
const express = require("express");
const { google } = require("googleapis");
const mime = require("mime-types");
const mysql = require("mysql2/promise");

const {
  LINE_CHANNEL_SECRET,
  LINE_CHANNEL_ACCESS_TOKEN,
  GOOGLE_DRIVE_ROOT_FOLDER_ID,
  GOOGLE_DRIVE_IMAGE_FOLDER_ID,
  GOOGLE_DRIVE_VIDEO_FOLDER_ID,
  GOOGLE_DRIVE_DOCUMENT_FOLDER_ID,
  SUMMARY_DELAY_SECONDS = 45,
  REMINDER_MORNING_HOUR = 8,
  REMINDER_CHECK_INTERVAL_SECONDS = 60,
  RESTART_SECRET,
  PORT = 3000,
} = process.env;

// In-memory error buffer (last 50 errors)
const errorBuffer = [];
function captureError(context, error) {
  const entry = {
    ts: new Date().toISOString(),
    context,
    message: error && error.message ? error.message : String(error),
  };
  errorBuffer.push(entry);
  if (errorBuffer.length > 50) errorBuffer.shift();
}

const REQUIRED_ENV = [
  "LINE_CHANNEL_SECRET",
  "LINE_CHANNEL_ACCESS_TOKEN",
  "GOOGLE_DRIVE_ROOT_FOLDER_ID",
];

for (const key of REQUIRED_ENV) {
  if (!process.env[key]) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
}

const app = express();
const drive = google.drive({ version: "v3", auth: createGoogleAuth() });
const dbPool = createDatabasePool();
const folderCache = new Map();
const activeFolders = new Map();
const uploadSummaryBatches = new Map();
const configuredSummaryDelaySeconds = Number(SUMMARY_DELAY_SECONDS);
const summaryDelayMs =
  Number.isFinite(configuredSummaryDelaySeconds) && configuredSummaryDelaySeconds > 0
    ? configuredSummaryDelaySeconds * 1000
    : 45000;
const reminderMorningHour = getPositiveInteger(REMINDER_MORNING_HOUR, 8);
const reminderCheckIntervalMs = getPositiveInteger(REMINDER_CHECK_INTERVAL_SECONDS, 60) * 1000;

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "monitor.html"));
});

app.get("/api/stats", (_req, res) => {
  fetchStats().then((result) => {
    res.json(result);
  }).catch((err) => {
    res.status(500).json({ error: err.message });
  });
});

app.get("/api/errors", (_req, res) => {
  res.json({ errors: errorBuffer.slice().reverse() });
});

app.post("/api/restart", express.json(), (req, res) => {
  if (!RESTART_SECRET || (req.body && req.body.key) !== RESTART_SECRET) {
    return res.status(403).json({ error: "Forbidden" });
  }
  res.json({ ok: true });
  setTimeout(() => process.exit(0), 500);
});

app.post("/api/appointments/cancel", express.json(), (req, res) => {
  const { code, key } = req.body;
  if (!RESTART_SECRET || key !== RESTART_SECRET) {
    return res.status(403).json({ error: "Forbidden" });
  }
  if (!code) {
    return res.status(400).json({ error: "Missing appointment code" });
  }

  cancelAppointmentFromWeb(code)
    .then((success) => {
      res.json({ ok: true, success });
    })
    .catch((err) => {
      res.status(500).json({ error: err.message });
      captureError("web-cancel", err);
    });
});

async function cancelAppointmentFromWeb(code) {
  if (!dbPool) {
    throw new Error("MySQL Database is not enabled");
  }
  const [result] = await dbPool.execute(
    `
      UPDATE appointments
      SET status = 'cancelled'
      WHERE appointment_code = :code
        AND status = 'active'
    `,
    { code }
  );
  return result.affectedRows > 0;
}


app.get("/health", (_req, res) => {
  checkHealth().then((result) => {
    res.status(result.ok ? 200 : 503).json(result);
  });
});

async function checkHealth() {
  const checks = await Promise.allSettled([checkDrive(), checkDatabase()]);
  const [driveResult, dbResult] = checks;

  const drive =
    driveResult.status === "fulfilled"
      ? { ok: true }
      : { ok: false, error: driveResult.reason?.message || "unknown" };

  const database =
    dbResult.status === "fulfilled"
      ? dbResult.value
      : { ok: false, error: dbResult.reason?.message || "unknown" };

  return {
    ok: drive.ok && database.ok,
    drive,
    database,
  };
}

async function checkDrive() {
  await drive.files.list({ pageSize: 1, fields: "files(id)", q: "trashed = false", supportsAllDrives: true, includeItemsFromAllDrives: true });
}

async function checkDatabase() {
  if (!dbPool) {
    return { ok: true, note: "disabled" };
  }
  await dbPool.execute("SELECT 1");
  return { ok: true };
}

async function fetchStats() {
  const [images, videos, documents, appointments] = await Promise.allSettled([
    countDriveFiles("image"),
    countDriveFiles("video"),
    countDriveFiles("document"),
    fetchUpcomingAppointments(),
  ]);

  return {
    files: {
      images: images.status === "fulfilled" ? images.value : 0,
      videos: videos.status === "fulfilled" ? videos.value : 0,
      documents: documents.status === "fulfilled" ? documents.value : 0,
    },
    appointments: appointments.status === "fulfilled" ? appointments.value : [],
  };
}

async function countDriveFiles(category) {
  const folderName = { image: "Images", video: "Videos", document: "Documents" }[category];
  const list = await drive.files.list({
    q: `'${GOOGLE_DRIVE_ROOT_FOLDER_ID}' in parents and name = '${folderName}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
    fields: "files(id)",
    pageSize: 1,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });

  const folder = list.data.files && list.data.files[0];
  if (!folder) return 0;

  const files = await drive.files.list({
    q: `'${folder.id}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false`,
    fields: "files(id)",
    pageSize: 1000,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });

  return (files.data.files && files.data.files.length) || 0;
}

async function fetchUpcomingAppointments() {
  if (!dbPool) return [];

  const [rows] = await dbPool.execute(`
    SELECT appointment_code, title, details, appointment_at
    FROM appointments
    WHERE status = 'active' AND appointment_at >= UTC_TIMESTAMP()
    ORDER BY appointment_at ASC
    LIMIT 5
  `);

  return rows.map((r) => ({
    code: r.appointment_code,
    title: r.title,
    details: r.details || "",
    time: formatThaiDateTime(r.appointment_at),
  }));
}

app.post("/webhook", express.raw({ type: "*/*" }), (req, res) => {
  if (!isValidLineSignature(req)) {
    return res.status(401).json({ error: "Invalid LINE signature" });
  }

  let body;
  try {
    body = JSON.parse(req.body.toString("utf8"));
  } catch (error) {
    return res.status(400).json({ error: "Invalid JSON payload" });
  }

  res.sendStatus(200);

  for (const event of body.events || []) {
    handleLineEvent(event).catch((error) => {
      console.error("Failed to handle LINE event", {
        eventId: event.webhookEventId,
        messageId: event.message && event.message.id,
        error,
      });
      captureError("upload", error);

      notifyUploadFailure(event).catch((notifyError) => {
        console.error("Failed to notify upload failure", notifyError);
        captureError("notify", notifyError);
      });
    });
  }
});

async function handleLineEvent(event) {
  if (event.type !== "message" || !event.message) {
    return;
  }

  if (event.message.type === "text") {
    await handleTextCommand(event);
    return;
  }

  const category = getMessageCategory(event.message);
  if (!category) {
    return;
  }

  const messageId = event.message.id;
  const duplicate = await findExistingUpload(messageId);
  if (duplicate) {
    console.log(`Skip duplicate LINE message ${messageId}`);
    return;
  }

  const content = await fetchLineContent(messageId);
  const organization = getActiveOrganization(event.source);
  const metadata = buildDriveFileMetadata(event, category, content.contentType, organization);
  const folderId = await getFolderId(category, organization && organization.folderName);
  const folderPath = getDriveFolderPath(category, organization);

  // Buffer the stream to calculate md5Checksum and check for duplicate files in Drive
  const chunks = [];
  const hash = crypto.createHash("md5");
  for await (const chunk of content.stream) {
    chunks.push(chunk);
    hash.update(chunk);
  }
  const buffer = Buffer.concat(chunks);
  const md5Hex = hash.digest("hex");

  // Check if file with same checksum already exists in target folder
  const duplicateCheck = await drive.files.list({
    q: `'${folderId}' in parents and md5Checksum = '${md5Hex}' and trashed = false`,
    fields: "files(id,name,webViewLink)",
    pageSize: 1,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });

  const driveDuplicate = duplicateCheck.data.files && duplicateCheck.data.files[0];
  if (driveDuplicate) {
    console.log(
      `Skip duplicate file upload. File already exists in folder: ${driveDuplicate.name} (ID: ${driveDuplicate.id})`
    );
    scheduleUploadSummary(event.source, event.replyToken, {
      category,
      folderPath,
      folderId,
      isDuplicate: true,
      fileName: driveDuplicate.name,
    });
    return;
  }

  await drive.files.create({
    requestBody: {
      name: metadata.fileName,
      parents: [folderId],
      mimeType: metadata.mimeType,
      description: metadata.description,
      appProperties: {
        lineMessageId: messageId,
      },
    },
    media: {
      mimeType: metadata.mimeType,
      body: Readable.from(buffer),
    },
    fields: "id,name,webViewLink",
    supportsAllDrives: true,
  });

  console.log(
    `Uploaded ${category}${organization ? `/${organization.folderName}` : ""}: ${metadata.fileName}`
  );

  scheduleUploadSummary(event.source, event.replyToken, {
    category,
    folderPath,
    folderId,
  });
}

async function handleTextCommand(event) {
  const command = parseCommand(event.message.text || "");
  if (!command) {
    return;
  }

  if (command.type.startsWith("appointment.")) {
    await handleAppointmentCommand(event, command);
    return;
  }

  if (command.type === "help") {
    await replyLineMessage(event.replyToken, getHelpMessage());
    return;
  }

  if (command.type === "status") {
    const organization = getActiveOrganization(event.source);
    await replyLineMessage(
      event.replyToken,
      organization
        ? `📂 หมวดปัจจุบัน: ${organization.folderName}\nไฟล์ถัดไปจะเข้าโฟลเดอร์ย่อยนี้`
        : "📂 ยังไม่ได้ตั้งหมวด\nใช้ /folder ชื่อหมวด เช่น /folder งานประชุม"
    );
    return;
  }

  if (command.type === "clear") {
    const sourceKey = getSourceKey(event.source);
    activeFolders.delete(sourceKey);
    await persistActiveFolder(sourceKey, null);
    await replyLineMessage(event.replyToken, "🗂 ล้างหมวดแล้ว\nไฟล์ถัดไปจะเข้าโฟลเดอร์หลักตามประเภทไฟล์");
    return;
  }

  if (command.type === "set") {
    const sourceKey = getSourceKey(event.source);
    activeFolders.set(sourceKey, { folderName: command.folderName });
    await persistActiveFolder(sourceKey, command.folderName);
    await replyLineMessage(event.replyToken, "✅ พร้อมเก็บไฟล์ อัพโหลดได้เลยค่ะ");
  }
}

async function handleAppointmentCommand(event, command) {
  if (!dbPool) {
    await replyLineMessage(
      event.replyToken,
      "ยังไม่ได้เปิดใช้งานระบบนัดหมาย กรุณาตั้งค่า DB_HOST, DB_USER, DB_PASSWORD และ DB_NAME บน Hostinger"
    );
    return;
  }

  if (command.type === "appointment.help") {
    await replyLineMessage(event.replyToken, getAppointmentHelpMessage());
    return;
  }

  if (command.type === "appointment.create") {
    await createAppointment(event, command);
    return;
  }

  if (command.type === "appointment.list") {
    await listAppointments(event);
    return;
  }

  if (command.type === "appointment.cancel") {
    await cancelAppointment(event, command.code);
  }
}

async function createAppointment(event, command) {
  const appointmentAt = parseBangkokDateTime(command.appointmentText);

  if (!command.title || !appointmentAt) {
    await replyLineMessage(event.replyToken, getAppointmentHelpMessage());
    return;
  }

  if (appointmentAt.getTime() <= Date.now()) {
    await replyLineMessage(event.replyToken, "เวลานัดหมายต้องเป็นเวลาในอนาคต");
    return;
  }

  const code = await createAppointmentCode();

  await dbPool.execute(
    `
      INSERT INTO appointments
        (appointment_code, source_type, source_id, title, details, appointment_at)
      VALUES
        (:code, :sourceType, :sourceId, :title, :details, :appointmentAt)
    `,
    {
      code,
      sourceType: event.source && event.source.type ? event.source.type : "unknown",
      sourceId: getSourceId(event.source),
      title: command.title,
      details: command.details || null,
      appointmentAt: toMysqlDateTime(appointmentAt),
    }
  );

  await replyLineMessage(
    event.replyToken,
    [
      "✅ บันทึกนัดหมายแล้ว",
      "─────────────────",
      `📌 ${command.title}`,
      `🕐 ${formatThaiDateTime(appointmentAt)}`,
      `🔖 รหัส: ${code}`,
      command.details ? `📝 ${command.details}` : "",
    ]
      .filter(Boolean)
      .join("\n")
  );
}

async function listAppointments(event) {
  const [rows] = await dbPool.execute(
    `
      SELECT appointment_code, title, details, appointment_at
      FROM appointments
      WHERE source_type = :sourceType
        AND source_id = :sourceId
        AND status = 'active'
        AND appointment_at >= UTC_TIMESTAMP()
      ORDER BY appointment_at ASC
      LIMIT 10
    `,
    {
      sourceType: event.source && event.source.type ? event.source.type : "unknown",
      sourceId: getSourceId(event.source),
    }
  );

  if (!rows.length) {
    await replyLineMessage(event.replyToken, "ยังไม่มีนัดหมายที่กำลังจะมาถึง");
    return;
  }

  await replyLineMessage(
    event.replyToken,
    [
      "📅 นัดหมายที่กำลังจะมาถึง",
      "─────────────────",
      ...rows.map((row) => {
        const details = row.details ? `\n📝 ${row.details}` : "";
        return `📌 ${row.title}\n🕐 ${formatThaiDateTime(row.appointment_at)}\n🔖 ${row.appointment_code}${details}`;
      }),
    ].join("\n\n")
  );
}

async function cancelAppointment(event, code) {
  const [result] = await dbPool.execute(
    `
      UPDATE appointments
      SET status = 'cancelled'
      WHERE appointment_code = :code
        AND source_type = :sourceType
        AND source_id = :sourceId
        AND status = 'active'
    `,
    {
      code,
      sourceType: event.source && event.source.type ? event.source.type : "unknown",
      sourceId: getSourceId(event.source),
    }
  );

  await replyLineMessage(
    event.replyToken,
    result.affectedRows > 0 ? `✅ ยกเลิกนัดหมาย ${code} แล้ว` : `❌ ไม่พบนัดหมายรหัส ${code} ในกลุ่มนี้`
  );
}

function getAppointmentHelpMessage() {
  return [
    "📅 คำสั่งนัดหมาย",
    "─────────────────",
    "/นัด หัวข้อ DD/MM/YYYY HH.mm สถานที่",
    "/นัดหมาย — ดูตารางนัดหมาย",
    "/ยกเลิกนัด รหัส",
    "",
    "💡 ตัวอย่าง",
    "/นัด อบรมต่อต้านยาเสพติด 30/06/2569 13.30 ห้องประชุมเอราวรรณ งานปกครอง",
  ].join("\n");
}

function createGoogleAuth() {
  const credentials = getGoogleCredentials();
  return new google.auth.GoogleAuth({
    credentials,
    scopes: ["https://www.googleapis.com/auth/drive"],
  });
}

function getGoogleCredentials() {
  if (process.env.GOOGLE_SERVICE_ACCOUNT_BASE64) {
    const json = Buffer.from(process.env.GOOGLE_SERVICE_ACCOUNT_BASE64, "base64").toString("utf8");
    return normalizeServiceAccount(JSON.parse(json));
  }

  if (process.env.GOOGLE_SERVICE_ACCOUNT_JSON) {
    return normalizeServiceAccount(JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON));
  }

  if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    const file = fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS, "utf8");
    return normalizeServiceAccount(JSON.parse(file));
  }

  throw new Error(
    "Set GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SERVICE_ACCOUNT_BASE64, or GOOGLE_APPLICATION_CREDENTIALS"
  );
}

function normalizeServiceAccount(credentials) {
  if (credentials.private_key) {
    credentials.private_key = credentials.private_key.replace(/\\n/g, "\n");
  }
  return credentials;
}

function createDatabasePool() {
  const { DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT = 3306 } = process.env;

  if (!DB_HOST || !DB_USER || !DB_NAME) {
    console.warn("Appointment commands disabled: set DB_HOST, DB_USER, DB_PASSWORD, and DB_NAME");
    return null;
  }

  return mysql.createPool({
    host: DB_HOST,
    user: DB_USER,
    password: DB_PASSWORD || "",
    database: DB_NAME,
    port: Number(DB_PORT) || 3306,
    waitForConnections: true,
    connectionLimit: 5,
    namedPlaceholders: true,
    timezone: "Z",
  });
}

async function persistActiveFolder(sourceKey, folderName) {
  if (!dbPool) {
    return;
  }

  if (!folderName) {
    await dbPool.execute("DELETE FROM active_folders WHERE source_key = :sourceKey", { sourceKey });
    return;
  }

  await dbPool.execute(
    `INSERT INTO active_folders (source_key, folder_name)
     VALUES (:sourceKey, :folderName)
     ON DUPLICATE KEY UPDATE folder_name = :folderName`,
    { sourceKey, folderName }
  );
}

async function loadActiveFolders() {
  if (!dbPool) {
    return;
  }

  const [rows] = await dbPool.execute("SELECT source_key, folder_name FROM active_folders");
  for (const row of rows) {
    activeFolders.set(row.source_key, { folderName: row.folder_name });
  }

  console.log(`Loaded ${rows.length} active folder(s) from database`);
}

async function initializeDatabase() {
  if (!dbPool) {
    return;
  }

  await dbPool.execute(`
    CREATE TABLE IF NOT EXISTS active_folders (
      source_key VARCHAR(120) NOT NULL PRIMARY KEY,
      folder_name VARCHAR(80) NOT NULL,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  await dbPool.execute(`
    CREATE TABLE IF NOT EXISTS appointments (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
      appointment_code VARCHAR(20) NOT NULL UNIQUE,
      source_type VARCHAR(20) NOT NULL,
      source_id VARCHAR(80) NOT NULL,
      title VARCHAR(255) NOT NULL,
      details TEXT NULL,
      appointment_at DATETIME NOT NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'active',
      morning_reminder_sent TINYINT(1) NOT NULL DEFAULT 0,
      ten_minute_reminder_sent TINYINT(1) NOT NULL DEFAULT 0,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      INDEX idx_appointments_due (status, appointment_at),
      INDEX idx_appointments_source (source_type, source_id, status)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
  `);

  console.log("Appointment database is ready");
}

function getPositiveInteger(value, fallback) {
  const number = Number(value);
  return Number.isInteger(number) && number > 0 ? number : fallback;
}

function isValidLineSignature(req) {
  const signature = req.get("x-line-signature");
  if (!signature) {
    return false;
  }

  const expected = crypto
    .createHmac("sha256", LINE_CHANNEL_SECRET)
    .update(req.body)
    .digest("base64");

  const signatureBuffer = Buffer.from(signature);
  const expectedBuffer = Buffer.from(expected);

  if (signatureBuffer.length !== expectedBuffer.length) {
    return false;
  }

  return crypto.timingSafeEqual(signatureBuffer, expectedBuffer);
}

function getMessageCategory(message) {
  if (message.type === "image") {
    return "image";
  }
  if (message.type === "video") {
    return "video";
  }
  if (message.type === "file") {
    return "document";
  }
  return null;
}

function parseCommand(text) {
  const trimmed = text.trim();
  const lower = trimmed.toLowerCase();

  const appointmentCommand = parseAppointmentCommand(trimmed);
  if (appointmentCommand) {
    return appointmentCommand;
  }

  if (lower === "/help" || lower === "/folder help" || lower === "/หมวด help") {
    return { type: "help" };
  }

  if (lower === "/folder" || lower === "/หมวด" || lower === "/tag") {
    return { type: "status" };
  }

  if (
    lower === "/folder off" ||
    lower === "/folder clear" ||
    lower === "/หมวด ปิด" ||
    lower === "/หมวด ล้าง" ||
    lower === "/tag off" ||
    lower === "/tag clear"
  ) {
    return { type: "clear" };
  }

  const match = trimmed.match(/^\/(?:folder|tag|หมวด)\s+(.+)$/i);
  if (!match) {
    return null;
  }

  const folderName = sanitizeDriveFolderName(match[1]);
  if (!folderName) {
    return { type: "help" };
  }

  return { type: "set", folderName };
}

function parseAppointmentCommand(text) {
  const lower = text.toLowerCase();

  if (lower === "/นัด help" || lower === "/appointment help") {
    return { type: "appointment.help" };
  }

  if (lower === "/นัดหมาย" || lower === "/appointments") {
    return { type: "appointment.list" };
  }

  const cancelMatch = text.match(/^\/(?:ยกเลิกนัด|cancelappt)\s+([A-Za-z0-9-]+)$/i);
  if (cancelMatch) {
    return { type: "appointment.cancel", code: cancelMatch[1].toUpperCase() };
  }

  const createMatch = text.match(/^\/(?:นัด|appointment)\s+(.+)$/i);
  if (!createMatch) {
    return null;
  }

  const body = createMatch[1].trim();
  // Format: /นัด [title] DD/MM/YYYY HH.mm [details]
  const dateTimeMatch = body.match(/^(.+?)\s+(\d{1,2}\/\d{1,2}\/\d{4})\s+(\d{1,2}[.:]\d{2})\s*(.*)$/);
  if (!dateTimeMatch) {
    return { type: "appointment.help" };
  }

  return {
    type: "appointment.create",
    title: sanitizePlainText(dateTimeMatch[1], 180),
    appointmentText: `${dateTimeMatch[2]} ${dateTimeMatch[3]}`,
    details: sanitizePlainText(dateTimeMatch[4], 800),
  };
}

function sanitizePlainText(value, maxLength) {
  return String(value || "")
    .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, "")
    .trim()
    .slice(0, maxLength);
}

function getHelpMessage() {
  return [
    "📂 จัดระเบียบไฟล์",
    "/folder ชื่อหมวด — ตั้งหมวด",
    "/folder — ดูหมวดปัจจุบัน",
    "/folder off — ปิดหมวด",
    "",
    "📅 นัดหมาย",
    "/นัด หัวข้อ DD/MM/YYYY HH.mm สถานที่",
    "/นัดหมาย — ดูนัดหมายที่กำลังจะมาถึง",
    "/ยกเลิกนัด รหัส",
    "",
    "💡 ตัวอย่าง",
    "/folder งานประชุม",
    "/นัด อบรมต่อต้านยาเสพติด 30/06/2569 13.30 ห้องประชุมเอราวรรณ",
  ].join("\n");
}

async function replyLineMessage(replyToken, text) {
  if (!replyToken) {
    return;
  }

  const response = await fetch("https://api.line.me/v2/bot/message/reply", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      replyToken,
      messages: [{ type: "text", text }],
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`LINE reply API failed: ${response.status} ${errorText}`);
  }
}

async function pushLineMessage(to, text) {
  if (!to) {
    return;
  }

  const response = await fetch("https://api.line.me/v2/bot/message/push", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      to,
      messages: [{ type: "text", text }],
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`LINE push API failed: ${response.status} ${errorText}`);
  }
}

function scheduleUploadSummary(source, replyToken, upload) {
  const sourceKey = getSourceKey(source);
  const batch = uploadSummaryBatches.get(sourceKey) || {
    replyToken: null,
    uploads: [],
    timer: null,
  };

  batch.uploads.push(upload);
  if (replyToken) batch.replyToken = replyToken;

  if (batch.timer) clearTimeout(batch.timer);
  batch.timer = setTimeout(() => {
    flushUploadSummary(sourceKey).catch((error) => {
      console.error("Failed to send upload summary", error);
      captureError("summary", error);
    });
  }, summaryDelayMs);

  uploadSummaryBatches.set(sourceKey, batch);
}

async function flushUploadSummary(sourceKey) {
  const batch = uploadSummaryBatches.get(sourceKey);
  if (!batch) {
    return;
  }

  uploadSummaryBatches.delete(sourceKey);

  const message = buildUploadSummaryMessage(batch.uploads);
  await replyLineMessage(batch.replyToken, message);

  if (activeFolders.has(sourceKey)) {
    activeFolders.delete(sourceKey);
    await persistActiveFolder(sourceKey, null);
  }
}

function buildUploadSummaryMessage(uploads) {
  const newUploads = uploads.filter((u) => !u.isDuplicate);
  const duplicates = uploads.filter((u) => u.isDuplicate);
  const totalNew = newUploads.length;

  const countsByPath = new Map();
  const folderIdByPath = new Map();

  for (const upload of newUploads) {
    countsByPath.set(upload.folderPath, (countsByPath.get(upload.folderPath) || 0) + 1);
    folderIdByPath.set(upload.folderPath, upload.folderId);
  }

  // Also include folder paths/IDs of duplicates if they aren't already listed
  for (const upload of duplicates) {
    if (!folderIdByPath.has(upload.folderPath)) {
      folderIdByPath.set(upload.folderPath, upload.folderId);
      countsByPath.set(upload.folderPath, 0);
    }
  }

  const folderLines = Array.from(folderIdByPath.entries()).map(([folderPath, folderId]) => {
    const count = countsByPath.get(folderPath);
    const icon = folderPath.startsWith("Images") ? "🖼" : folderPath.startsWith("Videos") ? "🎬" : "📄";
    const thaiPath = folderPath.replace("Images", "รูปภาพ").replace("Videos", "วิดีโอ").replace("Documents", "เอกสาร");
    const newText = count > 0 ? ` (+${count} ไฟล์ใหม่)` : "";
    const dupCount = duplicates.filter((d) => d.folderPath === folderPath).length;
    const dupText = dupCount > 0 ? ` (ข้ามไฟล์ซ้ำ ${dupCount})` : "";
    return `${icon} ${thaiPath}${newText}${dupText}\n🔗 ${getDriveFolderLink(folderId)}`;
  });

  const header =
    totalNew > 0
      ? `✅ บันทึกไฟล์สำเร็จ ${totalNew} ไฟล์ใหม่`
      : duplicates.length > 0
      ? `ℹ️ ข้ามการบันทึกไฟล์ (พบไฟล์ซ้ำในระบบ)`
      : `✅ จัดการไฟล์เรียบร้อย`;

  return [
    header,
    "─────────────────",
    ...folderLines,
  ].join("\n");
}

async function notifyUploadFailure(event) {
  if (!event || event.type !== "message" || !event.message || !getMessageCategory(event.message)) {
    return;
  }

  await pushLineMessage(
    getSourceId(event.source),
    "❌ บันทึกไฟล์ไม่สำเร็จ\nกรุณาลองส่งใหม่อีกครั้ง หรือติดต่อผู้ดูแลระบบ"
  );
}

async function createAppointmentCode() {
  for (let attempt = 0; attempt < 5; attempt += 1) {
    const code = `A-${crypto.randomInt(1000, 10000)}`;
    const [rows] = await dbPool.execute(
      "SELECT id FROM appointments WHERE appointment_code = :code LIMIT 1",
      { code }
    );

    if (!rows.length) {
      return code;
    }
  }

  return `A-${Date.now().toString(36).toUpperCase()}`;
}

function parseBangkokDateTime(value) {
  const str = String(value || "").trim();

  // Format: DD/MM/YYYY HH.mm or DD/MM/YYYY HH:mm (Buddhist Era year auto-converted)
  const thaiMatch = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2})[.:](\d{2})$/);
  if (thaiMatch) {
    let [, dayText, monthText, yearText, hourText, minuteText] = thaiMatch;
    let year = Number(yearText);
    if (year > 2400) year -= 543; // แปลง พ.ศ. → ค.ศ.
    return parseBangkokDateTime(`${year}-${monthText.padStart(2,"0")}-${dayText.padStart(2,"0")} ${hourText.padStart(2,"0")}:${minuteText}`);
  }

  // Original format: YYYY-MM-DD HH:mm
  const match = str.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})$/);
  if (!match) {
    return null;
  }

  const [, yearText, monthText, dayText, hourText, minuteText] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);

  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > 31 ||
    hour < 0 ||
    hour > 23 ||
    minute < 0 ||
    minute > 59
  ) {
    return null;
  }

  const date = new Date(Date.UTC(year, month - 1, day, hour - 7, minute, 0));
  const bangkokParts = getBangkokDateParts(date);

  if (
    bangkokParts.year !== year ||
    bangkokParts.month !== month ||
    bangkokParts.day !== day ||
    bangkokParts.hour !== hour ||
    bangkokParts.minute !== minute
  ) {
    return null;
  }

  return date;
}

function getBangkokDateParts(date) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Bangkok",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const parts = Object.fromEntries(formatter.formatToParts(new Date(date)).map((part) => [part.type, part.value]));

  return {
    year: Number(parts.year),
    month: Number(parts.month),
    day: Number(parts.day),
    hour: Number(parts.hour),
    minute: Number(parts.minute),
  };
}

function toMysqlDateTime(date) {
  const pad = (value) => String(value).padStart(2, "0");
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(
    date.getUTCHours()
  )}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}`;
}

function formatThaiDateTime(date) {
  return new Intl.DateTimeFormat("th-TH", {
    timeZone: "Asia/Bangkok",
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(date));
}

async function checkAppointmentReminders() {
  if (!dbPool) {
    return;
  }

  await sendMorningReminders();
  await sendTenMinuteReminders();
}

async function sendMorningReminders() {
  const nowParts = getBangkokDateParts(new Date());
  if (nowParts.hour < reminderMorningHour) {
    return;
  }

  const [rows] = await dbPool.execute(
    `
      SELECT id, source_id, appointment_code, title, details, appointment_at
      FROM appointments
      WHERE status = 'active'
        AND morning_reminder_sent = 0
        AND DATE(CONVERT_TZ(appointment_at, '+00:00', '+07:00')) = DATE(CONVERT_TZ(UTC_TIMESTAMP(), '+00:00', '+07:00'))
        AND appointment_at >= UTC_TIMESTAMP()
      ORDER BY appointment_at ASC
      LIMIT 50
    `
  );

  for (const appointment of rows) {
    await pushLineMessage(appointment.source_id, buildMorningReminderMessage(appointment));
    await dbPool.execute("UPDATE appointments SET morning_reminder_sent = 1 WHERE id = :id", {
      id: appointment.id,
    });
  }
}

async function sendTenMinuteReminders() {
  const [rows] = await dbPool.execute(
    `
      SELECT id, source_id, appointment_code, title, details, appointment_at
      FROM appointments
      WHERE status = 'active'
        AND ten_minute_reminder_sent = 0
        AND appointment_at > UTC_TIMESTAMP()
        AND appointment_at <= DATE_ADD(UTC_TIMESTAMP(), INTERVAL 10 MINUTE)
      ORDER BY appointment_at ASC
      LIMIT 50
    `
  );

  for (const appointment of rows) {
    await pushLineMessage(appointment.source_id, buildTenMinuteReminderMessage(appointment));
    await dbPool.execute("UPDATE appointments SET ten_minute_reminder_sent = 1 WHERE id = :id", {
      id: appointment.id,
    });
  }
}

function buildMorningReminderMessage(appointment) {
  return [
    "🌅 แจ้งเตือนนัดหมายวันนี้",
    "─────────────────",
    `📌 ${appointment.title}`,
    `🕐 ${formatThaiDateTime(appointment.appointment_at)}`,
    `🔖 รหัส: ${appointment.appointment_code}`,
    appointment.details ? `📝 ${appointment.details}` : "",
  ]
    .filter(Boolean)
    .join("\n");
}

function buildTenMinuteReminderMessage(appointment) {
  return [
    "⏰ อีก 10 นาทีถึงเวลานัดหมาย!",
    "─────────────────",
    `📌 ${appointment.title}`,
    `🕐 ${formatThaiDateTime(appointment.appointment_at)}`,
    `🔖 รหัส: ${appointment.appointment_code}`,
    appointment.details ? `📝 ${appointment.details}` : "",
  ]
    .filter(Boolean)
    .join("\n");
}

async function fetchLineContent(messageId) {
  const response = await fetch(`https://api-data.line.me/v2/bot/message/${messageId}/content`, {
    headers: {
      Authorization: `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}`,
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`LINE content API failed: ${response.status} ${text}`);
  }

  return {
    contentType: response.headers.get("content-type") || "application/octet-stream",
    stream: Readable.fromWeb(response.body),
  };
}

function buildDriveFileMetadata(event, category, contentType, organization) {
  const message = event.message;
  const sourceId = getSourceId(event.source);
  const timestamp = new Date(event.timestamp || Date.now()).toISOString().replace(/[:.]/g, "-");
  const originalName = message.type === "file" ? message.fileName : "";
  const extension = getExtension(category, contentType, originalName);
  const safeOriginalName = originalName ? sanitizeFileName(originalName) : "";
  const fileName = safeOriginalName || `${timestamp}_${sourceId}_${message.id}${extension}`;
  const mimeType = mime.lookup(fileName) || contentType || "application/octet-stream";

  return {
    fileName,
    mimeType,
    description: [
      `LINE message ID: ${message.id}`,
      `Source: ${event.source ? event.source.type : "unknown"} ${sourceId}`,
      `Sent at: ${new Date(event.timestamp || Date.now()).toISOString()}`,
      organization ? `Organization folder: ${organization.folderName}` : "",
    ]
      .filter(Boolean)
      .join("\n"),
  };
}

function getExtension(category, contentType, originalName) {
  if (originalName && path.extname(originalName)) {
    return "";
  }

  const fromMime = mime.extension(contentType);
  if (fromMime) {
    return `.${fromMime}`;
  }

  if (category === "image") {
    return ".jpg";
  }
  if (category === "video") {
    return ".mp4";
  }
  return "";
}

function sanitizeFileName(fileName) {
  const parsed = path.parse(fileName);
  const base = parsed.name.replace(/[<>:"/\\|?*\x00-\x1F]/g, "_").slice(0, 120);
  const ext = parsed.ext.replace(/[<>:"/\\|?*\x00-\x1F]/g, "_").slice(0, 20);
  return `${base || "line-file"}${ext}`;
}

function sanitizeDriveFolderName(folderName) {
  return folderName.replace(/[<>:"/\\|?*\x00-\x1F]/g, "_").trim().slice(0, 80);
}

function getDriveFolderPath(category, organization) {
  const categoryFolderName = {
    image: "Images",
    video: "Videos",
    document: "Documents",
  }[category];

  if (!organization) {
    return categoryFolderName;
  }

  return `${categoryFolderName}/${organization.folderName}`;
}

function getDriveFolderLink(folderId) {
  return `https://drive.google.com/drive/folders/${folderId}`;
}

async function getFolderId(category, organizationFolderName) {
  const configured = {
    image: GOOGLE_DRIVE_IMAGE_FOLDER_ID,
    video: GOOGLE_DRIVE_VIDEO_FOLDER_ID,
    document: GOOGLE_DRIVE_DOCUMENT_FOLDER_ID,
  }[category];

  if (configured) {
    if (!organizationFolderName) {
      return configured;
    }

    return getOrCreateFolder(organizationFolderName, configured);
  }

  const folderName = {
    image: "Images",
    video: "Videos",
    document: "Documents",
  }[category];

  const categoryFolderId = await getOrCreateFolder(folderName, GOOGLE_DRIVE_ROOT_FOLDER_ID);

  if (!organizationFolderName) {
    return categoryFolderId;
  }

  return getOrCreateFolder(organizationFolderName, categoryFolderId);
}

async function getOrCreateFolder(folderName, parentFolderId) {
  const cacheKey = `${parentFolderId}:${folderName}`;

  if (folderCache.has(cacheKey)) {
    return folderCache.get(cacheKey);
  }

  const escapedName = folderName.replace(/'/g, "\\'");
  const list = await drive.files.list({
    q: [
      `'${parentFolderId}' in parents`,
      `name = '${escapedName}'`,
      "mimeType = 'application/vnd.google-apps.folder'",
      "trashed = false",
    ].join(" and "),
    fields: "files(id,name)",
    pageSize: 1,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });

  const existing = list.data.files && list.data.files[0];
  if (existing) {
    folderCache.set(cacheKey, existing.id);
    return existing.id;
  }

  const created = await drive.files.create({
    requestBody: {
      name: folderName,
      mimeType: "application/vnd.google-apps.folder",
      parents: [parentFolderId],
    },
    fields: "id",
    supportsAllDrives: true,
  });

  // Make newly created folder shared to anyone with the link can view (reader)
  try {
    await drive.permissions.create({
      fileId: created.data.id,
      requestBody: {
        role: "reader",
        type: "anyone",
      },
      supportsAllDrives: true,
    });
    console.log(`Shared newly created folder "${folderName}" (${created.data.id}) to anyone with the link`);
  } catch (err) {
    console.error(`Failed to share folder "${folderName}" (${created.data.id}):`, err.message);
    captureError("drive-share", err);
  }

  folderCache.set(cacheKey, created.data.id);
  return created.data.id;
}

async function findExistingUpload(messageId) {
  const escapedMessageId = messageId.replace(/'/g, "\\'");
  const result = await drive.files.list({
    q: `appProperties has { key='lineMessageId' and value='${escapedMessageId}' } and trashed = false`,
    fields: "files(id,name)",
    pageSize: 1,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });

  return result.data.files && result.data.files[0];
}

function getActiveOrganization(source) {
  const sourceKey = getSourceKey(source);
  const organization = activeFolders.get(sourceKey);
  return organization || null;
}

function getSourceKey(source = {}) {
  return `${source.type || "unknown"}:${getSourceId(source)}`;
}

function getSourceId(source = {}) {
  return source.groupId || source.roomId || source.userId || "unknown-source";
}

async function start() {
  await initializeDatabase();
  await loadActiveFolders();

  if (dbPool) {
    setInterval(() => {
      checkAppointmentReminders().catch((error) => {
        console.error("Failed to check appointment reminders", error);
      });
    }, reminderCheckIntervalMs);

    checkAppointmentReminders().catch((error) => {
      console.error("Failed to check appointment reminders", error);
    });
  }

  app.listen(PORT, () => {
    console.log(`PNP BOT listening on port ${PORT}`);
  });
}

start().catch((error) => {
  console.error("Failed to start server", error);
  process.exit(1);
});
