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
const folderCreationPromises = new Map();
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
    q: `'${folderId}' in parents and trashed = false`,
    fields: "files(id,name,webViewLink,md5Checksum)",
    pageSize: 100,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });

  const driveDuplicate = duplicateCheck.data.files && duplicateCheck.data.files.find((f) => f.md5Checksum === md5Hex);
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
  const text = event.message.text || "";
  const botPrefixRegex = /^(?:บอต|บอท|@?bot(?![a-zA-Z]))(?:\s+|,|:)*(.*)$/i;
  const botPrefixMatch = text.trim().match(botPrefixRegex);

  if (botPrefixMatch) {
    const queryText = botPrefixMatch[1].trim();
    if (!queryText) {
      await replyLineMessage(
        event.replyToken,
        [
          "🤖 สวัสดีค่ะ! มีอะไรให้บอตช่วยเหลือไหมคะ?",
          "─────────────────",
          "• *นัดหมายใหม่*: 'บอต นัดประชุมวันพรุ่งนี้ 10 โมง'",
          "• *เลื่อนนัดหมาย*: 'บอต เลื่อนนัดประชุมโครงการเป็นบ่ายสาม'",
          "• *ยกเลิกนัดหมาย*: 'บอต ยกเลิกนัดเย็นนี้'",
          "• *ดูนัดหมาย*: 'บอต วันนี้มีประชุมอะไรบ้าง'",
          "• *คุยทั่วไป*: ถามข้อสงสัยหรือพูดคุยทั่วไปกับบอตได้เลยค่ะ"
        ].join("\n")
      );
      return;
    }
    await handleNaturalLanguageCommand(event, queryText);
    return;
  }

  const command = parseCommand(text);
  if (!command) {
    return;
  }

  if (command.type.startsWith("appointment.")) {
    await handleAppointmentCommand(event, command);
    return;
  }

  if (command.type === "summarize") {
    await handleSummarizeCommand(event, command);
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

  if (command.type === "appointment.create_natural") {
    await createAppointmentNatural(event, command.text);
    return;
  }

  if (command.type === "appointment.reschedule_natural") {
    await handleNaturalLanguageCommand(event, command.text, "update");
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

async function createAppointmentNatural(event, text) {
  // Delegate to the unified natural language command handler
  await handleNaturalLanguageCommand(event, text);
}

async function handleNaturalLanguageCommand(event, text, forcedIntent = null) {
  if (!dbPool) {
    await replyLineMessage(
      event.replyToken,
      "ยังไม่ได้เปิดใช้งานระบบนัดหมาย กรุณาตั้งค่า DB_HOST, DB_USER, DB_PASSWORD และ DB_NAME บน Hostinger"
    );
    return;
  }

  if (!process.env.GEMINI_API_KEY) {
    await replyLineMessage(
      event.replyToken,
      "❌ ยังไม่ได้เปิดใช้งานระบบ AI (กรุณาตั้งค่า GEMINI_API_KEY ในสภาพแวดล้อมของเซิร์ฟเวอร์)"
    );
    return;
  }

  // 1. Fetch active appointments for the source
  const [activeAppointments] = await dbPool.execute(
    `
      SELECT appointment_code, title, details, appointment_at
      FROM appointments
      WHERE source_type = :sourceType
        AND source_id = :sourceId
        AND status = 'active'
        AND appointment_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 DAY)
      ORDER BY appointment_at ASC
      LIMIT 30
    `,
    {
      sourceType: event.source && event.source.type ? event.source.type : "unknown",
      sourceId: getSourceId(event.source),
    }
  );

  // 2. Format the active appointments list for Gemini context
  const formattedAppts = activeAppointments.map((appt) => {
    return `- Code: ${appt.appointment_code}
  Title: "${appt.title}"
  Time: ${formatThaiDateTime(appt.appointment_at)} (Christian Era Year: ${getBangkokDateParts(appt.appointment_at).year})
  Details: "${appt.details || ''}"`;
  }).join("\n");

  const now = new Date();
  const formattedNow = formatThaiDateTime(now);
  const bangkokParts = getBangkokDateParts(now);
  const nowADString = `${bangkokParts.year}-${String(bangkokParts.month).padStart(2, "0")}-${String(bangkokParts.day).padStart(2, "0")} ${String(bangkokParts.hour).padStart(2, "0")}:${String(bangkokParts.minute).padStart(2, "0")}`;

  let forcedIntentInstruction = "";
  if (forcedIntent === "update") {
    forcedIntentInstruction = `\nCRITICAL: The user has explicitly requested to UPDATE/RESCHEDULE an appointment. You MUST select "update" as the action (or "error" if no matching appointment is found or if it is completely ambiguous). Do NOT select "create", "cancel", "list", or "general".\n`;
  }

  // 3. Build prompt for Gemini to classify intent and extract details
  const prompt = `
You are an AI assistant for a LINE group chat bot. Your job is to analyze the user's message and determine their intent regarding appointments or general chatting, and return a structured JSON response.
${forcedIntentInstruction}
Current Time (Bangkok Time):
- Buddhist Era (พ.ศ.): ${formattedNow}
- Christian Era (ค.ศ. / A.D.): ${nowADString}

Active Upcoming/Recent Appointments in this group:
${formattedAppts || "(No active appointments found)"}

User Input: "${text}"

Analyze the User Input and match it to one of the following actions:

1. "create": The user wants to schedule or create a NEW appointment.
   - You must extract "title", "dateTime" (format: "YYYY-MM-DD HH:mm", Bangkok Time, Christian Era), and "details" (if any).
   - Carefully resolve relative dates like "พรุ่งนี้" (tomorrow), "วันจันทร์หน้า" (next Monday), etc. relative to the Current Time.
   - For year conversion, subtract 543 if Buddhist Era (พ.ศ.) is implied (e.g. 2569 -> 2026).

2. "update": The user wants to modify, reschedule, or change an EXISTING appointment from the list above.
   - You must identify which appointment from the list they are referring to (by title match, time proximity, or code).
   - Extract the "appointmentCode" (e.g. "A-1234") of the matched appointment.
   - Extract the changes under "changes". Only specify fields that are being changed:
     * "title": New title (if changed)
     * "dateTime": New date/time in "YYYY-MM-DD HH:mm" format (Bangkok Time, Christian Era) (if changed)
     * "details": New details (if changed)

3. "cancel": The user wants to cancel or delete an EXISTING appointment.
   - Identify the matched appointment from the list.
   - Extract the "appointmentCode" (e.g. "A-1234").

4. "list": The user wants to see their scheduled appointments (e.g. "วันนี้มีประชุมอะไรบ้าง", "ขอดูนัดหมายหน่อย").

5. "general": The user is asking a general question, greeting, or talking about something else not related to creating/updating/canceling appointments.
   - Provide a friendly, helpful response in Thai under "generalResponse". Use information from the appointments list if relevant (e.g., if they ask about their appointments, or say hello). Keep it concise (max 3-4 sentences) and polite.

If you are unsure or if the request is ambiguous (e.g., the user wants to update/cancel but there are multiple matching appointments or no appointments at all), set "action" to "error" and explain the issue in "explanation" in Thai (e.g. "ไม่พบนัดหมายที่จะเลื่อนค่ะ" or "มีนัดหมายที่คล้ายกันหลายรายการ กรุณาระบุรหัสการนัดหมาย เช่น A-1234").

Return ONLY a JSON object matching this schema. Do not output markdown block wrappers (like \`\`\`json).
`;

  try {
    const generationConfig = {
      responseMimeType: "application/json",
      responseSchema: {
        type: "OBJECT",
        properties: {
          action: { 
            type: "STRING", 
            enum: ["create", "update", "cancel", "list", "general", "error"] 
          },
          appointmentCode: { 
            type: "STRING", 
            description: "The code of the matched appointment (required for update/cancel)" 
          },
          changes: {
            type: "OBJECT",
            properties: {
              title: { type: "STRING", description: "New title if changed" },
              dateTime: { type: "STRING", description: "New date time in YYYY-MM-DD HH:mm format if changed" },
              details: { type: "STRING", description: "New details if changed" }
            }
          },
          createData: {
            type: "OBJECT",
            properties: {
              title: { type: "STRING", description: "Title of new appointment" },
              dateTime: { type: "STRING", description: "Date time of new appointment in YYYY-MM-DD HH:mm format" },
              details: { type: "STRING", description: "Details of new appointment" }
            }
          },
          generalResponse: { 
            type: "STRING", 
            description: "Response message for general chat or greetings" 
          },
          explanation: { 
            type: "STRING", 
            description: "Explanation for errors or ambiguity in Thai" 
          }
        },
        required: ["action"]
      }
    };

    const responseText = await callGeminiGenerateContent([{ text: prompt }], generationConfig);
    const result = JSON.parse(responseText);

    if (result.action === "error") {
      await replyLineMessage(event.replyToken, `❌ ${result.explanation || "เกิดข้อผิดพลาดในการวิเคราะห์คำสั่ง"}`);
      return;
    }

    if (result.action === "general") {
      await replyLineMessage(event.replyToken, result.generalResponse || "สวัสดีค่ะ มีอะไรให้ช่วยไหมคะ");
      return;
    }

    if (result.action === "list") {
      await listAppointments(event);
      return;
    }

    if (result.action === "cancel") {
      if (!result.appointmentCode) {
        await replyLineMessage(event.replyToken, "❌ ไม่พบรหัสการนัดหมายที่จะยกเลิก");
        return;
      }
      await cancelAppointment(event, result.appointmentCode.toUpperCase());
      return;
    }

    if (result.action === "create") {
      const data = result.createData;
      if (!data || !data.title || !data.dateTime) {
        await replyLineMessage(event.replyToken, "❌ ข้อมูลสำหรับการสร้างนัดหมายไม่ครบถ้วน (ต้องการหัวข้อและวันเวลา)");
        return;
      }
      
      const appointmentAt = parseBangkokDateTime(data.dateTime);
      if (!appointmentAt) {
        await replyLineMessage(event.replyToken, `❌ รูปแบบวันเวลาไม่ถูกต้อง: ${data.dateTime}`);
        return;
      }

      if (appointmentAt.getTime() <= Date.now()) {
        await replyLineMessage(
          event.replyToken,
          `❌ เวลานัดหมายต้องเป็นเวลาในอนาคต\n(วิเคราะห์เป็น: ${formatThaiDateTime(appointmentAt)})`
        );
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
          title: data.title,
          details: data.details || null,
          appointmentAt: toMysqlDateTime(appointmentAt),
        }
      );

      await replyLineMessage(
        event.replyToken,
        [
          "🤖 บันทึกนัดหมายสำเร็จด้วย AI",
          "─────────────────",
          `📌 ${data.title}`,
          `🕐 ${formatThaiDateTime(appointmentAt)}`,
          `🔖 รหัส: ${code}`,
          data.details ? `📝 ${data.details}` : "",
        ]
          .filter(Boolean)
          .join("\n")
      );
      return;
    }

    if (result.action === "update") {
      const code = result.appointmentCode;
      const changes = result.changes;

      if (!code) {
        await replyLineMessage(event.replyToken, "❌ ไม่พบรหัสการนัดหมายที่จะเลื่อน");
        return;
      }

      if (!changes || Object.keys(changes).length === 0) {
        await replyLineMessage(event.replyToken, "❌ ไม่พบข้อมูลการเปลี่ยนแปลงที่ระบุ");
        return;
      }

      // Fetch the current appointment details first to log the before-and-after change clearly
      const [rows] = await dbPool.execute(
        "SELECT title, details, appointment_at FROM appointments WHERE appointment_code = :code AND source_id = :sourceId AND status = 'active' LIMIT 1",
        { code: code.toUpperCase(), sourceId: getSourceId(event.source) }
      );

      if (!rows.length) {
        await replyLineMessage(event.replyToken, `❌ ไม่พบนัดหมายรหัส ${code.toUpperCase()} ในกลุ่มนี้`);
        return;
      }

      const current = rows[0];
      let newTitle = current.title;
      let newDetails = current.details;
      let newTime = current.appointment_at;
      let timeChanged = false;

      const updateFields = [];
      const updateParams = { code: code.toUpperCase(), sourceId: getSourceId(event.source) };

      if (changes.title) {
        newTitle = sanitizePlainText(changes.title, 180);
        updateFields.push("title = :newTitle");
        updateParams.newTitle = newTitle;
      }

      if (changes.details) {
        newDetails = sanitizePlainText(changes.details, 800);
        updateFields.push("details = :newDetails");
        updateParams.newDetails = newDetails;
      }

      if (changes.dateTime) {
        const parsedTime = parseBangkokDateTime(changes.dateTime);
        if (!parsedTime) {
          await replyLineMessage(event.replyToken, `❌ รูปแบบเวลาใหม่ไม่ถูกต้อง: ${changes.dateTime}`);
          return;
        }
        if (parsedTime.getTime() <= Date.now()) {
          await replyLineMessage(
            event.replyToken,
            `❌ เวลาที่จะเลื่อนไปต้องเป็นเวลาในอนาคต\n(วิเคราะห์เป็น: ${formatThaiDateTime(parsedTime)})`
          );
          return;
        }
        newTime = parsedTime;
        timeChanged = true;
        updateFields.push("appointment_at = :newTime");
        updateFields.push("morning_reminder_sent = 0");
        updateFields.push("ten_minute_reminder_sent = 0");
        updateParams.newTime = toMysqlDateTime(parsedTime);
      }

      if (updateFields.length === 0) {
        await replyLineMessage(event.replyToken, "ℹ️ ไม่มีฟิลด์ใดที่เปลี่ยนแปลง");
        return;
      }

      const query = `
        UPDATE appointments
        SET ${updateFields.join(", ")}
        WHERE appointment_code = :code
          AND source_id = :sourceId
          AND status = 'active'
      `;

      await dbPool.execute(query, updateParams);

      const summaryText = [
        "🔄 เลื่อนนัดหมายสำเร็จด้วย AI",
        "─────────────────",
        `📌 ${newTitle}`,
        timeChanged 
          ? `🕐 ${formatThaiDateTime(current.appointment_at)} ➡️ ${formatThaiDateTime(newTime)}`
          : `🕐 ${formatThaiDateTime(newTime)}`,
        `🔖 รหัส: ${code.toUpperCase()}`,
        newDetails ? `📝 ${newDetails}` : "",
      ]
        .filter(Boolean)
        .join("\n");

      await replyLineMessage(event.replyToken, summaryText);
      return;
    }

  } catch (error) {
    console.error("Natural language processing failed:", error);
    captureError("nlp-appointment", error);
    await replyLineMessage(
      event.replyToken,
      `❌ ระบบประมวลผลคำสั่งผิดพลาด:\n${error.message || error}`
    );
  }
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
    "/เลื่อนนัด ข้อความภาษาธรรมชาติ — เลื่อนเวลานัดหมาย",
    "/ยกเลิกนัด รหัส",
    "",
    "💡 ตัวอย่าง",
    "/นัด อบรมต่อต้านยาเสพติด 30/06/2569 13.30 ห้องประชุมเอราวรรณ",
    "/เลื่อนนัด ประชุมโครงการเอเป็นพรุ่งนี้สิบโมงเช้า",
  ].join("\n");
}

async function handleSummarizeCommand(event, command) {
  if (!process.env.GEMINI_API_KEY) {
    await replyLineMessage(
      event.replyToken,
      "❌ ยังไม่ได้เปิดใช้งานระบบสรุปเอกสารด้วย AI (กรุณาตั้งค่า GEMINI_API_KEY ในสภาพแวดล้อมของเซิร์ฟเวอร์)"
    );
    return;
  }

  const quotedMessageId = event.message.quotedMessageId;
  
  if (quotedMessageId) {
    // ----------------------------------------------------
    // CASE A: User replied to a single file/image
    // ----------------------------------------------------
    try {
      let content;
      try {
        content = await fetchLineContent(quotedMessageId);
      } catch (err) {
        await replyLineMessage(
          event.replyToken,
          "❌ ไม่สามารถดึงไฟล์จากข้อความที่ตอบกลับได้ (อาจไม่ใช่รูปภาพหรือไฟล์เอกสาร หรือไฟล์หมดอายุแล้ว)"
        );
        return;
      }

      const mimeType = content.contentType;
      const isImage = mimeType.startsWith("image/");
      const isPdf = mimeType === "application/pdf";
      const isText = mimeType.startsWith("text/");

      if (!isImage && !isPdf && !isText) {
        await replyLineMessage(
          event.replyToken,
          `❌ ไม่รองรับไฟล์ประเภทนี้ในการสรุปด้วย AI\n(รองรับเฉพาะ รูปภาพ และ PDF/Text เท่านั้น)`
        );
        return;
      }

      const chunks = [];
      for await (const chunk of content.stream) {
        chunks.push(chunk);
      }
      const buffer = Buffer.concat(chunks);

      let summary = "";

      if (isText) {
        const textContent = buffer.toString("utf-8");
        summary = await generateTextSummary(textContent, command.customInstruction);
      } else {
        summary = await generateMultimodalSummary([{ buffer, mimeType }], command.customInstruction);
      }

      await replyLineMessage(
        event.replyToken,
        [
          "🤖 สรุปเนื้อหาเอกสารด้วย AI",
          "─────────────────",
          summary
        ].join("\n")
      );

    } catch (error) {
      console.error("Summarization error:", error);
      captureError("summarize", error);
      await replyLineMessage(
        event.replyToken,
        `❌ เกิดข้อผิดพลาดในการสรุปเอกสาร: ${error.message}`
      );
    }
  } else {
    // ----------------------------------------------------
    // CASE B: User typed /สรุป (without replying to a message)
    //         -> Summarize the last N files in Google Drive active folders
    // ----------------------------------------------------
    try {
      const organization = getActiveOrganization(event.source);
      const orgName = organization ? organization.folderName : null;
      
      const imageFolderId = await getFolderId("image", orgName);
      const documentFolderId = await getFolderId("document", orgName);

      // List the most recent files from both folders
      const imagesList = await drive.files.list({
        q: `'${imageFolderId}' in parents and trashed = false and (mimeType = 'image/jpeg' or mimeType = 'image/png' or mimeType = 'image/webp' or mimeType = 'application/pdf')`,
        fields: "files(id, name, mimeType, createdTime)",
        orderBy: "createdTime desc",
        pageSize: 5,
        supportsAllDrives: true,
        includeItemsFromAllDrives: true,
      });

      const docsList = await drive.files.list({
        q: `'${documentFolderId}' in parents and trashed = false and (mimeType = 'application/pdf' or mimeType = 'text/plain')`,
        fields: "files(id, name, mimeType, createdTime)",
        orderBy: "createdTime desc",
        pageSize: 5,
        supportsAllDrives: true,
        includeItemsFromAllDrives: true,
      });

      // Combine and sort by createdTime desc
      const allFiles = [
        ...(imagesList.data.files || []),
        ...(docsList.data.files || [])
      ].sort((a, b) => new Date(b.createdTime) - new Date(a.createdTime));

      if (allFiles.length === 0) {
        await replyLineMessage(
          event.replyToken,
          [
            "💡 ไม่พบเอกสารหรือรูปภาพล่าสุดในโฟลเดอร์สำหรับสรุปความ",
            "คุณสามารถใช้งานโดยการกดตอบกลับ (Reply) รูปภาพ หรือ PDF ในห้องแชท แล้วพิมพ์ `/สรุป` ค่ะ"
          ].join("\n")
        );
        return;
      }

      // Filter files uploaded in the last 15 minutes (900 seconds)
      const fifteenMinutesAgo = new Date(Date.now() - 15 * 60 * 1000);
      const recentFiles = allFiles.filter(file => new Date(file.createdTime) >= fifteenMinutesAgo).slice(0, 5);

      if (recentFiles.length === 0) {
        await replyLineMessage(
          event.replyToken,
          [
            "💡 ไม่พบไฟล์เอกสารหรือรูปภาพที่ถูกส่งเข้ามาใหม่ในช่วง 15 นาทีที่ผ่านมา",
            "หากต้องการสรุปไฟล์เก่า กรุณากดตอบกลับ (Reply) ไฟล์นั้นแล้วพิมพ์คำสั่ง `/สรุป` ค่ะ"
          ].join("\n")
        );
        return;
      }

      // Download each file content from Google Drive
      const filePayloads = [];
      let textContents = [];

      for (const file of recentFiles) {
        try {
          const res = await drive.files.get(
            { fileId: file.id, alt: "media" },
            { responseType: "arraybuffer" }
          );
          const buffer = Buffer.from(res.data);
          
          if (file.mimeType.startsWith("text/")) {
            textContents.push({ name: file.name, text: buffer.toString("utf-8") });
          } else {
            filePayloads.push({ buffer, mimeType: file.mimeType });
          }
        } catch (downloadErr) {
          console.error(`Failed to download file ${file.name} (${file.id}):`, downloadErr);
        }
      }

      if (filePayloads.length === 0 && textContents.length === 0) {
        await replyLineMessage(
          event.replyToken,
          "❌ เกิดข้อผิดพลาดในการดึงข้อมูลไฟล์ล่าสุดจาก Google Drive"
        );
        return;
      }

      let summary = "";
      let combinedText = textContents.map(tc => `[ไฟล์: ${tc.name}]\n${tc.text}`).join("\n\n");
      
      if (filePayloads.length > 0) {
        summary = await generateMultimodalSummary(filePayloads, command.customInstruction, combinedText);
      } else {
        summary = await generateTextSummary(combinedText, command.customInstruction);
      }

      const fileNamesList = recentFiles.map(f => `• ${f.name}`).join("\n");
      await replyLineMessage(
        event.replyToken,
        [
          "🤖 สรุปรวมเอกสารล่าสุดด้วย AI",
          `📂 เอกสารที่ประมวลผล (${recentFiles.length} ไฟล์):`,
          fileNamesList,
          "─────────────────",
          summary
        ].join("\n")
      );

    } catch (error) {
      console.error("Multi-file summarization error:", error);
      captureError("summarize-multi", error);
      await replyLineMessage(
        event.replyToken,
        `❌ เกิดข้อผิดพลาดในการสรุปรวมเอกสาร: ${error.message}`
      );
    }
  }
}

async function generateTextSummary(text, customInstruction) {
  const instruction = customInstruction 
    ? `เพิ่มเติม: ${customInstruction}`
    : "สรุปประเด็นหลักและจุดประสงค์ของเอกสารนี้เป็นหัวข้อสั้น ๆ กระชับ เข้าใจง่าย ความยาวประมาณ 5-10 บรรทัด หากมีมติที่ประชุม, กำหนดวันส่งงาน, หรือ Action Items ที่สำคัญ ให้เน้นย้ำไว้ท้ายสรุป";

  const prompt = `
คุณคือผู้ช่วยสรุปรายงานการประชุมและเอกสารขององค์กร
ข้อความต่อไปนี้เป็นข้อความที่สกัดมาจากเอกสารที่ผู้ใช้ส่งเข้ามาในกลุ่ม LINE

ภารกิจของคุณ:
1. สรุปเนื้อหาต่อไปนี้เป็น "ภาษาไทย"
2. ${instruction}

ข้อความจากเอกสาร:
"""
${text.substring(0, 50000)}
"""
`;

  return callGeminiGenerateContent([{ text: prompt }]);
}

async function generateMultimodalSummary(files, customInstruction, additionalText = "") {
  const instruction = customInstruction 
    ? `เพิ่มเติม: ${customInstruction}`
    : "สรุปประเด็นหลักและจุดประสงค์ของเอกสารนี้เป็นหัวข้อสั้น ๆ กระชับ เข้าใจง่าย ความยาวประมาณ 5-10 บรรทัด หากมีมติที่ประชุม, กำหนดวันส่งงาน, หรือ Action Items ที่สำคัญ ให้เน้นย้ำไว้ท้ายสรุป";

  let prompt = `
คุณคือผู้ช่วยสรุปรายงานการประชุมและเอกสารขององค์กร
ข้อความหรือภาพต่อไปนี้เป็นเอกสารหรือรูปภาพที่ผู้ใช้ส่งเข้ามาในกลุ่ม LINE

ภารกิจของคุณ:
1. อ่านข้อความที่ปรากฏในรูปภาพหรือเอกสาร PDF สแกนทั้งหมดที่แนบมานี้
2. สรุปเนื้อหาเป็น "ภาษาไทย" โดยวิเคราะห์และเชื่อมโยงข้อมูลจากทุกรูปภาพ/เอกสารรวมเข้าด้วยกันเป็นเรื่องเดียวอย่างเป็นระบบ
3. ${instruction}
`;

  if (additionalText) {
    prompt += `\n\nเนื้อหาเพิ่มเติมจากเอกสารประเภทข้อความ:\n"""\n${additionalText.substring(0, 30000)}\n"""`;
  }

  const parts = [
    { text: prompt },
    ...files.map(f => ({
      inlineData: {
        mimeType: f.mimeType,
        data: f.buffer.toString("base64")
      }
    }))
  ];

  return callGeminiGenerateContent(parts);
}

async function callGeminiGenerateContent(parts, generationConfig = null) {
  const models = ["gemini-2.5-flash", "gemini-1.5-flash"];
  let lastError = null;

  for (const model of models) {
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        const body = {
          contents: [{ parts }],
        };
        if (generationConfig) {
          body.generationConfig = generationConfig;
        }

        const response = await fetch(
          `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${process.env.GEMINI_API_KEY}`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify(body),
          }
        );

        if (response.status === 503 || response.status === 429) {
          const delay = attempt * 1500;
          console.warn(`Gemini API returned ${response.status} for ${model}. Retrying in ${delay}ms...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
          continue;
        }

        if (!response.ok) {
          const errText = await response.text();
          throw new Error(`Gemini API returned status ${response.status}: ${errText}`);
        }

        const resJson = await response.json();
        const responseText = resJson.candidates?.[0]?.content?.parts?.[0]?.text;
        if (!responseText) {
          throw new Error("Invalid response structure from Gemini API");
        }

        return responseText.trim();
      } catch (err) {
        console.error(`Attempt ${attempt} for model ${model} failed:`, err.message);
        lastError = err;
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
    }
  }

  throw lastError || new Error("Failed to call Gemini API");
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

  const summarizeMatch = trimmed.match(/^\/(?:สรุป|summarize|summary)(?:\s+(.+))?$/i);
  if (summarizeMatch) {
    return { 
      type: "summarize", 
      customInstruction: summarizeMatch[1] ? summarizeMatch[1].trim() : null 
    };
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

  const rescheduleMatch = text.match(/^\/(?:เลื่อนนัด|reschedule)\s+(.+)$/i);
  if (rescheduleMatch) {
    return { type: "appointment.reschedule_natural", text: rescheduleMatch[1].trim() };
  }

  const createMatch = text.match(/^\/(?:นัด|appointment)\s+(.+)$/i);
  if (!createMatch) {
    return null;
  }

  const body = createMatch[1].trim();
  // Format: /นัด [title] DD/MM/YYYY HH.mm [details]
  const dateTimeMatch = body.match(/^(.+?)\s+(\d{1,2}\/\d{1,2}\/\d{4})\s+(\d{1,2}[.:]\d{2})\s*(.*)$/);
  if (!dateTimeMatch) {
    return { type: "appointment.create_natural", text: body };
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
    "/เลื่อนนัด ข้อความ — เลื่อนนัดด้วยภาษาธรรมชาติ",
    "/ยกเลิกนัด รหัส",
    "",
    "🤖 คุยและสั่งการด้วย AI (ภาษาธรรมชาติ)",
    "พิมพ์ขึ้นต้นด้วย 'บอต' หรือ 'บอท' หรือ 'bot' เช่น:",
    "- 'บอต นัดประชุมวันพรุ่งนี้ 10 โมง'",
    "- 'บอต เลื่อนนัดประชุมโครงการเป็นบ่ายสาม'",
    "- 'บอต ยกเลิกนัดเย็นนี้'",
    "- 'บอต วันนี้มีประชุมอะไรบ้าง'",
    "- พิมพ์คุยทั่วไปหรือถามข้อสงสัยได้เลยค่ะ",
    "",
    "🤖 สรุปเอกสาร/รูปภาพด้วย AI",
    "/สรุป — สรุปรวมรูปภาพ/ไฟล์ที่ส่งเข้ามาล่าสุด (ไม่เกิน 5 ไฟล์ใน 15 นาที)",
    "*(กด 'ตอบกลับ' รูปภาพ/PDF แล้วพิมพ์ /สรุป เพื่อระบุสรุปเฉพาะไฟล์ได้)*",
    "",
    "💡 ตัวอย่าง",
    "/folder งานประชุม",
    "/เลื่อนนัด เลื่อนประชุมเป็นวันจันทร์หน้าสิบโมงเช้า",
    "กดตอบกลับที่รูปภาพ -> พิมพ์ /สรุป",
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
  let year = Number(yearText);
  if (year > 2400) {
    year -= 543; // แปลง พ.ศ. → ค.ศ.
  }
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
      LIMIT 100
    `
  );

  if (!rows.length) {
    return;
  }

  // Group appointments by source_id to consolidate multiple notifications
  const groups = new Map();
  for (const appt of rows) {
    const list = groups.get(appt.source_id) || [];
    list.push(appt);
    groups.set(appt.source_id, list);
  }

  // Process each group
  for (const [sourceId, appts] of groups.entries()) {
    let sentSuccess = false;

    if (process.env.GEMINI_API_KEY) {
      try {
        const apptsText = appts
          .map((a, i) => {
            const detailStr = a.details ? ` (รายละเอียด: ${a.details})` : "";
            return `${i + 1}. รหัส ${a.appointment_code}: "${a.title}" เวลา ${formatThaiDateTime(a.appointment_at)}${detailStr}`;
          })
          .join("\n");

        const prompt = `
คุณคือผู้ช่วยรายงานข่าวยามเช้าขององค์กร (LINE Bot Daily Assistant)
วันนี้นัดหมายทั้งหมดในระบบของกลุ่มนี้มีดังนี้:
${apptsText}

ภารกิจของคุณ:
1. เขียนข้อความทักทายยามเช้าภาษาไทยที่เป็นกันเอง สุภาพ และอบอุ่น (เช่น "สวัสดีเช้าวันพุธค่ะทุกคน...")
2. เรียบเรียงรายการนัดหมายของวันนี้ให้อ่านง่าย ชัดเจน (ระบุหัวข้อ เวลา และรายละเอียดให้ครบถ้วน)
3. สอดแทรกคำอวยพรหรือข้อคิดสั้น ๆ ที่ให้พลังบวกในการเริ่มต้นทำงานไว้ตอนท้ายข้อความ
4. คืนค่าเฉพาะข้อความทักทายและสรุปที่พร้อมส่งให้ผู้ใช้อ่านทันที ไม่ต้องเกริ่นนำหรือใส่คำสั่งใดๆ
`;

        const briefing = await callGeminiGenerateContent([{ text: prompt }]);

        await pushLineMessage(sourceId, briefing);
        sentSuccess = true;
      } catch (err) {
        console.error(`Failed to generate AI morning briefing for ${sourceId}:`, err);
        captureError("morning-briefing-ai", err);
      }
    }

    // Fallback: If AI fails or is unconfigured, send standard individual messages
    if (!sentSuccess) {
      for (const appointment of appts) {
        try {
          await pushLineMessage(sourceId, buildMorningReminderMessage(appointment));
        } catch (pushErr) {
          console.error(`Failed to send standard morning reminder for appt ${appointment.id}:`, pushErr);
        }
      }
    }

    // Mark all these appointments as sent
    for (const appointment of appts) {
      await dbPool.execute("UPDATE appointments SET morning_reminder_sent = 1 WHERE id = :id", {
        id: appointment.id,
      });
    }
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

  if (folderCreationPromises.has(cacheKey)) {
    return folderCreationPromises.get(cacheKey);
  }

  const promise = (async () => {
    try {
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
    } finally {
      folderCreationPromises.delete(cacheKey);
    }
  })();

  folderCreationPromises.set(cacheKey, promise);
  return promise;
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
