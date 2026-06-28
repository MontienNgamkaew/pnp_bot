# PNP BOT

LINE bot สำหรับเก็บไฟล์ที่สมาชิกส่งในกลุ่ม LINE ลง Google Drive อัตโนมัติ โดยแยกเป็นโฟลเดอร์ `Images`, `Videos`, และ `Documents`

## สิ่งที่รองรับ

- รูปภาพจากข้อความ LINE ประเภท `image`
- วิดีโอจากข้อความ LINE ประเภท `video`
- เอกสาร/ไฟล์แนบจากข้อความ LINE ประเภท `file`
- ตรวจลายเซ็น webhook ของ LINE
- กันอัปโหลดซ้ำด้วย `lineMessageId` ใน Google Drive `appProperties`
- คำสั่งจัดหมวดไฟล์ เช่น `/folder งานประชุม`
- สรุปผลการอัปโหลดเป็นชุดในกลุ่ม พร้อมลิงก์โฟลเดอร์ Google Drive
- คำสั่งนัดหมายในกลุ่ม พร้อมแจ้งเตือนเช้าวันนัดหมายและก่อนเวลานัด 10 นาที

## คำสั่งบอต

ส่งคำสั่งในกลุ่ม LINE ที่มีบอตอยู่:

```text
/folder งานประชุม
```

หลังจากตั้งคำสั่งนี้ ไฟล์ถัดไปจะถูกเก็บเป็นโฟลเดอร์ย่อยตามประเภทไฟล์:

```text
LINE Group Files/
  Images/
    งานประชุม/
  Videos/
    งานประชุม/
  Documents/
    งานประชุม/
```

คำสั่งอื่น:

```text
/folder       ดูหมวดปัจจุบัน
/folder off   ปิดหมวดปัจจุบัน
/help         ดูคำสั่งทั้งหมด
```

ถ้าไม่ใช้คำสั่ง `/folder` ไฟล์จะถูกเก็บในโฟลเดอร์ปกติตามประเภทไฟล์ ถ้าตั้งหมวดใหม่และยังไม่มีโฟลเดอร์นั้น ระบบจะสร้างโฟลเดอร์ให้อัตโนมัติ

หลังอัปโหลดสำเร็จ บอตจะรอช่วงสั้น ๆ แล้วสรุปผลเป็นข้อความเดียว เช่น:

```text
ทำการบันทึกไฟล์เรียบร้อย 10 ไฟล์

สรุป:
Images/งานประชุม: 6 ไฟล์
Videos/งานประชุม: 2 ไฟล์
Documents/งานประชุม: 2 ไฟล์

โฟลเดอร์:
Images/งานประชุม: https://drive.google.com/drive/folders/...
Videos/งานประชุม: https://drive.google.com/drive/folders/...
Documents/งานประชุม: https://drive.google.com/drive/folders/...
```

ค่าเริ่มต้นจะรอ 45 วินาทีก่อนส่งสรุป ปรับได้ด้วย `SUMMARY_DELAY_SECONDS`

ถ้าต้องการให้สมาชิกในกลุ่มเปิดลิงก์ได้ ต้องแชร์โฟลเดอร์ Google Drive ให้สมาชิกเหล่านั้น หรือปรับสิทธิ์เป็น anyone with the link ตามนโยบายความปลอดภัยของคุณ

## คำสั่งนัดหมาย

ต้องตั้งค่า MySQL ก่อนใช้งานคำสั่งนัดหมาย:

```env
DB_HOST=
DB_PORT=3306
DB_USER=
DB_PASSWORD=
DB_NAME=
```

สร้างนัดหมาย:

```text
/นัด ประชุมโครงการ | 2026-07-05 14:00 | ห้องประชุม A
```

ดูนัดหมายที่กำลังจะมาถึง:

```text
/นัดหมาย
```

ยกเลิกนัดหมาย:

```text
/ยกเลิกนัด A-1234
```

บอตจะแจ้งเตือน:

```text
เช้าวันนัดหมาย เวลา 08:00
ก่อนเวลานัดหมาย 10 นาที
```

ปรับเวลาแจ้งเตือนตอนเช้าได้ด้วย:

```env
REMINDER_MORNING_HOUR=8
REMINDER_CHECK_INTERVAL_SECONDS=60
```

หมายเหตุ:

- เวลาที่พิมพ์ในคำสั่งใช้รูปแบบ `YYYY-MM-DD HH:mm` และอ้างอิงเวลาไทย `Asia/Bangkok`
- ถ้ายังไม่ตั้งค่า DB บอตยังเก็บไฟล์ได้ตามปกติ แต่คำสั่งนัดหมายจะตอบว่า “ยังไม่ได้เปิดใช้งานระบบนัดหมาย”
- บอตใช้ LINE push message สำหรับการแจ้งเตือน จึงนับรวมกับ quota/แพ็กเกจข้อความของ LINE Official Account
- ถ้า Hostinger restart หรือ redeploy นัดหมายจะไม่หาย เพราะเก็บไว้ใน MySQL

## ติดตั้ง

```bash
npm install
cp .env.example .env
npm run dev
```

## ตั้งค่า LINE

1. สร้าง Messaging API channel ใน LINE Developers
2. เปิดใช้ webhook
3. นำค่า `Channel secret` ใส่ `LINE_CHANNEL_SECRET`
4. สร้าง long-lived channel access token แล้วใส่ `LINE_CHANNEL_ACCESS_TOKEN`
5. ตั้ง webhook URL เป็น:

```text
https://your-domain.example.com/webhook
```

## ตั้งค่า Google Drive

1. สร้าง Google Cloud project
2. เปิดใช้ Google Drive API
3. สร้าง service account และดาวน์โหลด JSON key
4. สร้างโฟลเดอร์หลักใน Google Drive
5. แชร์โฟลเดอร์หลักให้ email ของ service account เป็น Editor
6. ใส่ folder ID ของโฟลเดอร์หลักใน `GOOGLE_DRIVE_ROOT_FOLDER_ID`
7. ใส่ credential อย่างใดอย่างหนึ่ง:

```env
GOOGLE_SERVICE_ACCOUNT_JSON={"type":"service_account","project_id":"..."}
```

หรือ

```env
GOOGLE_SERVICE_ACCOUNT_BASE64=base64_encoded_service_account_json
```

หรือ

```env
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service-account.json
```

ถ้ามีโฟลเดอร์ย่อยอยู่แล้ว สามารถกำหนดเองได้ด้วย:

```env
GOOGLE_DRIVE_IMAGE_FOLDER_ID=
GOOGLE_DRIVE_VIDEO_FOLDER_ID=
GOOGLE_DRIVE_DOCUMENT_FOLDER_ID=
```

## การรันจริง

เซิร์ฟเวอร์ต้องเข้าถึงจากอินเทอร์เน็ตได้และใช้ HTTPS เพราะ LINE webhook ต้องยิงเข้ามาจากภายนอก ถ้าทดสอบ local ให้ใช้ tunnel เช่น ngrok หรือ cloudflared แล้วตั้ง URL ที่ได้ใน LINE Developers

```bash
npm start
```
