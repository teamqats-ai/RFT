import express from 'express';
import http from 'http';
import fs from 'fs';
import os from 'os';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import { createRequire } from 'module';
import { WebSocketServer, WebSocket } from 'ws';
import type { PushSubscription as WebPushSubscription } from 'web-push';
import type * as WebPushModule from 'web-push';
import { GoogleGenAI } from '@google/genai';
import { FACTORY_MASTER_DATA, type FactoryLocationEntry } from './src/data/factoryData.ts';

const require = createRequire(import.meta.url);
const webpush = require('web-push') as typeof WebPushModule;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = Number(process.env.PORT) || 3000;

app.use(express.json({ limit: '25mb' }));

// ============================================================================
// SHARED SYSTEM CONFIG STORE (Cross-Account & Cross-Device GAS /exec Sync)
// ============================================================================
const SHARED_CONFIG_FILE = '/tmp/rft_shared_config.json';
const MASTER_SHEET_ID_FOR_CONFIG = '1W0MvbqhMrjBivgWhLcsCPcPoNFJHQ2AZU1d5DOXtH5I';
const DEFAULT_PDF_ARCHIVE_FOLDER_URL = 'https://drive.google.com/drive/folders/1Z8G3LQNe77MXWdEraH8vdZaahelU9_ZY';

interface SharedServerConfig {
  gasWebAppUrl: string;
  rftFolderUrl?: string;
  pdfFolderUrl?: string;
  sheetSourceUrl?: string;
  inputFileName?: string;
  inputSheetName?: string;
  outputFileName?: string;
  outputSheetName?: string;
  eodMilestone?: '16:30' | '18:00' | '20:00';
  rftTarget?: number;
  defaultChuyen?: string;
  autoSyncEnabled?: boolean;
  autoSyncIntervalSec?: number;
  qcDailyReminderEnabled?: boolean;
  qcReminderTime?: string;
  qcReminderRepeatMinutes?: number;
  qcReminderSkipSunday?: boolean;
  updatedAt?: string;
  updatedBy?: string;
}

function isValidGasExecUrl(candidate?: string): string {
  const raw = String(candidate || '').trim();
  if (!raw) return '';
  try {
    const parsed = new URL(raw);
    if (
      ['script.google.com', 'script.googleusercontent.com'].includes(parsed.hostname) &&
      (raw.includes('script.google.com/macros/s/') || raw.includes('script.googleusercontent.com/macros/'))
    ) {
      return raw;
    }
  } catch {
    // ignore invalid URL
  }
  return '';
}

function sanitizeReminderTimeHHmm(raw?: string): string {
  const m = String(raw || '').trim().match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
  if (!m) return '17:00';
  return `${m[1].padStart(2, '0')}:${m[2]}`;
}

function loadSharedServerConfigFromDisk(): SharedServerConfig {
  try {
    if (fs.existsSync(SHARED_CONFIG_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(SHARED_CONFIG_FILE, 'utf8'));
      if (parsed && typeof parsed === 'object') {
        return {
          pdfFolderUrl: DEFAULT_PDF_ARCHIVE_FOLDER_URL,
          qcDailyReminderEnabled: true,
          qcReminderTime: '17:00',
          qcReminderRepeatMinutes: 30,
          qcReminderSkipSunday: true,
          ...parsed,
          gasWebAppUrl: isValidGasExecUrl(parsed.gasWebAppUrl),
        };
      }
    }
  } catch {
    // non-blocking
  }
  return {
    gasWebAppUrl: '',
    pdfFolderUrl: DEFAULT_PDF_ARCHIVE_FOLDER_URL,
    qcDailyReminderEnabled: true,
    qcReminderTime: '17:00',
    qcReminderRepeatMinutes: 30,
    qcReminderSkipSunday: true,
  };
}

let sharedServerConfig: SharedServerConfig = loadSharedServerConfigFromDisk();
let lastSheetConfigCheckAt = 0;
let lastPersistedSheetConfigKey = '';

function buildConfigSyncKey(cfg: SharedServerConfig): string {
  return JSON.stringify({
    gasWebAppUrl: cfg.gasWebAppUrl || '',
    rftFolderUrl: cfg.rftFolderUrl || '',
    pdfFolderUrl: cfg.pdfFolderUrl || '',
    sheetSourceUrl: cfg.sheetSourceUrl || '',
    inputFileName: cfg.inputFileName || '',
    inputSheetName: cfg.inputSheetName || '',
    outputFileName: cfg.outputFileName || '',
    outputSheetName: cfg.outputSheetName || '',
    eodMilestone: cfg.eodMilestone || '16:30',
    rftTarget: cfg.rftTarget ?? 95,
  });
}

function persistSharedServerConfigToDisk(cfg: SharedServerConfig) {
  try {
    fs.writeFileSync(SHARED_CONFIG_FILE, JSON.stringify(cfg, null, 2), 'utf8');
  } catch {
    // non-blocking disk cache
  }
}

async function persistSharedConfigToMasterSheet(cfg: SharedServerConfig): Promise<boolean> {
  const gasUrl = isValidGasExecUrl(cfg.gasWebAppUrl);
  if (!gasUrl) return false;
  const key = buildConfigSyncKey(cfg);
  if (key === lastPersistedSheetConfigKey) return true;

  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 5000);
    const payload = {
      action: 'submitReport',
      outputUrl: `https://docs.google.com/spreadsheets/d/${MASTER_SHEET_ID_FOR_CONFIG}/edit`,
      outputSheetName: 'Cau_Hinh',
      headers: [
        'KEY',
        'VALUE',
        'RFT_FOLDER_URL',
        'SHEET_SOURCE_URL',
        'OUTPUT_FILE_NAME',
        'OUTPUT_SHEET_NAME',
        'EOD_MILESTONE',
        'RFT_TARGET',
        'DEFAULT_CHUYEN',
        'UPDATED_AT',
        'UPDATED_BY',
        'PDF_FOLDER_URL',
      ],
      rows: [
        {
          KEY: 'SYSTEM_CONFIG',
          VALUE: gasUrl,
          RFT_FOLDER_URL: cfg.rftFolderUrl || '',
          SHEET_SOURCE_URL: cfg.sheetSourceUrl || '',
          OUTPUT_FILE_NAME: cfg.outputFileName || '',
          OUTPUT_SHEET_NAME: cfg.outputSheetName || '',
          EOD_MILESTONE: cfg.eodMilestone || '16:30',
          RFT_TARGET: cfg.rftTarget ?? 95,
          DEFAULT_CHUYEN: cfg.defaultChuyen || '',
          UPDATED_AT: cfg.updatedAt || new Date().toISOString(),
          UPDATED_BY: cfg.updatedBy || 'Admin',
          PDF_FOLDER_URL: cfg.pdfFolderUrl || DEFAULT_PDF_ARCHIVE_FOLDER_URL,
        },
      ],
    };

    const res = await fetch(gasUrl, {
      method: 'POST',
      redirect: 'follow',
      signal: controller.signal,
      headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: JSON.stringify(payload),
    }).finally(() => clearTimeout(timer));

    const txt = await res.text().catch(() => '');
    if (res.ok && !txt.trim().startsWith('<!DOCTYPE html') && !txt.includes('"status":"error"')) {
      lastPersistedSheetConfigKey = key;
      return true;
    }
  } catch {
    // non-blocking sheet persistence failure
  }
  return false;
}

async function ensureServerConfigHydrated(forceRefresh = false): Promise<void> {
  if (!forceRefresh && (sharedServerConfig.gasWebAppUrl || isValidGasExecUrl(process.env.GAS_WEB_APP_URL))) {
    return;
  }
  const now = Date.now();
  if (!forceRefresh && now - lastSheetConfigCheckAt < 5_000) return;
  lastSheetConfigCheckAt = now;

  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 3500);
    const gvizUrl = `https://docs.google.com/spreadsheets/d/${MASTER_SHEET_ID_FOR_CONFIG}/gviz/tq?tqx=out:json&sheet=${encodeURIComponent('Cau_Hinh')}&t=${now}`;
    const res = await fetch(gvizUrl, {
      method: 'GET',
      redirect: 'follow',
      signal: controller.signal,
      headers: { Accept: 'application/json, text/plain, */*', 'User-Agent': 'RFT-Report-Proxy/1.0' },
    }).finally(() => clearTimeout(timer));
    if (!res.ok) return;
    const text = await res.text();
    if (!text.includes('google.visualization.Query.setResponse')) return;

    const rows = parseServerGvizRows(text);
    for (let i = rows.length - 1; i >= 0; i--) {
      const r = rows[i];
      const valUrl = isValidGasExecUrl(getServerRowCell(r, ['VALUE', 'GAS_URL', 'URL'], 1));
      if (!valUrl) continue;

      const folderUrl = getServerRowCell(r, ['RFT_FOLDER_URL'], 2);
      const sourceUrl = getServerRowCell(r, ['SHEET_SOURCE_URL'], 3);
      const outFile = getServerRowCell(r, ['OUTPUT_FILE_NAME'], 4);
      const outSheet = getServerRowCell(r, ['OUTPUT_SHEET_NAME'], 5);
      const milestoneRawCell = getServerRowCell(r, ['EOD_MILESTONE'], 6);
      const milestoneRaw = milestoneRawCell.includes('16,30')
        ? '16:30'
        : milestoneRawCell.includes('18,0')
        ? '18:00'
        : milestoneRawCell.includes('20,0')
        ? '20:00'
        : milestoneRawCell;
      const targetRaw = Number(getServerRowCell(r, ['RFT_TARGET'], 7));
      const defChuyen = getServerRowCell(r, ['DEFAULT_CHUYEN'], 8);
      const updAt = getServerRowCell(r, ['UPDATED_AT'], 9);
      const updBy = getServerRowCell(r, ['UPDATED_BY'], 10);
      const pdfFolderUrl = getServerRowCell(r, ['PDF_FOLDER_URL'], 11);

      sharedServerConfig = {
        ...sharedServerConfig,
        gasWebAppUrl: valUrl,
        ...(folderUrl ? { rftFolderUrl: folderUrl } : {}),
        ...(pdfFolderUrl ? { pdfFolderUrl } : {}),
        ...(sourceUrl ? { sheetSourceUrl: sourceUrl } : {}),
        ...(outFile ? { outputFileName: outFile } : {}),
        ...(outSheet ? { outputSheetName: outSheet } : {}),
        ...(milestoneRaw === '16:30' || milestoneRaw === '18:00' || milestoneRaw === '20:00'
          ? { eodMilestone: milestoneRaw }
          : {}),
        ...(!Number.isNaN(targetRaw) && targetRaw >= 50 && targetRaw <= 100
          ? { rftTarget: targetRaw }
          : {}),
        ...(defChuyen ? { defaultChuyen: defChuyen } : {}),
        updatedAt: updAt || new Date().toISOString(),
        updatedBy: updBy || 'Cau_Hinh_Sheet',
      };
      lastPersistedSheetConfigKey = buildConfigSyncKey(sharedServerConfig);
      persistSharedServerConfigToDisk(sharedServerConfig);
      return;
    }

    const allMatches = text.match(/https:\/\/script\.google\.com\/macros\/s\/[a-zA-Z0-9_-]+\/exec/g);
    const lastMatch = allMatches && allMatches.length > 0 ? allMatches[allMatches.length - 1] : '';
    const valid = isValidGasExecUrl(lastMatch);
    if (valid) {
      sharedServerConfig = {
        ...sharedServerConfig,
        gasWebAppUrl: valid,
        updatedAt: new Date().toISOString(),
        updatedBy: 'Cau_Hinh_Sheet',
      };
      lastPersistedSheetConfigKey = buildConfigSyncKey(sharedServerConfig);
      persistSharedServerConfigToDisk(sharedServerConfig);
    }
  } catch {
    // optional sheet discovery failure is harmless
  }
}

function resolveValidGasUrl(candidate?: string): string {
  return (
    isValidGasExecUrl(candidate) ||
    isValidGasExecUrl(sharedServerConfig.gasWebAppUrl) ||
    isValidGasExecUrl(process.env.GAS_WEB_APP_URL) ||
    ''
  );
}

// Health Check
app.get('/api/health', (_req, res) => {
  res.json({ status: 'ok', time: new Date().toISOString() });
});

app.get('/api/config', async (_req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  await ensureServerConfigHydrated();
  const activeGasUrl = resolveValidGasUrl('');
  if (activeGasUrl && !lastPersistedSheetConfigKey) {
    void persistSharedConfigToMasterSheet({
      ...sharedServerConfig,
      gasWebAppUrl: activeGasUrl,
    });
  }
  const hasServerGasUrl = Boolean(activeGasUrl);
  res.json({
    ok: true,
    hasServerGasUrl,
    sharedConfig: sharedServerConfig,
  });
});

app.post('/api/config', async (req, res) => {
  try {
    const body = req.body || {};
    const incomingGasRaw = typeof body.gasWebAppUrl === 'string' ? body.gasWebAppUrl.trim() : undefined;
    let nextGasUrl = sharedServerConfig.gasWebAppUrl;

    if (body.clearGasUrl === true) {
      nextGasUrl = '';
    } else if (incomingGasRaw) {
      const validGas = isValidGasExecUrl(incomingGasRaw);
      if (!validGas) {
        return res.status(400).json({
          ok: false,
          error: 'URL Google Apps Script Web App (/exec) không hợp lệ.',
        });
      }
      nextGasUrl = validGas;
    }

    const nextConfig: SharedServerConfig = {
      ...sharedServerConfig,
      gasWebAppUrl: nextGasUrl,
      ...(typeof body.rftFolderUrl === 'string' && body.rftFolderUrl.trim()
        ? { rftFolderUrl: body.rftFolderUrl.trim() }
        : {}),
      ...(typeof body.pdfFolderUrl === 'string' && body.pdfFolderUrl.trim()
        ? { pdfFolderUrl: body.pdfFolderUrl.trim() }
        : {}),
      ...(typeof body.sheetSourceUrl === 'string' && body.sheetSourceUrl.trim()
        ? { sheetSourceUrl: body.sheetSourceUrl.trim() }
        : {}),
      ...(typeof body.inputFileName === 'string'
        ? { inputFileName: body.inputFileName.trim() }
        : {}),
      ...(typeof body.inputSheetName === 'string'
        ? { inputSheetName: body.inputSheetName.trim() }
        : {}),
      ...(typeof body.outputFileName === 'string'
        ? { outputFileName: body.outputFileName.trim() }
        : {}),
      ...(typeof body.outputSheetName === 'string'
        ? { outputSheetName: body.outputSheetName.trim() }
        : {}),
      ...(body.eodMilestone === '16:30' || body.eodMilestone === '18:00' || body.eodMilestone === '20:00'
        ? { eodMilestone: body.eodMilestone }
        : {}),
      ...(typeof body.rftTarget === 'number' && !Number.isNaN(body.rftTarget)
        ? { rftTarget: Math.min(100, Math.max(50, Number(body.rftTarget))) }
        : {}),
      ...(typeof body.defaultChuyen === 'string' && body.defaultChuyen.trim()
        ? { defaultChuyen: body.defaultChuyen.trim() }
        : {}),
      ...(typeof body.autoSyncEnabled === 'boolean'
        ? { autoSyncEnabled: body.autoSyncEnabled }
        : {}),
      ...(typeof body.autoSyncIntervalSec === 'number' && body.autoSyncIntervalSec >= 10
        ? { autoSyncIntervalSec: Number(body.autoSyncIntervalSec) }
        : {}),
      ...(typeof body.qcDailyReminderEnabled === 'boolean'
        ? { qcDailyReminderEnabled: body.qcDailyReminderEnabled }
        : {}),
      ...(typeof body.qcReminderTime === 'string' && body.qcReminderTime.trim()
        ? { qcReminderTime: sanitizeReminderTimeHHmm(body.qcReminderTime) }
        : {}),
      ...(typeof body.qcReminderRepeatMinutes === 'number' && !Number.isNaN(body.qcReminderRepeatMinutes)
        ? { qcReminderRepeatMinutes: Math.max(0, Math.min(240, Number(body.qcReminderRepeatMinutes))) }
        : {}),
      ...(typeof body.qcReminderSkipSunday === 'boolean'
        ? { qcReminderSkipSunday: body.qcReminderSkipSunday }
        : {}),
      updatedAt: new Date().toISOString(),
      ...(typeof body.updatedBy === 'string' && body.updatedBy.trim()
        ? { updatedBy: body.updatedBy.trim() }
        : {}),
    };

    sharedServerConfig = nextConfig;
    persistSharedServerConfigToDisk(sharedServerConfig);

    // Đồng bộ cấu hình vào tab Cau_Hinh trên Google Sheet chính để mọi link/bản sao (dev & thực tế) tự nhận ngay lập tức
    const syncedToSheet = nextGasUrl ? await persistSharedConfigToMasterSheet(nextConfig) : false;

    return res.json({
      ok: true,
      hasServerGasUrl: Boolean(resolveValidGasUrl('')),
      syncedToSheet,
      sharedConfig: sharedServerConfig,
    });
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: err?.message || 'Không thể lưu cấu hình dùng chung trên máy chủ.',
    });
  }
});

// AI Data Audit Proxy Route
app.post('/api/ai/audit-report', async (req, res) => {
  try {
    const { input, issues, language = 'vi' } = req.body;

    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
      return res.json({
        analysis: language === 'vi'
          ? 'Hệ thống kiểm toán dữ liệu nội bộ đã hoàn thành đối soát 7 tiêu chuẩn nghiệp vụ.'
          : 'Internal data audit engine has completed 7 quality checks.',
      });
    }

    const ai = new GoogleGenAI();
    const isInput = Boolean(input?.isInputArea);
    const totalChecked = Number(input?.tongSoLuongDoikiem || 0);
    const defectQty = Number(input?.tongSoLuongLoiKhongDat || 0);
    const defectPct =
      typeof input?.defectRatePercent === 'number'
        ? input.defectRatePercent
        : totalChecked > 0
        ? Number(((defectQty / totalChecked) * 100).toFixed(2))
        : 0;

    const prompt = `
Bạn là chuyên gia kiểm toán dữ liệu chất lượng sản xuất giày dép (Decathlon / TBS Group Footwear Quality Assurance).
Nhiệm vụ: ${
      isInput
        ? 'Phân tích nhanh số liệu kiểm tra ĐẦU VÀO theo chỉ số % Lỗi (KHÔNG tính RFT cho khu vực Đầu vào) để đưa ra 1-2 câu nhận xét nghiệp vụ ngắn gọn, súc tích (dưới 50 từ), không dài dòng, không dùng từ sáo rỗng.'
        : 'Phân tích nhanh số liệu báo cáo RFT theo giờ để đưa ra 1-2 câu nhận xét nghiệp vụ ngắn gọn, súc tích (dưới 50 từ), không dài dòng, không dùng từ sáo rỗng.'
    }
Ngôn ngữ phản hồi: ${language === 'vi' ? 'Tiếng Việt chuyên ngành giày' : 'Professional English footwear quality terms'}.

Dữ liệu báo cáo:
- Nhà máy: ${input?.selectedFactory || 'Thoại Sơn 1'}
- Chuyền/Vị trí: ${input?.chuyen} (${input?.selectedLine})
- Công đoạn: ${input?.currentStageName}
- Mã hàng/vật tư/đế: ${input?.maHang}
- Thời điểm / Khung giờ: ${input?.gioKiem}
- Tổng số lượng kiểm: ${input?.tongSoLuongDoikiem}
- Tổng số lượng kiểm không đạt: ${input?.tongSoLuongDoiKiemKhongDat}
- Tổng số lượng kiểm đạt: ${input?.tongSoLuongDoiKiemDat}
- Tổng số lỗi ghi nhận: ${input?.tongSoLuongLoiKhongDat} lỗi
${isInput ? `- Chỉ số % Lỗi Đầu vào: ${defectPct}%` : `- Tỷ lệ RFT: ${input?.rftPercent}%`}
- Danh mục lỗi chi tiết: ${JSON.stringify(input?.defects?.filter((d: any) => d.quantity > 0) || [])}
- Kết quả kiểm tra quy tắc: ${issues?.length > 0 ? JSON.stringify(issues) : 'Tất cả 7 quy tắc hợp lệ 100%'}

Chỉ đưa ra 1-2 câu nhận định chất lượng, cảnh báo rủi ro nếu có, không lặp lại toàn bộ dữ liệu.`;

    const response = await ai.models.generateContent({
      model: 'gemini-2.5-flash',
      contents: prompt,
    });

    const analysisText = response.text?.trim() || '';
    return res.json({ analysis: analysisText });
  } catch (error: any) {
    console.error('AI Audit Error:', error);
    return res.json({
      analysis: req.body?.language === 'vi'
        ? 'Dữ liệu đã qua kiểm soát bởi bộ quy tắc kiểm toán nghiệp vụ tiêu chuẩn TBS Group.'
        : 'Data audited according to standard TBS Group operational criteria.',
    });
  }
});

// Google Sheets & Apps Script Proxy (avoids browser CORS / 302 redirect issues)
app.post('/api/sheets/fetch', async (req, res) => {
  try {
    const { targetUrl } = req.body || {};
    if (!targetUrl || typeof targetUrl !== 'string') {
      return res.status(400).json({ error: 'Thiếu đường dẫn targetUrl.' });
    }

    const parsedUrl = new URL(targetUrl);
    const allowedHosts = ['docs.google.com', 'script.google.com', 'script.googleusercontent.com'];
    if (!allowedHosts.includes(parsedUrl.hostname)) {
      return res.status(400).json({ error: 'Chỉ hỗ trợ đường dẫn Google Sheets hoặc Google Apps Script.' });
    }

    const upstream = await fetch(targetUrl, {
      method: 'GET',
      redirect: 'follow',
      headers: {
        Accept: 'application/json, text/plain, */*',
        'User-Agent': 'RFT-Report-Proxy/1.0',
      },
    });

    const finalUrl = upstream.url || '';
    const text = await upstream.text();

    // Detect redirect to Google Login (private sheet or restricted GAS deployment)
    if (
      finalUrl.includes('accounts.google.com') ||
      text.includes('accounts.google.com/ServiceLogin') ||
      (text.trim().startsWith('<!DOCTYPE html') && !text.includes('google.visualization.Query.setResponse'))
    ) {
      return res.status(403).json({
        error:
          'Google Sheet hoặc Web App đang ở chế độ riêng tư! Vui lòng mở Chia sẻ Google Sheet -> chọn "Bất kỳ ai có đường liên kết đều có thể xem" (hoặc triển khai Apps Script với quyền "Bất kỳ ai / Anyone").',
      });
    }

    if (!upstream.ok) {
      return res.status(upstream.status).json({
        error: `Google phản hồi mã lỗi HTTP ${upstream.status}. Vui lòng kiểm tra lại đường link và quyền chia sẻ.`,
      });
    }

    return res.json({ ok: true, contentType: upstream.headers.get('content-type') || '', body: text });
  } catch (err: any) {
    console.error('Sheets fetch proxy error:', err);
    return res.status(500).json({
      error: err?.message || 'Không thể kết nối tới Google Sheets từ máy chủ.',
    });
  }
});

app.get('/api/sheets/status', async (_req, res) => {
  await ensureServerConfigHydrated();
  const hasServerGasUrl = Boolean(resolveValidGasUrl(''));
  res.json({ hasServerGasUrl, sharedConfig: sharedServerConfig });
});

app.post('/api/sheets/submit', async (req, res) => {
  try {
    await ensureServerConfigHydrated();
    const { webAppUrl, payload } = req.body || {};
    const resolvedUrl = resolveValidGasUrl(webAppUrl);
    if (!resolvedUrl) {
      return res.status(400).json({
        error: 'Chưa cấu hình URL Google Apps Script Web App (/exec) trong Cấu Hình > Đổ Báo Cáo Ra!',
      });
    }

    const upstream = await fetch(resolvedUrl, {
      method: 'POST',
      redirect: 'follow',
      headers: {
        'Content-Type': 'text/plain;charset=utf-8',
      },
      body: JSON.stringify(payload),
    });

    const text = await upstream.text();
    if (text.includes('accounts.google.com') || text.trim().startsWith('<!DOCTYPE html')) {
      return res.status(403).json({
        error: 'Apps Script Web App yêu cầu quyền truy cập "Bất kỳ ai (Anyone)". Vui lòng triển khai lại Web App.',
      });
    }

    try {
      const json = JSON.parse(text);
      if (json.status === 'error') {
        return res.status(400).json({ error: json.message || 'Apps Script báo lỗi khi ghi dữ liệu.' });
      }
      return res.json({ ok: true, result: json });
    } catch {
      if (!upstream.ok) {
        return res.status(upstream.status).json({
          error: `Google Apps Script trả về lỗi HTTP ${upstream.status}.`,
        });
      }
      return res.json({ ok: true, raw: text });
    }
  } catch (err: any) {
    console.error('Sheets submit proxy error:', err);
    return res.status(500).json({ error: err?.message || 'Lỗi gửi dữ liệu tới Google Apps Script.' });
  }
});

// Login Audit Logger (writes backup log + forwards to Google Sheet RFT > Lich_Su_Dang_Nhap when GAS Web App URL is set)
app.post('/api/auth/log-login', async (req, res) => {
  try {
    await ensureServerConfigHydrated();
    const { webAppUrl, payload } = req.body || {};
    const targetGasUrl = resolveValidGasUrl(webAppUrl);

    if (payload && typeof payload === 'object') {
      try {
        const logFile = '/tmp/rft_login_audit.json';
        let existing: any[] = [];
        if (fs.existsSync(logFile)) {
          try {
            existing = JSON.parse(fs.readFileSync(logFile, 'utf8'));
            if (!Array.isArray(existing)) existing = [];
          } catch {
            existing = [];
          }
        }
        existing.unshift({
          ...payload,
          serverLoggedAt: new Date().toISOString(),
        });
        fs.writeFileSync(logFile, JSON.stringify(existing.slice(0, 1000), null, 2), 'utf8');
      } catch {
        // Non-blocking disk backup
      }
    }

    let forwarded = false;
    if (targetGasUrl) {
      try {
        const upstream = await fetch(targetGasUrl, {
          method: 'POST',
          redirect: 'follow',
          headers: {
            'Content-Type': 'text/plain;charset=utf-8',
          },
          body: JSON.stringify(payload),
        });
        forwarded = upstream.ok;
      } catch {
        forwarded = false;
      }
    }

    return res.json({ ok: true, forwarded });
  } catch (err: any) {
    console.error('Auth log error:', err);
    return res.status(200).json({ ok: false, forwarded: false });
  }
});

// ============================================================================
// AUTHENTICATION & FORGOT PASSWORD ROUTES (Supports Protected/Private TK_MK)
// ============================================================================
const RFT_MASTER_SHEET_ID = '1W0MvbqhMrjBivgWhLcsCPcPoNFJHQ2AZU1d5DOXtH5I';
const RFT_MASTER_SHEET_URL = `https://docs.google.com/spreadsheets/d/${RFT_MASTER_SHEET_ID}/edit`;
const TK_MK_SHEET_NAME = 'TK_MK';
const TK_MK_GID = '1761380248';

const SERVER_VN_OLD_TO_NEW_TONE_MAP: Record<string, string> = {
  oà: 'òa',
  oá: 'óa',
  oả: 'ỏa',
  oã: 'õa',
  oạ: 'ọa',
  oè: 'òe',
  oé: 'óe',
  oẻ: 'ỏe',
  oẽ: 'õe',
  oẹ: 'ọe',
  uỳ: 'ùy',
  uý: 'úy',
  uỷ: 'ủy',
  uỹ: 'ũy',
  uỵ: 'ụy',
};

function normAuthName(input: string): string {
  return String(input || '')
    .replace(/[\u200B-\u200D\uFEFF]/g, '')
    .replace(/\u00A0/g, ' ')
    .normalize('NFC')
    .replace(/\s+/g, ' ')
    .trim();
}

function compareAuthNameKey(input: string): string {
  return normAuthName(input)
    .toLocaleLowerCase('vi-VN')
    .replace(
      /(o[àáảãạ]|o[èéẻẽẹ]|u[ỳýỷỹỵ])(?![a-z\u00c0-\u1ef9])/g,
      (m) => SERVER_VN_OLD_TO_NEW_TONE_MAP[m] || m
    );
}

function convertServerAvatarUrl(rawUrl?: string): string {
  const raw = String(rawUrl || '').trim();
  if (!raw) return '';
  const fileDMatch = raw.match(/drive\.google\.com\/file\/d\/([a-zA-Z0-9_-]+)/i);
  if (fileDMatch && fileDMatch[1]) {
    return `https://drive.google.com/thumbnail?id=${fileDMatch[1]}&sz=w200`;
  }
  const queryIdMatch = raw.match(/[?&]id=([a-zA-Z0-9_-]+)/i);
  if (queryIdMatch && queryIdMatch[1] && raw.includes('google.com')) {
    return `https://drive.google.com/thumbnail?id=${queryIdMatch[1]}&sz=w200`;
  }
  const lh3Match = raw.match(/googleusercontent\.com\/d\/([a-zA-Z0-9_-]+)/i);
  if (lh3Match && lh3Match[1]) {
    return `https://drive.google.com/thumbnail?id=${lh3Match[1]}&sz=w200`;
  }
  return /^https?:\/\//i.test(raw) ? raw : '';
}

function formatServerLoginTimestamp(date: Date = new Date()): string {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Ho_Chi_Minh',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(date);
  const get = (type: string) => parts.find((p) => p.type === type)?.value || '00';
  return `${get('day')}/${get('month')}/${get('year')} ${get('hour')}:${get('minute')}:${get('second')}`;
}

function getServerRowCell(
  row: Record<string, any>,
  preferredNames: string[],
  fallbackColIndex: number
): string {
  const keys = Object.keys(row);
  const norm = (s: string) =>
    String(s || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/đ/g, 'd')
      .replace(/Đ/g, 'D')
      .toLowerCase()
      .replace(/[_]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();

  for (const target of preferredNames) {
    const tNorm = norm(target);
    const matchedKey = keys.find((k) => norm(k) === tNorm || norm(k).includes(tNorm));
    if (matchedKey !== undefined && row[matchedKey] !== undefined && row[matchedKey] !== null) {
      return String(row[matchedKey]).trim();
    }
  }

  const colKey = keys[fallbackColIndex];
  if (colKey !== undefined && row[colKey] !== undefined && row[colKey] !== null) {
    return String(row[colKey]).trim();
  }
  return '';
}

function parseServerGvizRows(text: string): Array<Record<string, any>> {
  const jsonStart = text.indexOf('{');
  const jsonEnd = text.lastIndexOf('}');
  if (jsonStart === -1 || jsonEnd === -1) return [];
  const parsed = JSON.parse(text.substring(jsonStart, jsonEnd + 1));
  if (parsed.status === 'error' || !parsed.table) return [];

  const table = parsed.table;
  if (!Array.isArray(table.cols) || !Array.isArray(table.rows) || table.rows.length === 0) {
    return [];
  }

  const rawLabels = table.cols.map((col: any) => String(col.label || '').trim());
  const hasAnyLabel = rawLabels.some((lbl: string) => lbl.length > 0);
  let headers: string[] = [];
  let dataRows = table.rows;

  if (hasAnyLabel) {
    headers = table.cols.map((col: any, idx: number) => String(col.label || '').trim() || String(col.id || `Cột_${idx + 1}`));
  } else {
    const firstRowCells = table.rows[0]?.c || [];
    const firstRowTexts = table.cols.map((_: any, idx: number) => {
      const cell = firstRowCells[idx];
      return cell ? (cell.v !== null && cell.v !== undefined ? String(cell.v).trim() : String(cell.f || '').trim()) : '';
    });
    if (firstRowTexts.some(Boolean)) {
      headers = firstRowTexts.map((txt: string, idx: number) => txt || String(table.cols[idx]?.id || `Cột_${idx + 1}`));
      dataRows = table.rows.slice(1);
    } else {
      headers = table.cols.map((col: any, idx: number) => String(col.id || `Cột_${idx + 1}`));
    }
  }

  return dataRows
    .map((row: any) => {
      const item: Record<string, any> = {};
      if (row.c) {
        headers.forEach((key, idx) => {
          const cell = row.c[idx];
          item[key] = cell ? (cell.v !== null && cell.v !== undefined ? cell.v : cell.f || '') : '';
        });
      }
      return item;
    })
    .filter((obj: Record<string, any>) => Object.values(obj).some((v) => v !== '' && v !== null && v !== undefined));
}

function validateTkMkRowsOnServer(
  rows: Array<Record<string, any>>,
  usernameInput: string,
  passwordInput: string
) {
  const inputKey = compareAuthNameKey(usernameInput);
  const cleanPassword = String(passwordInput || '').trim();

  const matchingRows = rows.filter((row) => {
    const colA = getServerRowCell(row, ['TÊN NGƯỜI DÙNG', 'TEN NGUOI DUNG'], 0);
    return colA ? compareAuthNameKey(colA) === inputKey : false;
  });

  if (matchingRows.length === 0) {
    return {
      ok: false,
      error: 'Tài khoản không tồn tại trong danh sách TK_MK. Vui lòng kiểm tra lại Họ và tên!',
      reason: 'Sai tên tài khoản (không tồn tại trong TK_MK)',
    };
  }

  const passwordMatchedRow = matchingRows.find((row) => {
    const colC = getServerRowCell(row, ['MẬT KHẨU', 'MAT KHAU'], 2);
    return colC === cleanPassword;
  });

  if (!passwordMatchedRow) {
    return {
      ok: false,
      matchedName: normAuthName(getServerRowCell(matchingRows[0], ['TÊN NGƯỜI DÙNG'], 0)),
      matchedRole: getServerRowCell(matchingRows[0], ['Vai trò', 'Vai tro'], 3),
      error: 'Mật khẩu không chính xác. Vui lòng kiểm tra lại!',
      reason: 'Sai mật khẩu',
    };
  }

  const fullName = normAuthName(getServerRowCell(passwordMatchedRow, ['TÊN NGƯỜI DÙNG'], 0));
  const chucVu = getServerRowCell(passwordMatchedRow, ['CHỨC VỤ', 'CHUC VU'], 1);
  const role = getServerRowCell(passwordMatchedRow, ['Vai trò', 'Vai tro'], 3) || 'QC_User';
  const permissions = getServerRowCell(passwordMatchedRow, ['Quyền hạn', 'Quyen han'], 4);
  const status = getServerRowCell(passwordMatchedRow, ['Trạng thái', 'Trang thai'], 5);
  const email = getServerRowCell(passwordMatchedRow, ['Email'], 7);
  const rawAvatarUrl = getServerRowCell(passwordMatchedRow, ['Hình ảnh TK', 'Hinh anh TK'], 8);

  if (status.toUpperCase() !== 'ACTIVE') {
    return {
      ok: false,
      matchedName: fullName,
      matchedRole: role,
      error: `Tài khoản "${fullName}" hiện có trạng thái "${status || 'CHƯA KÍCH HOẠT'}". Chỉ tài khoản ACTIVE mới được phép đăng nhập!`,
      reason: `Trạng thái tài khoản không hợp lệ (${status || 'TRỐNG'})`,
    };
  }

  return {
    ok: true,
    matchedName: fullName,
    matchedRole: role,
    reason: 'Đăng nhập hợp lệ',
    user: {
      fullName,
      normalizedName: fullName,
      chucVu,
      role,
      permissions,
      status: status.toUpperCase(),
      email,
      rawAvatarUrl,
      avatarUrl: convertServerAvatarUrl(rawAvatarUrl),
      loggedInAt: formatServerLoginTimestamp(),
    },
  };
}

app.post('/api/auth/login', async (req, res) => {
  try {
    await ensureServerConfigHydrated();
    const { username, password, sheetUrl, webAppUrl } = req.body || {};
    const cleanUser = normAuthName(username);
    const cleanPass = String(password || '').trim();
    if (!cleanUser || !cleanPass) {
      return res.status(400).json({ ok: false, error: 'Vui lòng nhập đầy đủ Họ và tên và Mật khẩu!' });
    }

    const gasUrl = resolveValidGasUrl(webAppUrl);
    const preferredSheetUrl =
      typeof sheetUrl === 'string' && sheetUrl.includes('docs.google.com/spreadsheets/d/')
        ? sheetUrl.trim()
        : RFT_MASTER_SHEET_URL;

    let gasRows: Array<Record<string, any>> | null = null;

    // 1. Priority 1: Read TK_MK via Google Apps Script Web App (works even when sheet/tab is protected/private)
    if (gasUrl) {
      const sheetCandidates = preferredSheetUrl !== RFT_MASTER_SHEET_URL
        ? [preferredSheetUrl, RFT_MASTER_SHEET_URL]
        : [RFT_MASTER_SHEET_URL];

      for (const sUrl of sheetCandidates) {
        try {
          const params = new URLSearchParams({
            action: 'getData',
            url: sUrl,
            sheet: TK_MK_SHEET_NAME,
            gid: TK_MK_GID,
            t: String(Date.now()),
          });
          const fetchUrl = `${gasUrl.split('#')[0]}${gasUrl.includes('?') ? '&' : '?'}${params.toString()}`;
          const gasRes = await fetch(fetchUrl, {
            method: 'GET',
            redirect: 'follow',
            headers: { Accept: 'application/json, text/plain, */*', 'User-Agent': 'RFT-Report-Proxy/1.0' },
          });
          const gasText = await gasRes.text();
          if (gasRes.ok && !gasText.trim().startsWith('<')) {
            const parsed = JSON.parse(gasText);
            if (Array.isArray(parsed)) {
              gasRows = parsed;
              break;
            }
            if (Array.isArray(parsed?.data) && parsed.data.length > 0) {
              gasRows = parsed.data;
              break;
            }
          }
        } catch {
          // Try next candidate or GViz fallback
        }
      }
    }

    if (gasRows && gasRows.length > 0) {
      updateSafeTkMkCacheFromRows(gasRows);
      const authResult = validateTkMkRowsOnServer(gasRows, cleanUser, cleanPass);
      if (authResult.ok && authResult.user) {
        const sessionToken = signServerSessionToken(authResult.user);
        recordAccountLoginPresence(authResult.user, req.body?.deviceSummary);
        return res.status(200).json({ ...authResult, sessionToken });
      }
      return res.status(401).json(authResult);
    }

    // 2. Priority 2: Fallback to Google GViz (works for public-view sheets even when cells/tabs have Google Sheet protection)
    const gvizUrls: string[] = [];
    const addGvizCandidates = (urlStr: string) => {
      const m = urlStr.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
      if (m && m[1]) {
        gvizUrls.push(
          `https://docs.google.com/spreadsheets/d/${m[1]}/gviz/tq?tqx=out:json&sheet=${encodeURIComponent(TK_MK_SHEET_NAME)}&t=${Date.now()}`,
          `https://docs.google.com/spreadsheets/d/${m[1]}/gviz/tq?tqx=out:json&gid=${TK_MK_GID}&t=${Date.now()}`
        );
      }
    };
    addGvizCandidates(preferredSheetUrl);
    if (preferredSheetUrl !== RFT_MASTER_SHEET_URL) {
      addGvizCandidates(RFT_MASTER_SHEET_URL);
    }

    let lastHttpStatus = 0;
    let detectedPrivateRedirect = false;

    for (const gvizUrl of gvizUrls) {
      try {
        const up = await fetch(gvizUrl, {
          method: 'GET',
          redirect: 'follow',
          headers: { Accept: 'application/json, text/plain, */*', 'User-Agent': 'RFT-Report-Proxy/1.0' },
        });
        const body = await up.text();
        if (
          up.url?.includes('accounts.google.com') ||
          body.includes('accounts.google.com/ServiceLogin') ||
          (body.trim().startsWith('<!DOCTYPE html') && !body.includes('google.visualization.Query.setResponse'))
        ) {
          detectedPrivateRedirect = true;
          continue;
        }
        if (!up.ok) {
          lastHttpStatus = up.status;
          continue;
        }
        const rows = parseServerGvizRows(body);
        if (rows.length > 0) {
          updateSafeTkMkCacheFromRows(rows);
          const authResult = validateTkMkRowsOnServer(rows, cleanUser, cleanPass);
          if (authResult.ok && authResult.user) {
            const sessionToken = signServerSessionToken(authResult.user);
            recordAccountLoginPresence(authResult.user, req.body?.deviceSummary);
            return res.status(200).json({ ...authResult, sessionToken });
          }
          return res.status(401).json(authResult);
        }
      } catch {
        // continue to next URL
      }
    }

    if (detectedPrivateRedirect || lastHttpStatus === 401 || lastHttpStatus === 403 || lastHttpStatus === 404) {
      return res.status(403).json({
        ok: false,
        requiresGasConfig: !gasUrl,
        error: gasUrl
          ? 'Không thể đọc danh sách TK_MK từ Google Sheet hoặc Apps Script Web App. Vui lòng kiểm tra quyền triển khai Apps Script ("Thực thi: Tôi" & "Truy cập: Bất kỳ ai").'
          : 'Trang tính đang được bảo vệ hoặc giới hạn chia sẻ. Vui lòng kết nối URL Apps Script Web App (/exec) để đăng nhập bình thường!',
      });
    }

    return res.status(502).json({
      ok: false,
      requiresGasConfig: !gasUrl,
      error: 'Không đọc được danh sách tài khoản từ tab TK_MK trên Google Sheet RFT.',
    });
  } catch (err: any) {
    console.error('Auth login route error:', err);
    return res.status(500).json({
      ok: false,
      error: err?.message || 'Lỗi máy chủ khi xác thực tài khoản.',
    });
  }
});

app.post('/api/auth/reset-password', async (req, res) => {
  try {
    await ensureServerConfigHydrated();
    const { username, email, newPassword, sheetUrl, webAppUrl } = req.body || {};
    const cleanUser = normAuthName(username);
    const cleanEmail = String(email || '').trim();
    const cleanNewPass = String(newPassword || '').trim();

    if (!cleanUser || !cleanEmail || cleanNewPass.length < 4) {
      return res.status(400).json({
        ok: false,
        error: 'Vui lòng nhập đầy đủ Họ và tên, Email đã đăng ký và Mật khẩu mới (tối thiểu 4 ký tự).',
      });
    }

    const gasUrl = resolveValidGasUrl(webAppUrl);
    if (!gasUrl) {
      return res.status(400).json({
        ok: false,
        requiresGasConfig: true,
        error: 'Chưa kết nối URL Google Apps Script Web App (/exec) nên chưa thể tự cập nhật mật khẩu trên trang tính TK_MK. Vui lòng dán URL /exec hoặc liên hệ Quản trị viên (ADMIN).',
      });
    }

    // Kiểm tra nhanh xem Web App đã cập nhật V7.2 (hỗ trợ resetPassword) hay chưa
    try {
      const statusCheckUrl = `${gasUrl.split('#')[0]}${gasUrl.includes('?') ? '&' : '?'}action=status&t=${Date.now()}`;
      const statusRes = await fetch(statusCheckUrl, {
        method: 'GET',
        redirect: 'follow',
        headers: { Accept: 'application/json, text/plain, */*', 'User-Agent': 'RFT-Report-Proxy/1.0' },
      });
      const statusText = await statusRes.text();
      if (statusText.includes('accounts.google.com') || statusText.trim().startsWith('<!DOCTYPE html')) {
        return res.status(403).json({
          ok: false,
          error: 'Apps Script Web App yêu cầu quyền truy cập "Bất kỳ ai (Anyone)". Vui lòng triển khai lại Web App.',
        });
      }
      const statusJson = JSON.parse(statusText);
      if (!statusJson?.authSupported) {
        return res.status(400).json({
          ok: false,
          requiresGasUpdate: true,
          error: 'Apps Script hiện tại đang là phiên bản cũ chưa hỗ trợ tự đặt lại mật khẩu. Vui lòng cập nhật mã Code.gs V7.2 trong Cấu hình hoặc liên hệ ADMIN để đổi mật khẩu.',
        });
      }
    } catch {
      return res.status(502).json({
        ok: false,
        error: 'Không thể kiểm tra kết nối tới Apps Script Web App (/exec).',
      });
    }

    const postRes = await fetch(gasUrl, {
      method: 'POST',
      redirect: 'follow',
      headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: JSON.stringify({
        action: 'resetPassword',
        username: cleanUser,
        email: cleanEmail,
        newPassword: cleanNewPass,
        sheetUrl:
          typeof sheetUrl === 'string' && sheetUrl.includes('docs.google.com/spreadsheets/d/')
            ? sheetUrl.trim()
            : RFT_MASTER_SHEET_URL,
      }),
    });

    const postText = await postRes.text();
    const postJson = JSON.parse(postText);
    if (postJson?.status === 'success') {
      return res.json({
        ok: true,
        fullName: postJson.fullName || cleanUser,
        message: postJson.message || 'Đã cập nhật mật khẩu mới thành công!',
      });
    }

    return res.status(400).json({
      ok: false,
      error: postJson?.message || 'Không thể đặt lại mật khẩu. Vui lòng kiểm tra lại thông tin.',
    });
  } catch (err: any) {
    console.error('Reset password route error:', err);
    return res.status(500).json({
      ok: false,
      error: err?.message || 'Lỗi máy chủ khi đặt lại mật khẩu.',
    });
  }
});

// ============================================================================
// SAFE PERSONNEL DIRECTORY API (TK_MK + Danh sách — strictly NO passwords/emails)
// ============================================================================
type PersonnelCategoryServer = 'QA' | 'QC' | 'CBQL' | 'THU_KHO' | 'OTHER';

interface SafePersonnelItem {
  name: string;
  chucVu: string;
  role: string;
  category: PersonnelCategoryServer;
  factory?: string;
  line?: string;
  viTri?: string;
}

function classifyServerPersonnelCategory(role?: string, chucVu?: string): PersonnelCategoryServer {
  const roleNorm = normAccentFreeUpper(role);
  const cvNorm = normAccentFreeUpper(chucVu);
  const combined = `${roleNorm} | ${cvNorm}`;

  if (
    roleNorm.startsWith('TK_') ||
    roleNorm === 'TK_USER' ||
    combined.includes('THU KHO') ||
    combined.includes('WAREHOUSE') ||
    /\bKHO\b/.test(combined)
  ) {
    return 'THU_KHO';
  }
  if (
    roleNorm.startsWith('QA_') ||
    roleNorm === 'QA' ||
    /\bQA\b/.test(combined) ||
    combined.includes('QA ')
  ) {
    return 'QA';
  }
  if (
    roleNorm === 'MANAGER' ||
    roleNorm === 'TP_LEADER' ||
    roleNorm === 'T.TEAM_LEADER' ||
    roleNorm.includes('LEADER') ||
    combined.includes('CBQL') ||
    combined.includes('CAN BO QUAN LY') ||
    combined.includes('TO TRUONG') ||
    combined.includes('QUAN LY') ||
    combined.includes('LINE MANAGER') ||
    combined.includes('TGD') ||
    combined.includes('PGD') ||
    combined.includes('TP.QLCL') ||
    combined.includes('T.TEAM')
  ) {
    return 'CBQL';
  }
  if (
    roleNorm.startsWith('QC_') ||
    roleNorm.startsWith('RQC_') ||
    roleNorm === 'QC' ||
    roleNorm === 'RQC' ||
    /\bQC\b/.test(combined) ||
    /\bRQC\b/.test(combined)
  ) {
    return 'QC';
  }
  return 'OTHER';
}

let personnelCache: {
  data: SafePersonnelItem[];
  fetchedAt: number;
} | null = null;

async function fetchServerSheetRowsByTabOrGid(
  sheetUrl: string,
  tabName: string,
  gid: string,
  gasUrl?: string
): Promise<Array<Record<string, any>>> {
  if (gasUrl) {
    try {
      const params = new URLSearchParams({
        action: 'getData',
        url: sheetUrl,
        sheet: tabName,
        gid,
        t: String(Date.now()),
      });
      const fetchUrl = `${gasUrl.split('#')[0]}${gasUrl.includes('?') ? '&' : '?'}${params.toString()}`;
      const gasRes = await fetch(fetchUrl, {
        method: 'GET',
        redirect: 'follow',
        headers: { Accept: 'application/json, text/plain, */*', 'User-Agent': 'RFT-Report-Proxy/1.0' },
      });
      const gasText = await gasRes.text();
      if (gasRes.ok && !gasText.trim().startsWith('<')) {
        const parsed = JSON.parse(gasText);
        if (Array.isArray(parsed) && parsed.length > 0) return parsed;
        if (Array.isArray(parsed?.data) && parsed.data.length > 0) return parsed.data;
      }
    } catch {
      // Fallback to GViz
    }
  }

  const m = sheetUrl.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
  const sheetId = m?.[1] || '1W0MvbqhMrjBivgWhLcsCPcPoNFJHQ2AZU1d5DOXtH5I';
  const candidates = [
    `https://docs.google.com/spreadsheets/d/${sheetId}/gviz/tq?tqx=out:json&sheet=${encodeURIComponent(tabName)}&t=${Date.now()}`,
    `https://docs.google.com/spreadsheets/d/${sheetId}/gviz/tq?tqx=out:json&gid=${gid}&t=${Date.now()}`,
  ];

  for (const url of candidates) {
    try {
      const up = await fetch(url, {
        method: 'GET',
        redirect: 'follow',
        headers: { Accept: 'application/json, text/plain, */*', 'User-Agent': 'RFT-Report-Proxy/1.0' },
      });
      if (!up.ok) continue;
      const body = await up.text();
      if (body.includes('accounts.google.com/ServiceLogin')) continue;
      const rows = parseServerGvizRows(body);
      if (rows.length > 0) return rows;
    } catch {
      // next candidate
    }
  }
  return [];
}

function getServerRowCellExactFirst(
  row: Record<string, any>,
  preferredNames: string[],
  fallbackColIndex: number,
  excludeKeys: Set<string> = new Set()
): { value: string; matchedKey?: string } {
  const keys = Object.keys(row).filter((k) => !excludeKeys.has(k));
  const norm = (s: string) =>
    String(s || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/đ/g, 'd')
      .replace(/Đ/g, 'D')
      .toLowerCase()
      .replace(/\s+/g, ' ')
      .trim();

  // 1. Exact match first
  for (const target of preferredNames) {
    const tNorm = norm(target);
    const matchedKey = keys.find((k) => norm(k) === tNorm);
    if (matchedKey !== undefined && row[matchedKey] !== undefined && row[matchedKey] !== null) {
      return { value: String(row[matchedKey]).trim(), matchedKey };
    }
  }
  // 2. Partial match second
  for (const target of preferredNames) {
    const tNorm = norm(target);
    const matchedKey = keys.find((k) => norm(k).includes(tNorm));
    if (matchedKey !== undefined && row[matchedKey] !== undefined && row[matchedKey] !== null) {
      return { value: String(row[matchedKey]).trim(), matchedKey };
    }
  }

  if (fallbackColIndex >= 0) {
    const allKeys = Object.keys(row);
    const colKey = allKeys[fallbackColIndex];
    if (colKey !== undefined && !excludeKeys.has(colKey) && row[colKey] !== undefined && row[colKey] !== null) {
      return { value: String(row[colKey]).trim(), matchedKey: colKey };
    }
  }
  return { value: '' };
}

function splitPersonnelCellNames(raw: string): string[] {
  const text = String(raw || '').trim();
  if (!text) return [];
  return text
    .split(/[,;\n]+/)
    .map((part) => normAuthName(part))
    .filter((name) => {
      if (!name) return false;
      const up = normAccentFreeUpper(name);
      return !['QA', 'QC', 'CBQL', 'THU KHO', 'CBQL/THU KHO', 'CBQL / THU KHO', 'NV QA', 'CN QC', '-', 'N/A', 'NONE'].includes(up);
    });
}

app.get('/api/personnel', async (req, res) => {
  try {
    await ensureServerConfigHydrated();
    const forceRefresh = req.query.refresh === '1';
    const now = Date.now();
    if (!forceRefresh && personnelCache && now - personnelCache.fetchedAt < 120_000) {
      return res.json({
        ok: true,
        personnel: personnelCache.data,
        cachedAt: new Date(personnelCache.fetchedAt).toISOString(),
      });
    }

    const gasUrl = resolveValidGasUrl();
    const configuredSheetUrl =
      sharedServerConfig.sheetSourceUrl &&
      sharedServerConfig.sheetSourceUrl.includes('docs.google.com/spreadsheets/d/')
        ? sharedServerConfig.sheetSourceUrl
        : RFT_MASTER_SHEET_URL;

    let rosterRows = await fetchServerSheetRowsByTabOrGid(
      configuredSheetUrl,
      'Danh sách',
      '0',
      gasUrl || undefined
    );
    if (rosterRows.length === 0 && configuredSheetUrl !== RFT_MASTER_SHEET_URL) {
      rosterRows = await fetchServerSheetRowsByTabOrGid(
        RFT_MASTER_SHEET_URL,
        'Danh sách',
        '0',
        gasUrl || undefined
      );
    }

    const rosterByNameKey = new Map<string, SafePersonnelItem>();
    let hasDanhSachQa = false;
    let hasDanhSachQc = false;
    let hasDanhSachCbql = false;
    let hasDanhSachThuKho = false;

    const upsertRosterPerson = (
      name: string,
      category: PersonnelCategoryServer,
      chucVu: string,
      role: string,
      meta: { factory?: string; line?: string; viTri?: string }
    ) => {
      const k = compareAuthNameKey(name);
      if (!k) return;
      if (!rosterByNameKey.has(k)) {
        rosterByNameKey.set(k, {
          name,
          chucVu,
          role,
          category,
          ...(meta.factory ? { factory: meta.factory } : {}),
          ...(meta.line ? { line: meta.line } : {}),
          ...(meta.viTri ? { viTri: meta.viTri } : {}),
        });
      }
    };

    rosterRows.forEach((row) => {
      const usedKeys = new Set<string>();
      const nhaMayRes = getServerRowCellExactFirst(row, ['Nhà máy', 'Nha may', 'Factory'], 0, usedKeys);
      if (nhaMayRes.matchedKey) usedKeys.add(nhaMayRes.matchedKey);

      const lineRes = getServerRowCellExactFirst(row, ['Line', 'Chuyền', 'Chuyen'], 1, usedKeys);
      if (lineRes.matchedKey) usedKeys.add(lineRes.matchedKey);

      const viTriRes = getServerRowCellExactFirst(row, ['Vị trí', 'Vi tri', 'Trạm', 'Station'], 2, usedKeys);
      if (viTriRes.matchedKey) usedKeys.add(viTriRes.matchedKey);

      const qaRes = getServerRowCellExactFirst(row, ['QA', 'NV QA', 'QA phụ trách'], 3, usedKeys);
      if (qaRes.matchedKey) usedKeys.add(qaRes.matchedKey);

      const qcRes = getServerRowCellExactFirst(row, ['QC', 'CN QC', 'QC kiểm tra'], 4, usedKeys);
      if (qcRes.matchedKey) usedKeys.add(qcRes.matchedKey);

      // Optional CBQL / Thủ kho columns on sheet "Danh sách" (matched strictly by header name)
      const combinedMgrRes = getServerRowCellExactFirst(
        row,
        ['CBQL/Thủ kho', 'CBQL / Thủ kho', 'CBQL - Thủ kho', 'CBQL/Thu kho', 'CBQL / Thu kho', 'Xác nhận'],
        -1,
        usedKeys
      );
      if (combinedMgrRes.matchedKey) usedKeys.add(combinedMgrRes.matchedKey);

      const thuKhoRes = getServerRowCellExactFirst(
        row,
        ['Thủ kho', 'Thu kho', 'NV Kho', 'Thủ kho xác nhận', 'Kho'],
        -1,
        usedKeys
      );
      if (thuKhoRes.matchedKey) usedKeys.add(thuKhoRes.matchedKey);

      const cbqlRes = getServerRowCellExactFirst(
        row,
        ['CBQL', 'Cán bộ quản lý', 'Can bo quan ly', 'Tổ trưởng', 'To truong', 'Quản lý', 'Quan ly', 'CBQL xác nhận'],
        -1,
        usedKeys
      );
      if (cbqlRes.matchedKey) usedKeys.add(cbqlRes.matchedKey);

      const nhaMay = nhaMayRes.value;
      const line = lineRes.value;
      const viTri = viTriRes.value;
      const meta = { factory: nhaMay, line, viTri };
      const lineLocNorm = normAccentFreeUpper(`${line} ${viTri}`);
      const isWarehouseRow =
        lineLocNorm.includes('DAU VAO') ||
        lineLocNorm.includes('KHO VAT TU') ||
        lineLocNorm.includes('KHO TTPP') ||
        lineLocNorm.includes('THU KHO');

      splitPersonnelCellNames(qaRes.value).forEach((name) => {
        hasDanhSachQa = true;
        upsertRosterPerson(name, 'QA', 'NV QA', 'QA_Leader', meta);
      });

      splitPersonnelCellNames(qcRes.value).forEach((name) => {
        hasDanhSachQc = true;
        upsertRosterPerson(name, 'QC', 'CN QC', 'QC_User', meta);
      });

      splitPersonnelCellNames(thuKhoRes.value).forEach((name) => {
        hasDanhSachThuKho = true;
        upsertRosterPerson(name, 'THU_KHO', 'Thủ kho', 'TK_User', meta);
      });

      splitPersonnelCellNames(cbqlRes.value).forEach((name) => {
        if (isWarehouseRow) {
          hasDanhSachThuKho = true;
          upsertRosterPerson(name, 'THU_KHO', 'Thủ kho', 'TK_User', meta);
        } else {
          hasDanhSachCbql = true;
          upsertRosterPerson(name, 'CBQL', 'CBQL', 'T.Team_Leader', meta);
        }
      });

      splitPersonnelCellNames(combinedMgrRes.value).forEach((name) => {
        if (isWarehouseRow) {
          hasDanhSachThuKho = true;
          upsertRosterPerson(name, 'THU_KHO', 'Thủ kho', 'TK_User', meta);
        } else {
          hasDanhSachCbql = true;
          upsertRosterPerson(name, 'CBQL', 'CBQL', 'T.Team_Leader', meta);
        }
      });
    });

    // Only consult TK_MK if sheet "Danh sách" failed to load or does not yet have CBQL / Thủ kho columns
    const needsTkMkFallback =
      rosterRows.length === 0 || !hasDanhSachQa || !hasDanhSachQc || !hasDanhSachCbql || !hasDanhSachThuKho;

    if (needsTkMkFallback) {
      const tkMkRows = await fetchServerSheetRowsByTabOrGid(
        RFT_MASTER_SHEET_URL,
        TK_MK_SHEET_NAME,
        TK_MK_GID,
        gasUrl || undefined
      );

      tkMkRows.forEach((row) => {
        const fullName = normAuthName(getServerRowCell(row, ['TÊN NGƯỜI DÙNG', 'TEN NGUOI DUNG'], 0));
        if (!fullName || normAccentFreeUpper(fullName) === 'TEN NGUOI DUNG') return;

        const status = getServerRowCell(row, ['Trạng thái', 'Trang thai'], 5);
        if (status && status.toUpperCase() !== 'ACTIVE') return;

        const chucVu = getServerRowCell(row, ['CHỨC VỤ', 'CHUC VU'], 1);
        const role = getServerRowCell(row, ['Vai trò', 'Vai tro'], 3);
        const category = classifyServerPersonnelCategory(role, chucVu);
        if (category === 'OTHER') return;

        // Strict rule: QA & QC always come from sheet "Danh sách" when available
        if (category === 'QA' && hasDanhSachQa) return;
        if (category === 'QC' && hasDanhSachQc) return;
        if (category === 'CBQL' && hasDanhSachCbql) return;
        if (category === 'THU_KHO' && hasDanhSachThuKho) return;

        const key = compareAuthNameKey(fullName);
        if (rosterByNameKey.has(key)) return;

        rosterByNameKey.set(key, {
          name: fullName,
          chucVu: chucVu || role || category,
          role: role || '',
          category,
        });
      });
    }

    const list = Array.from(rosterByNameKey.values()).sort((a, b) =>
      a.name.localeCompare(b.name, 'vi')
    );

    if (list.length > 0) {
      personnelCache = { data: list, fetchedAt: now };
    }

    return res.json({
      ok: true,
      personnel: list,
      sourceSummary: {
        qaFromDanhSach: hasDanhSachQa,
        qcFromDanhSach: hasDanhSachQc,
        cbqlFromDanhSach: hasDanhSachCbql,
        thuKhoFromDanhSach: hasDanhSachThuKho,
      },
      cachedAt: new Date(now).toISOString(),
    });
  } catch (err: any) {
    console.error('Personnel directory route error:', err);
    return res.status(500).json({
      ok: false,
      personnel: personnelCache?.data || [],
      error: err?.message || 'Không tải được danh sách nhân sự.',
    });
  }
});

// Direct Google Drive Folder & Spreadsheet Tab Inspector (works for shared folders without needing GAS /exec)
function decodeJsEscapes(str: string): string {
  return str
    .replace(/\\x([0-9a-fA-F]{2})/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)))
    .replace(/\\u([0-9a-fA-F]{4})/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)))
    .replace(/\\\//g, '/');
}

function decodeHtmlEntities(str: string): string {
  return str
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

async function fetchSpreadsheetTabsFromHtmlView(sheetId: string): Promise<Array<{ name: string; gid: string }>> {
  try {
    const res = await fetch(`https://docs.google.com/spreadsheets/d/${sheetId}/htmlview`, {
      redirect: 'follow',
      headers: { 'User-Agent': 'RFT-Report-Proxy/1.0' },
    });
    if (!res.ok) return [];
    const html = await res.text();
    if (html.includes('accounts.google.com/ServiceLogin')) return [];

    const tabs: Array<{ name: string; gid: string }> = [];
    const itemRegex = /items\.push\(\{name:\s*"((?:[^"\\]|\\.)*)"[^}]*?gid:\s*"([0-9]+)"/g;
    let m: RegExpExecArray | null;
    while ((m = itemRegex.exec(html)) !== null) {
      tabs.push({
        name: decodeJsEscapes(m[1]),
        gid: m[2],
      });
    }
    return tabs;
  } catch {
    return [];
  }
}

interface ScannedDriveFile {
  fileId: string;
  fileName: string;
  folderPath: string;
  url: string;
  lastUpdated?: string;
  sheets: Array<{ name: string; gid: string }>;
}

async function scanPublicDriveFolder(folderId: string, prefixPath = ''): Promise<{
  folderName: string;
  files: ScannedDriveFile[];
  requiresShare?: boolean;
}> {
  const folderRes = await fetch(`https://drive.google.com/embeddedfolderview?id=${encodeURIComponent(folderId)}#list`, {
    redirect: 'follow',
    headers: { 'User-Agent': 'RFT-Report-Proxy/1.0' },
  });
  const html = await folderRes.text();
  if (
    folderRes.url?.includes('accounts.google.com') ||
    html.includes('accounts.google.com/ServiceLogin') ||
    !folderRes.ok
  ) {
    return { folderName: 'RFT', files: [], requiresShare: true };
  }

  const titleMatch = html.match(/<title>([\s\S]*?)<\/title>/i);
  const folderName = titleMatch ? decodeHtmlEntities(titleMatch[1].trim()) : 'RFT';
  const currentPath = prefixPath ? `${prefixPath}/${folderName}` : folderName;

  const entryBlocks = html.split('<div class="flip-entry"').slice(1);
  const spreadsheetEntries: Array<{ fileId: string; fileName: string; lastUpdated?: string }> = [];
  const subFolderIds: string[] = [];

  for (const block of entryBlocks) {
    const sheetMatch = block.match(/href="https:\/\/docs\.google\.com\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
    const subFolderMatch = block.match(/href="https:\/\/drive\.google\.com\/drive\/folders\/([a-zA-Z0-9_-]+)/);
    const entryTitleMatch = block.match(/<div class="flip-entry-title">([\s\S]*?)<\/div>/);
    const modMatch = block.match(/<div class="flip-entry-last-modified"><div>([\s\S]*?)<\/div>/);
    const title = entryTitleMatch ? decodeHtmlEntities(entryTitleMatch[1].trim()) : 'Untitled';
    const lastMod = modMatch ? decodeHtmlEntities(modMatch[1].trim()) : undefined;

    if (sheetMatch && sheetMatch[1]) {
      spreadsheetEntries.push({
        fileId: sheetMatch[1],
        fileName: title,
        lastUpdated: lastMod,
      });
    } else if (subFolderMatch && subFolderMatch[1]) {
      subFolderIds.push(subFolderMatch[1]);
    }
  }

  const filesWithTabs: ScannedDriveFile[] = await Promise.all(
    spreadsheetEntries.map(async (entry) => ({
      fileId: entry.fileId,
      fileName: entry.fileName,
      folderPath: currentPath,
      url: `https://docs.google.com/spreadsheets/d/${entry.fileId}/edit`,
      lastUpdated: entry.lastUpdated,
      sheets: await fetchSpreadsheetTabsFromHtmlView(entry.fileId),
    }))
  );

  // Also scan 1 level of subfolders if present
  if (!prefixPath && subFolderIds.length > 0) {
    const subResults = await Promise.all(
      subFolderIds.slice(0, 8).map((subId) => scanPublicDriveFolder(subId, currentPath))
    );
    for (const sub of subResults) {
      filesWithTabs.push(...sub.files);
    }
  }

  return { folderName, files: filesWithTabs };
}

app.post('/api/drive/inspect', async (req, res) => {
  try {
    const { targetUrl, folderUrl } = req.body || {};
    const urlStr = String(targetUrl || folderUrl || '').trim();
    if (!urlStr) {
      return res.status(400).json({ error: 'Thiếu đường link Folder Drive hoặc Google Sheet.' });
    }

    const folderMatch = urlStr.match(/\/folders\/([a-zA-Z0-9_-]+)/);
    if (folderMatch && folderMatch[1]) {
      const folderId = folderMatch[1];
      const result = await scanPublicDriveFolder(folderId);
      if (result.requiresShare) {
        return res.status(403).json({
          error:
            'Folder Google Drive đang ở chế độ riêng tư! Hãy mở Chia sẻ Folder -> chọn "Bất kỳ ai có đường liên kết đều có thể xem", hoặc dán URL Apps Script Web App (/exec).',
        });
      }
      return res.json({
        ok: true,
        sourceType: 'folder',
        folderId,
        folderName: result.folderName,
        files: result.files,
        timestamp: new Date().toISOString(),
      });
    }

    const sheetMatch = urlStr.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
    if (sheetMatch && sheetMatch[1]) {
      const sheetId = sheetMatch[1];
      const tabs = await fetchSpreadsheetTabsFromHtmlView(sheetId);
      const htmlRes = await fetch(`https://docs.google.com/spreadsheets/d/${sheetId}/htmlview`, { redirect: 'follow' });
      const htmlText = await htmlRes.text().catch(() => '');
      const titleMatch = htmlText.match(/<title>([\s\S]*?)<\/title>/i);
      const fileName = titleMatch ? decodeHtmlEntities(titleMatch[1].replace(/\s*-\s*Google.*$/i, '').trim()) : sheetId;

      return res.json({
        ok: true,
        sourceType: 'sheet',
        files: [
          {
            fileId: sheetId,
            fileName,
            url: `https://docs.google.com/spreadsheets/d/${sheetId}/edit`,
            sheets: tabs,
          },
        ],
        timestamp: new Date().toISOString(),
      });
    }

    return res.status(400).json({ error: 'Đường link chưa phải Google Drive Folder hoặc Google Sheets hợp lệ.' });
  } catch (err: any) {
    console.error('Drive inspect error:', err);
    return res.status(500).json({ error: err?.message || 'Không thể quét Folder Google Drive.' });
  }
});

// ============================================================================
// REAL OUTSIDE-APP WEB PUSH NOTIFICATIONS & CROSS-DEVICE EOD SIGNING QUEUE
// ============================================================================
const VAPID_KEYS_FILE = '/tmp/rft_vapid_keys.json';
const PUSH_SUBS_FILE = '/tmp/rft_push_subscriptions.json';
const EOD_PENDING_FILE = '/tmp/rft_eod_pending_reports.json';

type PushRoleCategory = 'QA' | 'CBQL' | 'THU_KHO' | 'QC' | 'ADMIN';

interface StoredPushSubscription {
  id: string;
  endpoint: string;
  subscription: WebPushSubscription;
  userName: string;
  rawRole: string;
  chucVu: string;
  normalizedRole: PushRoleCategory;
  receiveAllEodAlerts?: boolean;
  deviceSummary?: string;
  createdAt: string;
  updatedAt: string;
}

interface StoredPendingEodItem {
  id: string;
  report: Record<string, any>;
  dailyReportsSnapshot?: Array<Record<string, any>>;
  outputRouting?: Record<string, any>;
  managerRoleLabel: string;
  requiredRoles: Array<'QA' | 'CBQL' | 'THU_KHO'>;
  milestoneTime?: string;
  createdAt: string;
  updatedAt: string;
  createdBy: string;
  status: 'pending' | 'completed';
  pushSentCount?: number;
}

function isValidVapidKeyPair(pub: string, priv: string): boolean {
  try {
    if (!pub || !priv) return false;
    const pubBuf = Buffer.from(pub, 'base64url');
    const privBuf = Buffer.from(priv, 'base64url');
    return pubBuf.length === 65 && pubBuf[0] === 0x04 && privBuf.length === 32;
  } catch {
    return false;
  }
}

function sanitizeBase64UrlKey(raw?: string): string {
  return String(raw || '')
    .replace(/[\s"'`]/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '')
    .trim();
}

function loadOrCreateVapidKeys(): { publicKey: string; privateKey: string } {
  const envPub = sanitizeBase64UrlKey(process.env.VAPID_PUBLIC_KEY);
  const envPriv = sanitizeBase64UrlKey(process.env.VAPID_PRIVATE_KEY);
  if (isValidVapidKeyPair(envPub, envPriv)) {
    return { publicKey: envPub, privateKey: envPriv };
  }
  try {
    if (fs.existsSync(VAPID_KEYS_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(VAPID_KEYS_FILE, 'utf8'));
      const filePub = sanitizeBase64UrlKey(parsed?.publicKey);
      const filePriv = sanitizeBase64UrlKey(parsed?.privateKey);
      if (isValidVapidKeyPair(filePub, filePriv)) {
        return { publicKey: filePub, privateKey: filePriv };
      }
    }
  } catch {
    // regenerate below
  }
  const generated = webpush.generateVAPIDKeys();
  const cleanGen = {
    publicKey: sanitizeBase64UrlKey(generated.publicKey),
    privateKey: sanitizeBase64UrlKey(generated.privateKey),
  };
  try {
    fs.writeFileSync(VAPID_KEYS_FILE, JSON.stringify(cleanGen, null, 2), 'utf8');
  } catch {
    // non-blocking disk write
  }
  return cleanGen;
}

const vapidKeys = loadOrCreateVapidKeys();
const rawVapidSubject = String(process.env.VAPID_SUBJECT || '').trim();
const vapidSubject =
  /^mailto:|^https?:\/\//i.test(rawVapidSubject)
    ? rawVapidSubject
    : 'mailto:qa-qc@tbsgroup.vn';
webpush.setVapidDetails(vapidSubject, vapidKeys.publicKey, vapidKeys.privateKey);

function normAccentFreeUpper(val?: string): string {
  return String(val || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/đ/g, 'd')
    .replace(/Đ/g, 'D')
    .toUpperCase()
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizePushRoleCategory(role?: string, chucVu?: string): PushRoleCategory {
  const roleNorm = normAccentFreeUpper(role);
  const combined = `${roleNorm} | ${normAccentFreeUpper(chucVu)}`;
  if (
    roleNorm.startsWith('TK_') ||
    roleNorm === 'TK_USER' ||
    combined.includes('THU KHO') ||
    combined.includes('WAREHOUSE') ||
    /\bKHO\b/.test(combined)
  ) {
    return 'THU_KHO';
  }
  if (
    roleNorm.startsWith('QA_') ||
    roleNorm === 'QA' ||
    /\bQA\b/.test(combined) ||
    combined.includes('QA ')
  ) {
    return 'QA';
  }
  if (
    roleNorm === 'ADMIN' ||
    roleNorm === 'SUPER_ADMIN' ||
    combined.includes('QUAN TRI')
  ) {
    return 'ADMIN';
  }
  if (
    roleNorm === 'MANAGER' ||
    roleNorm === 'TP_LEADER' ||
    roleNorm === 'T.TEAM_LEADER' ||
    roleNorm.includes('LEADER') ||
    combined.includes('CBQL') ||
    combined.includes('CAN BO QUAN LY') ||
    combined.includes('TO TRUONG') ||
    combined.includes('QUAN LY') ||
    combined.includes('LINE MANAGER') ||
    combined.includes('TGD') ||
    combined.includes('PGD') ||
    combined.includes('TP.QLCL') ||
    combined.includes('T.TEAM')
  ) {
    return 'CBQL';
  }
  return 'QC';
}

function loadPushSubscriptions(): StoredPushSubscription[] {
  try {
    if (fs.existsSync(PUSH_SUBS_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(PUSH_SUBS_FILE, 'utf8'));
      if (Array.isArray(parsed)) return parsed;
    }
  } catch {
    // ignore
  }
  return [];
}

function savePushSubscriptions(list: StoredPushSubscription[]) {
  try {
    fs.writeFileSync(PUSH_SUBS_FILE, JSON.stringify(list, null, 2), 'utf8');
  } catch {
    // ignore
  }
}

function loadPendingEodList(): StoredPendingEodItem[] {
  try {
    if (fs.existsSync(EOD_PENDING_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(EOD_PENDING_FILE, 'utf8'));
      if (Array.isArray(parsed)) {
        const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
        return parsed.filter((item) => {
          const ts = Date.parse(item?.updatedAt || item?.createdAt || '');
          return Number.isNaN(ts) || ts >= cutoff;
        });
      }
    }
  } catch {
    // ignore
  }
  return [];
}

function savePendingEodList(list: StoredPendingEodItem[]) {
  try {
    fs.writeFileSync(EOD_PENDING_FILE, JSON.stringify(list.slice(0, 120), null, 2), 'utf8');
  } catch {
    // ignore
  }
}

const QC_DAILY_ACTIVITY_FILE = '/tmp/rft_qc_daily_activity.json';
const QC_REMINDER_LOG_FILE = '/tmp/rft_daily_qc_reminder_log.json';

interface StoredQcDailyActivity {
  key: string;
  dateStr: string;
  qcName: string;
  factory?: string;
  line?: string;
  chuyen?: string;
  stageId?: string;
  stageName?: string;
  lastReportAt: string;
  reportCount: number;
  hasClosedShift: boolean;
  closedAt?: string;
}

interface DailyQcReminderLog {
  dateStr: string;
  sentSlots: string[];
  lastSentAt?: string;
  lastSummary?: string;
}

function getVietnamClock(date: Date = new Date()): {
  dateStr: string;
  displayDate: string;
  hhmm: string;
  minutesOfDay: number;
  weekdaySun0: number;
} {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Ho_Chi_Minh',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    weekday: 'short',
    hour12: false,
  }).formatToParts(date);

  const map: Record<string, string> = {};
  for (const p of parts) {
    if (p.type !== 'literal') map[p.type] = p.value;
  }
  const yyyy = map.year || '2026';
  const mm = map.month || '01';
  const dd = map.day || '01';
  const hourNum = Number(map.hour === '24' ? '0' : map.hour || '0');
  const minNum = Number(map.minute || '0');
  const weekdayMap: Record<string, number> = {
    Sun: 0,
    Mon: 1,
    Tue: 2,
    Wed: 3,
    Thu: 4,
    Fri: 5,
    Sat: 6,
  };
  return {
    dateStr: `${yyyy}-${mm}-${dd}`,
    displayDate: `${dd}/${mm}/${yyyy}`,
    hhmm: `${String(hourNum).padStart(2, '0')}:${String(minNum).padStart(2, '0')}`,
    minutesOfDay: hourNum * 60 + minNum,
    weekdaySun0: weekdayMap[map.weekday || ''] ?? 1,
  };
}

function loadQcDailyActivities(): StoredQcDailyActivity[] {
  try {
    if (fs.existsSync(QC_DAILY_ACTIVITY_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(QC_DAILY_ACTIVITY_FILE, 'utf8'));
      if (Array.isArray(parsed)) {
        const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
        return parsed.filter((a) => {
          const ts = Date.parse(a?.lastReportAt || '');
          return Number.isNaN(ts) || ts >= cutoff;
        });
      }
    }
  } catch {
    // ignore
  }
  return [];
}

function saveQcDailyActivities(list: StoredQcDailyActivity[]) {
  try {
    fs.writeFileSync(QC_DAILY_ACTIVITY_FILE, JSON.stringify(list.slice(0, 300), null, 2), 'utf8');
  } catch {
    // ignore
  }
}

function loadQcReminderLog(): DailyQcReminderLog {
  const { dateStr } = getVietnamClock();
  try {
    if (fs.existsSync(QC_REMINDER_LOG_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(QC_REMINDER_LOG_FILE, 'utf8'));
      if (parsed && parsed.dateStr === dateStr && Array.isArray(parsed.sentSlots)) {
        return parsed;
      }
    }
  } catch {
    // ignore
  }
  return { dateStr, sentSlots: [] };
}

function saveQcReminderLog(log: DailyQcReminderLog) {
  try {
    fs.writeFileSync(QC_REMINDER_LOG_FILE, JSON.stringify(log, null, 2), 'utf8');
  } catch {
    // ignore
  }
}

let pushSubscriptions: StoredPushSubscription[] = loadPushSubscriptions();
let pendingEodList: StoredPendingEodItem[] = loadPendingEodList();
let qcDailyActivities: StoredQcDailyActivity[] = loadQcDailyActivities();
let qcReminderLog: DailyQcReminderLog = loadQcReminderLog();
const sseClients = new Set<express.Response>();
const wsClients = new Set<WebSocket>();

function recordQcActivityInternal(params: {
  dateStr?: string;
  qcName?: string;
  factory?: string;
  line?: string;
  chuyen?: string;
  stageId?: string;
  stageName?: string;
  isEndOfDay?: boolean;
}) {
  const vnNow = getVietnamClock();
  const dateStr = String(params.dateStr || vnNow.dateStr).trim() || vnNow.dateStr;
  const qcName = normAuthName(params.qcName || 'QC');
  const chuyen = String(params.chuyen || '').trim();
  const stageId = String(params.stageId || '').trim();
  const key = `${dateStr}|${compareAuthNameKey(qcName)}|${chuyen}|${stageId}`;
  const nowIso = new Date().toISOString();

  const idx = qcDailyActivities.findIndex((a) => a.key === key);
  if (idx >= 0) {
    const existing = qcDailyActivities[idx];
    qcDailyActivities[idx] = {
      ...existing,
      qcName: qcName || existing.qcName,
      factory: params.factory || existing.factory,
      line: params.line || existing.line,
      chuyen: chuyen || existing.chuyen,
      stageId: stageId || existing.stageId,
      stageName: params.stageName || existing.stageName,
      lastReportAt: nowIso,
      reportCount: (existing.reportCount || 0) + 1,
      hasClosedShift: existing.hasClosedShift || Boolean(params.isEndOfDay),
      closedAt: params.isEndOfDay ? nowIso : existing.closedAt,
    };
  } else {
    qcDailyActivities.unshift({
      key,
      dateStr,
      qcName,
      factory: params.factory || '',
      line: params.line || '',
      chuyen,
      stageId,
      stageName: params.stageName || '',
      lastReportAt: nowIso,
      reportCount: 1,
      hasClosedShift: Boolean(params.isEndOfDay),
      closedAt: params.isEndOfDay ? nowIso : undefined,
    });
  }
  saveQcDailyActivities(qcDailyActivities);
}

function getTodayQcShiftStatus() {
  const vnNow = getVietnamClock();
  const todayStr = vnNow.dateStr;
  if (qcReminderLog.dateStr !== todayStr) {
    qcReminderLog = { dateStr: todayStr, sentSlots: [] };
    saveQcReminderLog(qcReminderLog);
  }

  const reminderEnabled = sharedServerConfig.qcDailyReminderEnabled !== false;
  const reminderTime = sanitizeReminderTimeHHmm(sharedServerConfig.qcReminderTime || '17:00');
  const repeatMinutes =
    typeof sharedServerConfig.qcReminderRepeatMinutes === 'number'
      ? sharedServerConfig.qcReminderRepeatMinutes
      : 30;
  const skipSunday = sharedServerConfig.qcReminderSkipSunday !== false;

  const [rH, rM] = reminderTime.split(':').map((n) => Number(n));
  const targetMinutes = (Number.isNaN(rH) ? 17 : rH) * 60 + (Number.isNaN(rM) ? 0 : rM);
  const isPastReminderTime = vnNow.minutesOfDay >= targetMinutes;

  const todayEods = pendingEodList.filter((item) => {
    const repDate = String(item.report?.dateStr || '').trim();
    if (repDate === todayStr) return true;
    const createdVn = item.createdAt ? getVietnamClock(new Date(item.createdAt)).dateStr : '';
    return createdVn === todayStr;
  });

  const closedQcNameKeys = new Set<string>();
  const closedStationKeys = new Set<string>();
  todayEods.forEach((item) => {
    const hasQcSigned = Boolean(
      item.report?.qcKiemTraSign || item.report?.qcSignedAt || item.createdBy
    );
    if (hasQcSigned) {
      if (item.createdBy) closedQcNameKeys.add(compareAuthNameKey(item.createdBy));
      if (item.report?.qcKiemTra) closedQcNameKeys.add(compareAuthNameKey(item.report.qcKiemTra));
      const stationKey = `${String(item.report?.chuyen || '').trim()}|${String(item.report?.congDoan || '').trim()}`;
      if (stationKey !== '|') closedStationKeys.add(stationKey);
    }
  });

  const todayActivities = qcDailyActivities.filter((a) => a.dateStr === todayStr);
  const missingActivities = todayActivities.filter((a) => {
    if (a.hasClosedShift) return false;
    const nameKey = compareAuthNameKey(a.qcName);
    const stationKey = `${String(a.chuyen || '').trim()}|${String(a.stageId || '').trim()}`;
    if (nameKey && closedQcNameKeys.has(nameKey)) return false;
    if (stationKey !== '|' && closedStationKeys.has(stationKey)) return false;
    return true;
  });

  const qcSubs = pushSubscriptions.filter((s) => s.normalizedRole === 'QC');
  const unclosedQcSubs = qcSubs.filter((s) => {
    const k = compareAuthNameKey(s.userName);
    return !k || !closedQcNameKeys.has(k);
  });

  // Closed if either:
  // 1) There were activities today and all of them closed shift (and all active QC subs closed), OR
  // 2) No missing activities and at least 1 EOD closeout happened today with no remaining unclosed QC subscribers.
  const allClosed =
    missingActivities.length === 0 &&
    todayEods.length > 0 &&
    (qcSubs.length === 0 || unclosedQcSubs.length === 0);

  return {
    todayStr,
    displayDate: vnNow.displayDate,
    currentTimeVn: vnNow.hhmm,
    minutesOfDay: vnNow.minutesOfDay,
    weekdaySun0: vnNow.weekdaySun0,
    reminderEnabled,
    reminderTime,
    targetMinutes,
    repeatMinutes,
    skipSunday,
    isPastReminderTime,
    allClosed,
    totalEodClosedToday: todayEods.length,
    closedCount: todayEods.length,
    missingActivities: missingActivities.map((a) => ({
      qcName: a.qcName,
      factory: a.factory,
      chuyen: a.chuyen,
      stageName: a.stageName,
      reportCount: a.reportCount,
      lastReportAt: a.lastReportAt,
    })),
    subscribedQcCount: qcSubs.length,
    totalQcDevicesUnclosed: unclosedQcSubs.length,
    unclosedQcNames: Array.from(new Set(unclosedQcSubs.map((s) => s.userName))),
    lastSentAt: qcReminderLog.lastSentAt || null,
    sentSlotsToday: qcReminderLog.sentSlots,
  };
}

async function dispatchQcDailyReminder(options?: {
  slotKey?: string;
  force?: boolean;
  triggeredBy?: string;
}) {
  const status = getTodayQcShiftStatus();
  if (!options?.force && status.allClosed) {
    return { skipped: true, reason: 'Tất cả QC đã chốt ca hôm nay.', status, sentCount: 0, targetedDevices: 0 };
  }

  const missingNamesFromActivity = status.missingActivities
    .map((m) => [m.qcName, m.chuyen].filter(Boolean).join(' ('))
    .map((s) => (s.includes('(') ? `${s})` : s));

  const missingSummary =
    missingNamesFromActivity.length > 0
      ? `Chưa chốt (${missingNamesFromActivity.length}): ${missingNamesFromActivity.slice(0, 3).join(', ')}${
          missingNamesFromActivity.length > 3 ? ` +${missingNamesFromActivity.length - 3}` : ''
        }.`
      : status.allClosed
      ? `Tất cả QC đã chốt ca hôm nay (${status.displayDate}) với ${status.totalEodClosedToday} phiếu chốt ca.`
      : status.totalEodClosedToday === 0
      ? `Hôm nay (${status.displayDate}) chưa có báo cáo chốt ca nào từ QC.`
      : `Vẫn còn ${status.totalQcDevicesUnclosed} thiết bị QC chưa xác nhận chốt ca ngày ${status.displayDate}.`;

  // Target all unclosed QC devices + supervisors/admin who opted in
  const closedKeySet = new Set(
    pendingEodList
      .filter((item) => {
        const repDate = String(item.report?.dateStr || '').trim();
        return repDate === status.todayStr && Boolean(item.report?.qcKiemTraSign || item.report?.qcSignedAt || item.createdBy);
      })
      .flatMap((item) => [compareAuthNameKey(item.createdBy), compareAuthNameKey(item.report?.qcKiemTra)])
      .filter(Boolean)
  );

  const qcTargets = pushSubscriptions.filter(
    (s) => s.normalizedRole === 'QC' && !closedKeySet.has(compareAuthNameKey(s.userName))
  );
  const supervisorTargets = pushSubscriptions.filter(
    (s) => s.normalizedRole !== 'QC' && (s.receiveAllEodAlerts === true || s.normalizedRole === 'ADMIN')
  );

  const qcTitle = `⏰ Nhắc QC chốt ca cuối ngày (${status.reminderTime})`;
  const qcBody =
    missingNamesFromActivity.length > 0
      ? `Đã đến giờ chốt ca ngày ${status.displayDate}. ${missingSummary} Vui lòng kiểm tra và xác nhận chốt ca ngay!`
      : `Đã ${status.currentTimeVn} (${status.displayDate}) — Bạn chưa xác nhận chốt ca hôm nay. Chạm để vào app chốt ca ngay!`;

  let sentCount = 0;
  let failedCount = 0;

  if (qcTargets.length > 0) {
    const resQc = await sendWebPushToSubscriptions(qcTargets, {
      title: qcTitle,
      body: qcBody,
      type: 'qc_daily_reminder',
      tag: `rft-qc-daily-reminder-${status.todayStr}`,
      url: '/?openQcReminder=1',
    });
    sentCount += resQc.sentCount;
    failedCount += resQc.failedCount;
  }

  if (supervisorTargets.length > 0 && (status.missingActivities.length > 0 || status.totalEodClosedToday === 0)) {
    const resSup = await sendWebPushToSubscriptions(supervisorTargets, {
      title: `⚠️ Cảnh báo: QC chưa chốt ca (${status.displayDate})`,
      body: `${missingSummary} Vui lòng đôn đốc QC hoàn tất chốt ca trước khi kết thúc ngày.`,
      type: 'qc_daily_reminder',
      tag: `rft-qc-daily-sup-${status.todayStr}`,
      url: '/?openNotifications=1',
    });
    sentCount += resSup.sentCount;
    failedCount += resSup.failedCount;
  }

  const nowIso = new Date().toISOString();
  if (options?.slotKey && !qcReminderLog.sentSlots.includes(options.slotKey)) {
    qcReminderLog.sentSlots.push(options.slotKey);
  }
  qcReminderLog.lastSentAt = nowIso;
  qcReminderLog.lastSummary = missingSummary;
  saveQcReminderLog(qcReminderLog);

  const eventPayload = {
    todayStr: status.todayStr,
    displayDate: status.displayDate,
    currentTimeVn: status.currentTimeVn,
    reminderTime: status.reminderTime,
    missingSummary,
    missingActivities: status.missingActivities,
    totalEodClosedToday: status.totalEodClosedToday,
    sentAt: nowIso,
    triggeredBy: options?.triggeredBy || 'Hệ thống tự động',
  };
  broadcastSseEvent('qc_daily_reminder', eventPayload);

  return {
    skipped: false,
    status: getTodayQcShiftStatus(),
    sentCount,
    failedCount,
    targetedDevices: qcTargets.length + supervisorTargets.length,
    missingSummary,
  };
}

let isRunningScheduledReminder = false;
async function runScheduledQcReminderCheck() {
  if (isRunningScheduledReminder) return;
  isRunningScheduledReminder = true;
  try {
    const status = getTodayQcShiftStatus();
    if (!status.reminderEnabled) return;
    if (status.skipSunday && status.weekdaySun0 === 0) return;
    if (status.allClosed) return;

    // Active window: from targetMinutes up to 21:30 VN time
    const maxWindowMinutes = 21 * 60 + 30;
    if (status.minutesOfDay < status.targetMinutes || status.minutesOfDay > maxWindowMinutes) {
      return;
    }

    const elapsedSinceTarget = status.minutesOfDay - status.targetMinutes;
    const slotIdx =
      status.repeatMinutes > 0
        ? Math.floor(elapsedSinceTarget / status.repeatMinutes)
        : 0;
    const slotKey = `${status.todayStr}#slot-${slotIdx}`;
    if (qcReminderLog.sentSlots.includes(slotKey)) {
      return;
    }

    await dispatchQcDailyReminder({
      slotKey,
      triggeredBy: `Lịch tự động (${status.reminderTime})`,
    });
  } catch (err) {
    console.error('Scheduled QC reminder check error:', err);
  } finally {
    isRunningScheduledReminder = false;
  }
}

function broadcastWsEvent(eventName: string, data: unknown) {
  if (wsClients.size === 0) return;
  const serialized = JSON.stringify({ event: eventName, data });
  for (const ws of wsClients) {
    try {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(serialized);
      } else if (ws.readyState === WebSocket.CLOSING || ws.readyState === WebSocket.CLOSED) {
        wsClients.delete(ws);
      }
    } catch {
      wsClients.delete(ws);
    }
  }
}

function broadcastSseEvent(eventName: string, data: unknown) {
  const payload = `event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const client of sseClients) {
    try {
      client.write(payload);
    } catch {
      sseClients.delete(client);
    }
  }
  broadcastWsEvent(eventName, data);
}

async function sendWebPushToSubscriptions(
  targets: StoredPushSubscription[],
  notificationPayload: Record<string, any>
): Promise<{ sentCount: number; failedCount: number; prunedEndpoints: string[]; lastError?: string }> {
  if (targets.length === 0) {
    return { sentCount: 0, failedCount: 0, prunedEndpoints: [] };
  }
  const jsonPayload = JSON.stringify(notificationPayload);
  const prunedEndpoints: string[] = [];
  let sentCount = 0;
  let failedCount = 0;
  let lastError = '';

  await Promise.allSettled(
    targets.map(async (sub) => {
      try {
        await webpush.sendNotification(sub.subscription, jsonPayload, {
          TTL: 86400,
          urgency: 'high',
        });
        sentCount++;
      } catch (err: any) {
        failedCount++;
        const statusCode = Number(err?.statusCode || 0);
        const detail = String(err?.body || err?.message || '').trim();
        lastError = statusCode ? `HTTP ${statusCode}${detail ? `: ${detail}` : ''}` : detail || 'Push delivery failed';
        if (statusCode === 404 || statusCode === 410 || statusCode === 401 || statusCode === 403) {
          prunedEndpoints.push(sub.endpoint);
        }
      }
    })
  );

  if (prunedEndpoints.length > 0) {
    const pruneSet = new Set(prunedEndpoints);
    pushSubscriptions = pushSubscriptions.filter((s) => !pruneSet.has(s.endpoint));
    savePushSubscriptions(pushSubscriptions);
  }

  return { sentCount, failedCount, prunedEndpoints, ...(lastError ? { lastError } : {}) };
}

function getSubscriptionRoleCounts() {
  const counts = { QA: 0, CBQL: 0, THU_KHO: 0, QC: 0, ADMIN: 0 };
  pushSubscriptions.forEach((s) => {
    if (counts[s.normalizedRole] !== undefined) {
      counts[s.normalizedRole]++;
    }
  });
  return counts;
}

function resolveManagerPushRole(managerRoleLabel?: string, areaId?: string, stageId?: string): {
  label: string;
  pushRole: 'CBQL' | 'THU_KHO';
} {
  const normLabel = normAccentFreeUpper(managerRoleLabel);
  if (
    normLabel.includes('THU KHO') ||
    normLabel.includes('WAREHOUSE') ||
    areaId === 'dau_vao' ||
    ['VatTu', 'VTGiaCong', 'VTTheuGiaCong', 'BanTrai', 'Tem', 'DeGiaCong'].includes(String(stageId || ''))
  ) {
    return { label: 'Thủ kho', pushRole: 'THU_KHO' };
  }
  return { label: 'CBQL', pushRole: 'CBQL' };
}

function computeReportEodStatus(
  report: Record<string, any>,
  managerRoleLabel: string
): {
  eodStatus: string;
  isCompleted: boolean;
  missingRoles: Array<'QA' | 'CBQL' | 'THU_KHO' | 'QC'>;
  missingLabels: string[];
} {
  const hasQc = Boolean(report.qcKiemTraSign || report.qcSignedAt);
  const hasQa = Boolean(report.qaPhuTrachSign || report.qaSignedAt);
  const hasMgr = Boolean(report.cbqlXacNhanSign || report.cbqlSignedAt);
  const mgrPushRole = managerRoleLabel === 'Thủ kho' ? 'THU_KHO' : 'CBQL';

  const missingRoles: Array<'QA' | 'CBQL' | 'THU_KHO' | 'QC'> = [];
  const missingLabels: string[] = [];
  if (!hasQa) {
    missingRoles.push('QA');
    missingLabels.push('QA');
  }
  if (!hasMgr) {
    missingRoles.push(mgrPushRole);
    missingLabels.push(managerRoleLabel);
  }
  if (!hasQc) {
    missingRoles.push('QC');
    missingLabels.push('QC');
  }

  if (hasQc && hasQa && hasMgr) {
    return {
      eodStatus: 'Đã hoàn tất xác nhận',
      isCompleted: true,
      missingRoles: [],
      missingLabels: [],
    };
  }

  if (!hasQa && !hasMgr) {
    return {
      eodStatus: managerRoleLabel === 'Thủ kho' ? 'Chờ QA & Thủ kho xác nhận' : 'Chờ QA & CBQL xác nhận',
      isCompleted: false,
      missingRoles,
      missingLabels,
    };
  }
  if (!hasQa) {
    return {
      eodStatus: 'Chờ QA xác nhận',
      isCompleted: false,
      missingRoles,
      missingLabels,
    };
  }
  if (!hasMgr) {
    return {
      eodStatus: managerRoleLabel === 'Thủ kho' ? 'Chờ Thủ kho xác nhận' : 'Chờ CBQL xác nhận',
      isCompleted: false,
      missingRoles,
      missingLabels,
    };
  }
  return {
    eodStatus: 'Chờ QC ký cuối ngày',
    isCompleted: false,
    missingRoles,
    missingLabels,
  };
}

const SHARED_REPORTS_FILE = '/tmp/rft_shared_reports.json';
const DELETED_REPORT_IDS_FILE = '/tmp/rft_deleted_report_ids.json';
const APP_REPORTS_SYNC_TAB = 'App_Bao_Cao_Sync';
const SHEET_CELL_CHUNK_LIMIT = 40000;

interface ServerStageDataTarget {
  stageId: string;
  congDoanTen: string;
  defaultArea: string;
  fileName: string;
  spreadsheetId: string;
}

const SERVER_RFT_DATA_TARGETS: ServerStageDataTarget[] = [
  { stageId: 'May', congDoanTen: 'May', defaultArea: 'may', fileName: 'DATA MAY', spreadsheetId: '1MNRV3G8xdZAoIlr-crwrAMNOl-y0DhjKgl1fYcyov5g' },
  { stageId: 'Go', congDoanTen: 'Gò', defaultArea: 'go', fileName: 'DATA GÒ', spreadsheetId: '1Wg68-bYAAM7LHVh6Ob6xkAwPpqllSavd3dPnkg53GJ4' },
  { stageId: 'VatTu', congDoanTen: 'Kiểm Vật Tư (NPL)', defaultArea: 'dau_vao', fileName: 'DATA VẬT TƯ', spreadsheetId: '1uA9ksZuFdtTeRxIU-85auhqeCo2n2lfCORKkV41EOUM' },
  { stageId: 'BanTrai', congDoanTen: 'Kiểm Bàn Trải', defaultArea: 'dau_vao', fileName: 'DATA BÀN TRẢI', spreadsheetId: '1lLSXxYeebfHrrLq74ftECRizKiDOplSq84WYtveebh8' },
  { stageId: 'VTGiaCong', congDoanTen: 'VT Gia Công & Hàng Gia Công', defaultArea: 'dau_vao', fileName: 'DATA VT GIA CÔNG', spreadsheetId: '1n-591t1yQUENKJ4ZOkQ-VSSVk10ThUX1MsAwZ3z2DaI' },
  { stageId: 'Tem', congDoanTen: 'Kiểm Tem', defaultArea: 'dau_vao', fileName: 'DATA TEM', spreadsheetId: '1HfYNSDmnKyzXEN3fgdYbxLYI47PDah0yN4E_0b9pXF8' },
  { stageId: 'DeGiaCong', congDoanTen: 'Đế Gia Công', defaultArea: 'dau_vao', fileName: 'DATA ĐẾ GIA CÔNG', spreadsheetId: '1SixpSY6GF7Ct3ymj0S8NuLqazHL45JcCjk1z6wrrM8w' },
  { stageId: 'DeBTP', congDoanTen: 'Đế BTP', defaultArea: 'dau_vao', fileName: 'DATA ĐẾ BTP', spreadsheetId: '1_uqO127Nx7bdxzjYmmRSS5YBs7enCBhRr5fOIwDb6I4' },
  { stageId: 'BTP_DauVao', congDoanTen: 'Kiểm BTP Đầu Vào', defaultArea: 'dau_vao', fileName: 'DATA BTP', spreadsheetId: '1fnctOYk_3myANzSQbGIVHTvlZPtmsdlZKi-IJJIL_Xo' },
  { stageId: 'PhunHat', congDoanTen: 'Phun Hạt', defaultArea: 'hoan_thien_de', fileName: 'DATA PHUN HẠT', spreadsheetId: '1oeYwAfLer_XshylT1pqO1fDPWh9a3Ezcx2t4dTuM-A8' },
  { stageId: 'SonDanLogo', congDoanTen: 'Sơn & Dán Logo', defaultArea: 'hoan_thien_de', fileName: 'DATA SƠN & DÁN LOGO', spreadsheetId: '1tYt5IfoaoX2WOcAiDtNP8qoc4izUVrVibeYw2Wm9w0s' },
  { stageId: 'DeHoanThien', congDoanTen: 'Đế Hoàn Thiện', defaultArea: 'hoan_thien_de', fileName: 'DATA ĐẾ', spreadsheetId: '10mEL_VT15YAuOmgMLtQsTPudIShpfXVzEEwjp4EYx_Y' },
];

function loadSharedReportsFromDisk(): Array<Record<string, any>> {
  try {
    if (fs.existsSync(SHARED_REPORTS_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(SHARED_REPORTS_FILE, 'utf8'));
      if (Array.isArray(parsed)) return parsed;
    }
  } catch {
    // ignore
  }
  return [];
}

function saveSharedReportsToDisk(list: Array<Record<string, any>>) {
  try {
    fs.writeFileSync(SHARED_REPORTS_FILE, JSON.stringify(list.slice(0, 2000), null, 2), 'utf8');
  } catch {
    // ignore
  }
}

function loadDeletedReportIds(): string[] {
  try {
    if (fs.existsSync(DELETED_REPORT_IDS_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(DELETED_REPORT_IDS_FILE, 'utf8'));
      if (Array.isArray(parsed)) return parsed.map((id) => String(id)).slice(0, 300);
    }
  } catch {
    // ignore
  }
  return [];
}

function saveDeletedReportIds(ids: string[]) {
  try {
    fs.writeFileSync(DELETED_REPORT_IDS_FILE, JSON.stringify(ids.slice(0, 300), null, 2), 'utf8');
  } catch {
    // ignore
  }
}

let sharedReportsStore: Array<Record<string, any>> = loadSharedReportsFromDisk();
let deletedReportIdsStore: string[] = loadDeletedReportIds();
const lastPersistedReportHashes = new Map<string, string>();
let lastAppSyncSheetHydrationAt = 0;
let lastDataSheetsHydrationAt = 0;
let hasHydratedDataSheetsFromSheets = false;
let reportsHydrationInFlight: Promise<void> | null = null;

function scoreServerReportRichness(r: Record<string, any>): number {
  let score = 0;
  score += (Array.isArray(r.editHistory) ? r.editHistory.length : 0) * 100;
  const activeDefectsCount = Array.isArray(r.defects)
    ? r.defects.filter((d: any) => Number(d?.quantity || 0) > 0).length
    : 0;
  score += activeDefectsCount * 10;
  if (Array.isArray(r.materialEntries) && r.materialEntries.length > 0) score += 25;
  if (r.qcKiemTraSign) score += 15;
  if (r.qaPhuTrachSign) score += 15;
  if (r.cbqlXacNhanSign || r.toTruongCuoiNgaySign || r.toTruongXacNhanGioSign) score += 15;
  if (r.eodStatus === 'Đã hoàn tất xác nhận') score += 30;
  if (r.syncStatus === 'synced') score += 5;
  if (!r.__fromSheetSummary) score += 20;
  return score;
}

function mergeServerReportRecords(
  existing: Record<string, any>,
  incoming: Record<string, any>
): Record<string, any> {
  const existingEdits = Array.isArray(existing.editHistory) ? existing.editHistory.length : 0;
  const incomingEdits = Array.isArray(incoming.editHistory) ? incoming.editHistory.length : 0;
  const existingScore = scoreServerReportRichness(existing);
  const incomingScore = scoreServerReportRichness(incoming);
  const preferIncoming =
    incomingEdits !== existingEdits
      ? incomingEdits > existingEdits
      : incomingScore >= existingScore;
  const primary = preferIncoming ? incoming : existing;
  const secondary = preferIncoming ? existing : incoming;

  const primaryDefects = Array.isArray(primary.defects) ? primary.defects : [];
  const secondaryDefects = Array.isArray(secondary.defects) ? secondary.defects : [];
  const bestDefects =
    primaryDefects.some((d: any) => Number(d?.quantity || 0) > 0) || primaryDefects.length >= secondaryDefects.length
      ? primaryDefects
      : secondaryDefects;

  const primaryMats = Array.isArray(primary.materialEntries) ? primary.materialEntries : [];
  const secondaryMats = Array.isArray(secondary.materialEntries) ? secondary.materialEntries : [];
  const bestMats = primaryMats.length >= secondaryMats.length ? primaryMats : secondaryMats;

  const primaryHist = Array.isArray(primary.editHistory) ? primary.editHistory : [];
  const secondaryHist = Array.isArray(secondary.editHistory) ? secondary.editHistory : [];
  const bestHist = primaryHist.length >= secondaryHist.length ? primaryHist : secondaryHist;

  const merged: Record<string, any> = {
    ...secondary,
    ...primary,
    ...(bestDefects.length > 0 ? { defects: bestDefects } : {}),
    ...(bestMats.length > 0 ? { materialEntries: bestMats } : {}),
    ...(bestHist.length > 0 ? { editHistory: bestHist } : {}),
    qcKiemTra: primary.qcKiemTra || secondary.qcKiemTra,
    qcKiemTraSign: primary.qcKiemTraSign || secondary.qcKiemTraSign,
    qcSignedAt: primary.qcSignedAt || secondary.qcSignedAt,
    qaPhuTrach: primary.qaPhuTrach || secondary.qaPhuTrach,
    qaPhuTrachSign: primary.qaPhuTrachSign || secondary.qaPhuTrachSign,
    qaSignedAt: primary.qaSignedAt || secondary.qaSignedAt,
    cbqlXacNhan: primary.cbqlXacNhan || secondary.cbqlXacNhan,
    cbqlXacNhanSign: primary.cbqlXacNhanSign || secondary.cbqlXacNhanSign,
    cbqlSignedAt: primary.cbqlSignedAt || secondary.cbqlSignedAt,
    toTruongXacNhanGio: primary.toTruongXacNhanGio || secondary.toTruongXacNhanGio,
    toTruongXacNhanGioSign: primary.toTruongXacNhanGioSign || secondary.toTruongXacNhanGioSign,
    toTruongCuoiNgay: primary.toTruongCuoiNgay || secondary.toTruongCuoiNgay,
    toTruongCuoiNgaySign: primary.toTruongCuoiNgaySign || secondary.toTruongCuoiNgaySign,
    syncStatus:
      primary.syncStatus === 'synced' ||
      (incomingEdits === existingEdits && secondary.syncStatus === 'synced')
        ? 'synced'
        : primary.syncStatus,
  };
  delete merged.__fromSheetSummary;
  return merged;
}

function upsertSharedReports(
  items: Array<Record<string, any>>,
  options?: { allowResurrect?: boolean }
): Array<Record<string, any>> {
  const deletedSet = new Set(deletedReportIdsStore);
  const upserted: Array<Record<string, any>> = [];
  for (const rawItem of items) {
    if (!rawItem || !rawItem.id) continue;
    const id = String(rawItem.id).trim();
    if (!id) continue;
    if (deletedSet.has(id)) {
      if (!options?.allowResurrect) continue;
      deletedSet.delete(id);
      deletedReportIdsStore = deletedReportIdsStore.filter((d) => d !== id);
      saveDeletedReportIds(deletedReportIdsStore);
    }

    const idx = sharedReportsStore.findIndex((r) => r.id === id);
    if (idx >= 0) {
      const merged = mergeServerReportRecords(sharedReportsStore[idx], rawItem);
      sharedReportsStore[idx] = merged;
      upserted.push(merged);
    } else {
      const clean = { ...rawItem };
      delete clean.__fromSheetSummary;
      sharedReportsStore.unshift(clean);
      upserted.push(clean);
    }
  }
  sharedReportsStore.sort((a, b) => {
    const ka = `${a?.dateStr || ''}|${a?.submittedAt || a?.timestamp || ''}|${a?.id || ''}`;
    const kb = `${b?.dateStr || ''}|${b?.submittedAt || b?.timestamp || ''}|${b?.id || ''}`;
    return kb.localeCompare(ka);
  });
  if (sharedReportsStore.length > 2000) {
    sharedReportsStore = sharedReportsStore.slice(0, 2000);
  }
  if (upserted.length > 0) {
    saveSharedReportsToDisk(sharedReportsStore);
  }
  return upserted;
}

function buildCompactReportPayloadForSheet(report: Record<string, any>): string {
  const clone: Record<string, any> = { ...report };
  delete clone.__fromSheetSummary;

  if (clone.toTruongCuoiNgaySign && clone.toTruongCuoiNgaySign === clone.cbqlXacNhanSign) {
    clone.toTruongCuoiNgaySign = '__DUP_CBQL__';
  }
  if (clone.toTruongXacNhanGioSign && clone.toTruongXacNhanGioSign === clone.cbqlXacNhanSign) {
    clone.toTruongXacNhanGioSign = '__DUP_CBQL__';
  }
  // Drop zero-quantity defects to keep backup compact while keeping every active defect intact
  if (Array.isArray(clone.defects)) {
    const activeOnly = clone.defects.filter((d: any) => Number(d?.quantity || 0) > 0);
    if (activeOnly.length < clone.defects.length) {
      clone.defects = activeOnly;
    }
  }

  let json = JSON.stringify(clone);
  if (json.length <= SHEET_CELL_CHUNK_LIMIT * 3) {
    return json;
  }

  // Fallback if 3 PNG signatures exceed 120,000 chars: strip heavy data URLs while keeping timestamps & signer names
  const trimmed = { ...clone };
  for (const key of [
    'qcKiemTraSign',
    'qaPhuTrachSign',
    'cbqlXacNhanSign',
    'toTruongXacNhanGioSign',
    'toTruongCuoiNgaySign',
  ]) {
    if (typeof trimmed[key] === 'string' && trimmed[key].startsWith('data:image/')) {
      delete trimmed[key];
    }
  }
  json = JSON.stringify(trimmed);
  if (json.length <= SHEET_CELL_CHUNK_LIMIT * 3) {
    return json;
  }
  // Ultra-compact fallback
  return JSON.stringify({
    ...trimmed,
    defects: Array.isArray(trimmed.defects) ? trimmed.defects.slice(0, 30) : [],
    materialEntries: Array.isArray(trimmed.materialEntries) ? trimmed.materialEntries.slice(0, 30) : undefined,
  });
}

function expandHydratedSheetReport(raw: Record<string, any>): Record<string, any> {
  const out = { ...raw };
  if (out.toTruongCuoiNgaySign === '__DUP_CBQL__' && out.cbqlXacNhanSign) {
    out.toTruongCuoiNgaySign = out.cbqlXacNhanSign;
  }
  if (out.toTruongXacNhanGioSign === '__DUP_CBQL__' && out.cbqlXacNhanSign) {
    out.toTruongXacNhanGioSign = out.cbqlXacNhanSign;
  }
  return out;
}

async function persistReportActionsToMasterSheet(
  entries: Array<{ reportId: string; dateStr: string; action: 'UPSERT' | 'DELETE'; report?: Record<string, any> }>
): Promise<boolean> {
  await ensureServerConfigHydrated();
  const gasUrl = resolveValidGasUrl();
  if (!gasUrl || entries.length === 0) return false;

  const rowsToWrite: Array<Record<string, string>> = [];
  const pendingHashes: Array<{ key: string; hash: string }> = [];
  const nowIso = new Date().toISOString();

  for (const entry of entries) {
    const id = String(entry.reportId || '').trim();
    if (!id) continue;

    let payloadStr = '';
    if (entry.action === 'UPSERT' && entry.report) {
      payloadStr = buildCompactReportPayloadForSheet(entry.report);
    }
    const contentHash = crypto
      .createHash('sha1')
      .update(`${entry.action}|${payloadStr}`)
      .digest('hex');
    const trackKey = `${id}`;
    if (lastPersistedReportHashes.get(trackKey) === contentHash) {
      continue;
    }

    const p1 = payloadStr.slice(0, SHEET_CELL_CHUNK_LIMIT);
    const p2 = payloadStr.slice(SHEET_CELL_CHUNK_LIMIT, SHEET_CELL_CHUNK_LIMIT * 2);
    const p3 = payloadStr.slice(SHEET_CELL_CHUNK_LIMIT * 2, SHEET_CELL_CHUNK_LIMIT * 3);

    rowsToWrite.push({
      REPORT_ID: id,
      DATE_STR: String(entry.dateStr || '').trim(),
      UPDATED_AT: nowIso,
      ACTION: entry.action,
      PAYLOAD_1: p1,
      PAYLOAD_2: p2,
      PAYLOAD_3: p3,
    });
    pendingHashes.push({ key: trackKey, hash: contentHash });
  }

  if (rowsToWrite.length === 0) return true;

  let allOk = true;
  for (let i = 0; i < rowsToWrite.length; i += 8) {
    const chunk = rowsToWrite.slice(i, i + 8);
    const chunkHashes = pendingHashes.slice(i, i + 8);
    let chunkOk = false;
    for (let attempt = 0; attempt < 2 && !chunkOk; attempt++) {
      try {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), 15000);
        const res = await fetch(gasUrl, {
          method: 'POST',
          redirect: 'follow',
          signal: controller.signal,
          headers: { 'Content-Type': 'text/plain;charset=utf-8' },
          body: JSON.stringify({
            action: 'submitReport',
            outputUrl: RFT_MASTER_SHEET_URL,
            outputSheetName: APP_REPORTS_SYNC_TAB,
            headers: ['REPORT_ID', 'DATE_STR', 'UPDATED_AT', 'ACTION', 'PAYLOAD_1', 'PAYLOAD_2', 'PAYLOAD_3'],
            rows: chunk,
          }),
        }).finally(() => clearTimeout(timer));
        const txt = await res.text().catch(() => '');
        if (res.ok && !txt.trim().startsWith('<!DOCTYPE html') && !txt.includes('"status":"error"')) {
          chunkHashes.forEach((h) => lastPersistedReportHashes.set(h.key, h.hash));
          chunkOk = true;
        }
      } catch {
        // retry once after short backoff
      }
      if (!chunkOk && attempt === 0) {
        await new Promise((r) => setTimeout(r, 400));
      }
    }
    if (!chunkOk) {
      allOk = false;
    }
  }
  return allOk;
}

async function persistHourlyTasksToMasterSheet(tasks: StoredHourlyCbqlSignTask[]): Promise<boolean> {
  await ensureServerConfigHydrated();
  const gasUrl = resolveValidGasUrl();
  if (!gasUrl || tasks.length === 0) return false;

  const rowsToWrite: Array<Record<string, string>> = [];
  const pendingHashes: Array<{ key: string; hash: string }> = [];
  const nowIso = new Date().toISOString();

  for (const task of tasks) {
    if (!task?.id) continue;
    const journalId = `__HOURLY_TASK__::${task.id}`;
    const payloadStr = JSON.stringify(task);
    const contentHash = crypto.createHash('sha1').update(`HOURLY_TASK|${payloadStr}`).digest('hex');
    if (lastPersistedReportHashes.get(journalId) === contentHash) continue;

    rowsToWrite.push({
      REPORT_ID: journalId,
      DATE_STR: String(task.dateStr || '').trim(),
      UPDATED_AT: nowIso,
      ACTION: 'HOURLY_TASK',
      PAYLOAD_1: payloadStr.slice(0, SHEET_CELL_CHUNK_LIMIT),
      PAYLOAD_2: payloadStr.slice(SHEET_CELL_CHUNK_LIMIT, SHEET_CELL_CHUNK_LIMIT * 2),
      PAYLOAD_3: payloadStr.slice(SHEET_CELL_CHUNK_LIMIT * 2, SHEET_CELL_CHUNK_LIMIT * 3),
    });
    pendingHashes.push({ key: journalId, hash: contentHash });
  }

  if (rowsToWrite.length === 0) return true;

  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 15000);
    const res = await fetch(gasUrl, {
      method: 'POST',
      redirect: 'follow',
      signal: controller.signal,
      headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: JSON.stringify({
        action: 'submitReport',
        outputUrl: RFT_MASTER_SHEET_URL,
        outputSheetName: APP_REPORTS_SYNC_TAB,
        headers: ['REPORT_ID', 'DATE_STR', 'UPDATED_AT', 'ACTION', 'PAYLOAD_1', 'PAYLOAD_2', 'PAYLOAD_3'],
        rows: rowsToWrite,
      }),
    }).finally(() => clearTimeout(timer));
    const txt = await res.text().catch(() => '');
    if (res.ok && !txt.trim().startsWith('<!DOCTYPE html') && !txt.includes('"status":"error"')) {
      pendingHashes.forEach((h) => lastPersistedReportHashes.set(h.key, h.hash));
      return true;
    }
  } catch {
    // ignore
  }
  return false;
}

function normalizeSheetDateStr(raw: unknown): string {
  const text = String(raw || '').trim();
  if (!text) return '';
  const gvizMatch = text.match(/Date\((\d{4}),\s*(\d{1,2}),\s*(\d{1,2})/);
  if (gvizMatch) {
    const y = gvizMatch[1];
    const m = String(Number(gvizMatch[2]) + 1).padStart(2, '0');
    const d = String(Number(gvizMatch[3])).padStart(2, '0');
    return `${y}-${m}-${d}`;
  }
  const isoMatch = text.match(/(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (isoMatch) {
    return `${isoMatch[1]}-${isoMatch[2].padStart(2, '0')}-${isoMatch[3].padStart(2, '0')}`;
  }
  const vnMatch = text.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (vnMatch) {
    return `${vnMatch[3]}-${vnMatch[2].padStart(2, '0')}-${vnMatch[1].padStart(2, '0')}`;
  }
  const compactMatch = text.match(/-(20\d{2})(\d{2})(\d{2})-/);
  if (compactMatch) {
    return `${compactMatch[1]}-${compactMatch[2]}-${compactMatch[3]}`;
  }
  return '';
}

function roundServerQty(val: unknown, decimals = 2): number {
  const num = typeof val === 'number' ? val : Number(val);
  if (!Number.isFinite(num)) return 0;
  const factor = Math.pow(10, decimals);
  const eps = Math.sign(num) * Number.EPSILON * Math.max(1, Math.abs(num));
  const rounded = Math.round((num + eps) * factor) / factor;
  return Object.is(rounded, -0) ? 0 : rounded;
}

function parseSheetNumber(raw: unknown): number {
  if (typeof raw === 'number' && Number.isFinite(raw)) return raw;
  const clean = String(raw || '').replace(/%/g, '').replace(/,/g, '.').replace(/\s+/g, '').trim();
  if (!clean) return 0;
  const num = Number(clean);
  return Number.isFinite(num) ? num : 0;
}

function parseDefectSummaryTextToItems(summaryText: string): Array<Record<string, any>> {
  const raw = String(summaryText || '').trim();
  if (!raw || raw.toLowerCase() === 'không có lỗi' || raw.toLowerCase() === 'khong co loi') {
    return [];
  }
  const parts = raw.split(/\s*;\s*/).filter(Boolean);
  const items: Array<Record<string, any>> = [];
  parts.forEach((part, idx) => {
    const m = part.match(/^(.*?)\s*\((\d+(?:[.,]\d+)?)\)(?:\s*\[MSK:\s*(.*?)\])?$/i);
    if (m) {
      const name = m[1].trim();
      const qty = parseSheetNumber(m[2]);
      const msk = m[3] ? m[3].trim() : undefined;
      if (name && qty > 0) {
        items.push({
          id: 9000 + idx,
          name,
          kiemSoat: 'Ngoại quan',
          capDo: 'Nặng',
          quantity: qty,
          ...(msk ? { maSoKiem: msk } : {}),
        });
      }
    }
  });
  return items;
}

function findServerLocationMeta(stationOrLine: string): FactoryLocationEntry | undefined {
  const key = compareAuthNameKey(stationOrLine);
  if (!key) return undefined;
  return FACTORY_MASTER_DATA.find(
    (entry) =>
      compareAuthNameKey(entry.viTri) === key ||
      compareAuthNameKey(entry.line) === key
  );
}

function mapDataSheetRowsToReports(
  target: ServerStageDataTarget,
  rows: Array<Record<string, any>>
): Array<Record<string, any>> {
  const grouped = new Map<string, Array<Record<string, any>>>();

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    if (!row || typeof row !== 'object') continue;
    const sheetNameMeta = String(row._sheetName || '').trim();
    if (sheetNameMeta === '_Signature_Cache') continue;

    const idCell = getServerRowCell(row, ['ID_lỗi', 'ID lỗi', 'ID_loi', 'ID loi', 'Mã báo cáo', 'Ma bao cao'], -1);
    const dateCell = getServerRowCell(row, ['Ngày kiểm', 'Ngay kiem'], -1);
    const dateStr =
      normalizeSheetDateStr(dateCell) ||
      normalizeSheetDateStr(getServerRowCell(row, ['Thời gian', 'Thoi gian', 'Dấu thời gian'], -1)) ||
      normalizeSheetDateStr(idCell);
    const chuyen =
      getServerRowCell(row, ['Chuyền', 'Vị trí kiểm', 'Vi tri kiem', 'Chuyen'], -1) ||
      getServerRowCell(row, ['Line sản xuất', 'Line san xuat', 'Line'], -1);

    if (!idCell && (!dateStr || !chuyen)) continue;
    if (normAccentFreeUpper(idCell) === 'ID LOI' || normAccentFreeUpper(idCell) === 'MA BAO CAO') continue;

    const gioKiem = getServerRowCell(row, ['Giờ kiểm', 'Gio kiem', 'Hình thức / Lô kiểm', 'Hinh thuc / Lo kiem'], -1) || '7h30 - 8h30';
    const fallbackId = `SHEET-${target.stageId}-${dateStr || 'UNK'}-${gioKiem.replace(/\s+/g, '')}-${chuyen.replace(/\s+/g, '') || i}`;
    const reportId = idCell || fallbackId;
    const existingGroup = grouped.get(reportId);
    if (existingGroup) {
      existingGroup.push(row);
    } else {
      grouped.set(reportId, [row]);
    }
  }

  const results: Array<Record<string, any>> = [];
  grouped.forEach((groupRows, reportId) => {
    const first = groupRows[0];
    const timestampRaw = getServerRowCell(first, ['Thời gian', 'Thoi gian', 'Dấu thời gian'], -1);
    const dateStr =
      normalizeSheetDateStr(getServerRowCell(first, ['Ngày kiểm', 'Ngay kiem'], -1)) ||
      normalizeSheetDateStr(timestampRaw) ||
      normalizeSheetDateStr(reportId);
    const gioKiem = getServerRowCell(first, ['Giờ kiểm', 'Gio kiem', 'Hình thức / Lô kiểm'], -1) || '7h30 - 8h30';
    const rawLineCell = getServerRowCell(first, ['Line sản xuất', 'Line san xuat', 'Line'], -1);
    const rawChuyenCell = getServerRowCell(first, ['Chuyền', 'Vị trí kiểm', 'Vi tri kiem'], -1);
    const rawFactoryCell = getServerRowCell(first, ['Nhà máy', 'Nha may'], -1);
    let chuyen = rawChuyenCell || rawLineCell;
    let locMeta = findServerLocationMeta(chuyen) || findServerLocationMeta(rawLineCell);
    if (!locMeta && rawFactoryCell && target.defaultArea === 'dau_vao') {
      const fKey = compareAuthNameKey(rawFactoryCell);
      locMeta = FACTORY_MASTER_DATA.find(
        (entry) =>
          compareAuthNameKey(entry.nhaMay) === fKey &&
          normAccentFreeUpper(entry.line).includes('DAU VAO')
      );
    }
    const nhaMay = rawFactoryCell || locMeta?.nhaMay || '';
    const lineSanXuat = rawLineCell || locMeta?.line || '';
    if (!chuyen) {
      chuyen = locMeta?.viTri || locMeta?.line || (nhaMay ? `${target.congDoanTen} - ${nhaMay}` : target.congDoanTen);
    }
    const khuVuc = getServerRowCell(first, ['Khu vực', 'Khu vuc'], -1) || target.defaultArea;
    const congDoanId = reportId.startsWith('QC-DeBTP-') ? 'DeBTP' : target.stageId;
    const congDoanTen = getServerRowCell(first, ['Công đoạn', 'Cong doan'], -1) || target.congDoanTen;

    const maTongHop = getServerRowCell(first, ['Mã giày/Mã đế/SAP', 'Mã tổng hợp', 'Ma tong hop'], -1);
    const maGiayRaw = getServerRowCell(first, ['Mã giày', 'Ma giay'], -1);
    const maDeRaw = getServerRowCell(first, ['Mã đế', 'Ma de'], -1);
    const maSap = getServerRowCell(first, ['Mã SAP/Code/LOT', 'Mã SAP', 'Ma SAP'], -1);
    const isSoleStage = ['PhunHat', 'SonDanLogo', 'DeHoanThien', 'DeGiaCong', 'DeBTP'].includes(congDoanId);
    const maGiay = maGiayRaw || (!isSoleStage ? maTongHop : '');
    const maDe = maDeRaw || (isSoleStage ? maTongHop : '');
    const maHang = maTongHop || [maGiay, maDe, maSap].filter(Boolean).join(' / ');
    const nhaCungCap = getServerRowCell(first, ['Nhà cung cấp', 'Nhà cung ứng', 'Nhà cung ứng / NCC', 'NCC'], -1);

    const tongSoLuongDoikiem = roundServerQty(
      parseSheetNumber(
        getServerRowCell(first, ['Tổng SL kiểm (Phiếu)', 'Tổng số lượng kiểm', 'Tổng số lượng đôi kiểm', 'Số lượng kiểm'], -1)
      )
    );
    const tongSoLuongLoiKhongDat = roundServerQty(
      parseSheetNumber(
        getServerRowCell(first, ['Tổng SL lỗi không đạt (Phiếu)', 'Tổng số lượng lỗi không đạt', 'Tổng lỗi', 'Số lượng lỗi'], -1)
      )
    );
    const tongSoLuongDoiKiemKhongDat = roundServerQty(
      parseSheetNumber(
        getServerRowCell(first, ['Tổng SL kiểm không đạt', 'Tổng số lượng đôi kiểm không đạt', 'Tổng số đôi lỗi'], -1)
      )
    );
    const rawDat = roundServerQty(
      parseSheetNumber(
        getServerRowCell(first, ['Số lượng đôi kiểm đạt', 'Tổng số lượng đôi kiểm đạt'], -1)
      )
    );
    const tongSoLuongDoiKiemDat =
      rawDat > 0 || tongSoLuongDoikiem === 0
        ? rawDat
        : roundServerQty(Math.max(0, tongSoLuongDoikiem - tongSoLuongDoiKiemKhongDat));

    const rawRft = parseSheetNumber(
      getServerRowCell(first, ['Tỷ lệ RFT (%)', 'Tỷ lệ RFT', '%RFT', 'RFT'], -1)
    );
    const rftPercent =
      rawRft > 0 || target.defaultArea === 'dau_vao'
        ? rawRft
        : tongSoLuongDoikiem > 0
        ? Number(((tongSoLuongDoiKiemDat / tongSoLuongDoikiem) * 100).toFixed(1))
        : 0;

    const submittedBy = getServerRowCell(first, ['Người nhập liệu', 'Nguoi nhap lieu'], -1);
    const submittedByRole = getServerRowCell(first, ['Vai trò người nhập', 'Vai tro nguoi nhap'], -1) || 'QC_User';
    const managerRoleLabel = getServerRowCell(first, ['Chức danh quản lý', 'Chuc danh quan ly'], -1) || (khuVuc === 'dau_vao' ? 'Thủ kho' : 'CBQL');
    const qcKiemTra = getServerRowCell(first, ['QC kiểm tra', 'QC kiem tra'], -1) || submittedBy;
    const qcSignedAt = getServerRowCell(first, ['Thời gian ký QC', 'Thoi gian ky QC'], -1);
    const qaPhuTrach = getServerRowCell(first, ['QA phụ trách', 'QLCL', 'QA phu trach'], -1);
    const qaSignedAt = getServerRowCell(first, ['Thời gian ký QA', 'Thoi gian ky QA'], -1);
    const cbqlXacNhan = getServerRowCell(first, ['CBQL / Thủ kho xác nhận', 'Thủ kho xác nhận', 'Tổ trưởng xác nhận', 'CBQL xác nhận'], -1);
    const cbqlSignedAt = getServerRowCell(first, ['Thời gian ký quản lý', 'Thời gian ký CBQL', 'Thời gian ký Thủ kho', 'Thời gian ký Tổ trưởng'], -1);
    const eodStatus = getServerRowCell(first, ['Trạng thái chốt ca', 'Trang thai chot ca'], -1) || 'Chờ QC hoàn tất';

    const defectTexts = groupRows
      .map((gr) => getServerRowCell(gr, ['Tên lỗi', 'Chi tiết lỗi', 'Tóm tắt lỗi chi tiết', 'Danh mục lỗi đính kèm'], -1))
      .filter((t) => t && t.toLowerCase() !== 'không có lỗi');
    const defectSummary = defectTexts.length > 0 ? defectTexts.join('; ') : 'Không có lỗi';
    const parsedDefects = parseDefectSummaryTextToItems(defectSummary);

    const isMaterial = ['VatTu', 'VTGiaCong', 'VTTheuGiaCong', 'BanTrai', 'Tem', 'DeGiaCong'].includes(congDoanId);
    const materialEntries: Array<Record<string, any>> = [];
    if (isMaterial) {
      groupRows.forEach((gr, idx) => {
        const tenVatTu = getServerRowCell(gr, ['Tên vật tư / Chủng loại vật tư, NPL', 'Tên vật tư', 'Chủng loại vật tư'], -1);
        const maSoVatTu = getServerRowCell(gr, ['Mã SAP/LOT', 'Mã số vật tư', 'Mã VT'], -1);
        const slKiem = roundServerQty(parseSheetNumber(getServerRowCell(gr, ['Số lượng kiểm', 'So luong kiem'], -1)));
        const slNhap = roundServerQty(parseSheetNumber(getServerRowCell(gr, ['Tổng số lượng nhập', 'Tổng SL nhập'], -1)));
        if (!tenVatTu && !maSoVatTu && slKiem <= 0 && slNhap <= 0) return;
        const lotCell = getServerRowCell(gr, ['Số LOT', 'LOT'], -1);
        const poCell = getServerRowCell(gr, ['Số PO', 'PO'], -1);
        const lotPoCell = getServerRowCell(gr, ['LOT / PO', 'LOT/PO', 'Số LOT & PO', 'LOT & PO'], -1);
        materialEntries.push({
          id: `mat-sheet-${reportId}-${idx}`,
          tenVatTu,
          maSoVatTu,
          lotPo: lotPoCell || [lotCell ? `LOT: ${lotCell}` : '', poCell ? `PO: ${poCell}` : ''].filter(Boolean).join(' | '),
          ...(lotCell ? { lotNumber: lotCell } : {}),
          ...(poCell ? { poNumber: poCell } : {}),
          mauSac: getServerRowCell(gr, ['13. Màu', 'Màu sắc', 'Màu', 'Color'], -1),
          donViTinh: getServerRowCell(gr, ['Đơn vị tính', 'ĐVT'], -1),
          tongSoLuongNhap: slNhap,
          soLuongKiem: slKiem,
          soLuongLoi: roundServerQty(parseSheetNumber(getServerRowCell(gr, ['Số lượng lỗi'], -1))),
          loiNgoaiQuan: parseSheetNumber(getServerRowCell(gr, ['Loại lỗi - Ngoại quan', 'Loại lỗi Ngoại quan', 'Ngoại quan'], -1)),
          loiQuyCach: parseSheetNumber(getServerRowCell(gr, ['Loại lỗi - Quy cách', 'Loại lỗi Quy cách', 'Quy cách'], -1)),
          capDoNang: parseSheetNumber(getServerRowCell(gr, ['Cấp độ - Nặng', 'Cấp độ Nặng', 'Nặng'], -1)),
          capDoNhe: parseSheetNumber(getServerRowCell(gr, ['Cấp độ - Nhẹ', 'Cấp độ Nhẹ', 'Nhẹ'], -1)),
          phanTramLoi: parseSheetNumber(getServerRowCell(gr, ['% LỖI', '%Lỗi', '% Lỗi', 'Phần trăm lỗi'], -1)),
          nhaCungCap: getServerRowCell(gr, ['Nhà cung cấp', 'Nhà cung ứng', 'Nhà cung ứng / NCC', 'NCC'], -1) || nhaCungCap,
          maSoKiem: getServerRowCell(gr, ['Mã số kiểm', 'Ma so kiem'], -1),
        });
      });
    }

    const qcSig = typeof first.qcKiemTraSign === 'string' && first.qcKiemTraSign.startsWith('data:image/') ? first.qcKiemTraSign : undefined;
    const qaSig = typeof first.qaPhuTrachSign === 'string' && first.qaPhuTrachSign.startsWith('data:image/') ? first.qaPhuTrachSign : undefined;
    const cbqlSig = typeof first.cbqlXacNhanSign === 'string' && first.cbqlXacNhanSign.startsWith('data:image/') ? first.cbqlXacNhanSign : undefined;

    const isPilot = normAccentFreeUpper(getServerRowCell(first, ['Đơn hàng Pilot?', 'Pilot'], -1)) === 'CO';
    const isImplant = normAccentFreeUpper(getServerRowCell(first, ['PO Implant?', 'Implant'], -1)) === 'CO';
    const isTheoDoiPilot4Tuan = normAccentFreeUpper(getServerRowCell(first, ['Theo dõi Pilot 4W?'], -1)) === 'CO';

    const submittedAtTime = timestampRaw.includes(' ')
      ? timestampRaw.split(' ').find((tok) => /^\d{1,2}:\d{2}(:\d{2})?$/.test(tok)) || ''
      : '';

    results.push({
      id: reportId,
      timestamp: timestampRaw || `${dateStr} ${submittedAtTime}`.trim(),
      dateStr,
      nhaMay,
      lineSanXuat,
      khuVuc,
      congDoan: congDoanId,
      congDoanTen,
      gioKiem,
      chuyen,
      maHang,
      maGiay,
      maDe,
      maSap,
      ...(nhaCungCap ? { nhaCungCap } : {}),
      isPilot,
      isImplant,
      isTheoDoiPilot4Tuan,
      tongSoLuongDoikiem,
      tongSoLuongLoiKhongDat,
      tongSoLuongDoiKiemKhongDat,
      tongSoLuongDoiKiemDat,
      rftPercent,
      rawRftPercent: rftPercent,
      ...(materialEntries.length > 0
        ? {
            materialEntries,
            materialSummary: {
              tongSoLuongNhap: roundServerQty(materialEntries.reduce((s, m) => s + Number(m.tongSoLuongNhap || 0), 0)),
              tongSoLuongKiem: roundServerQty(tongSoLuongDoikiem || materialEntries.reduce((s, m) => s + Number(m.soLuongKiem || 0), 0)),
              tongSoLuongLoiKhongDat,
              tongSoLuongKiemKhongDat: tongSoLuongDoiKiemKhongDat,
              phanTramLoi: tongSoLuongDoikiem > 0 ? Number(((tongSoLuongLoiKhongDat / tongSoLuongDoikiem) * 100).toFixed(2)) : 0,
            },
          }
        : {}),
      submittedBy: submittedBy || qcKiemTra,
      submittedByRole,
      managerRoleLabel,
      eodOnlyStage: khuVuc === 'dau_vao',
      outputTargetFile: target.fileName,
      submittedAt: submittedAtTime,
      editHistory: [
        {
          timestamp: timestampRaw || dateStr,
          user: submittedBy || qcKiemTra || 'Google Sheet',
          action: 'Đồng bộ từ Google Sheets',
          note: `Nguồn: ${target.fileName}`,
        },
      ],
      qcKiemTra,
      ...(qcSig ? { qcKiemTraSign: qcSig } : {}),
      ...(qcSignedAt ? { qcSignedAt } : {}),
      qaPhuTrach,
      ...(qaSig ? { qaPhuTrachSign: qaSig } : {}),
      ...(qaSignedAt ? { qaSignedAt } : {}),
      cbqlXacNhan,
      ...(cbqlSig ? { cbqlXacNhanSign: cbqlSig } : {}),
      ...(cbqlSignedAt ? { cbqlSignedAt } : {}),
      eodStatus,
      defects: parsedDefects,
      defectSummary,
      syncStatus: 'synced',
      __fromSheetSummary: true,
    });
  });

  return results;
}

async function fetchSingleSheetRowsWithTimeout(
  sheetUrl: string,
  tabName: string,
  gid: string,
  gasUrl?: string,
  timeoutMs = 7500
): Promise<Array<Record<string, any>>> {
  return Promise.race([
    fetchServerSheetRowsByTabOrGid(sheetUrl, tabName, gid, gasUrl),
    new Promise<Array<Record<string, any>>>((resolve) => setTimeout(() => resolve([]), timeoutMs)),
  ]);
}

function restorePendingEodFromHydratedReports() {
  const existingIds = new Set(pendingEodList.map((i) => i.id));
  let changed = false;

  for (const rep of sharedReportsStore) {
    if (!rep || !rep.id) continue;
    const eodSt = String(rep.eodStatus || '');
    const isPendingEod =
      eodSt.includes('Chờ QA') ||
      eodSt.includes('Chờ CBQL') ||
      eodSt.includes('Chờ Thủ kho') ||
      eodSt.includes('Chờ QC ký cuối ngày');
    if (!isPendingEod || existingIds.has(rep.id)) continue;

    const { label: managerRoleLabel } = resolveManagerPushRole(
      rep.managerRoleLabel,
      rep.khuVuc,
      rep.congDoan
    );
    const statusInfo = computeReportEodStatus(rep, managerRoleLabel);
    if (statusInfo.isCompleted) continue;

    const nowIso = new Date().toISOString();
    pendingEodList.push({
      id: rep.id,
      report: { ...rep, managerRoleLabel },
      managerRoleLabel,
      requiredRoles: ['QA', managerRoleLabel === 'Thủ kho' ? 'THU_KHO' : 'CBQL'],
      milestoneTime: rep.shiftEndMilestone || '16:30',
      createdAt: nowIso,
      updatedAt: nowIso,
      createdBy: rep.qcKiemTra || rep.submittedBy || 'QC',
      status: 'pending',
      pushSentCount: 0,
    });
    existingIds.add(rep.id);
    changed = true;
  }

  if (changed) {
    savePendingEodList(pendingEodList);
  }
}

async function ensureReportsHydratedFromSheets(
  force = false,
  options?: { broadcastDiffs?: boolean }
): Promise<void> {
  if (reportsHydrationInFlight) {
    await reportsHydrationInFlight;
    return;
  }

  const now = Date.now();
  const needAppSyncTab = force || now - lastAppSyncSheetHydrationAt >= 3_000;
  const needDataSheets =
    !hasHydratedDataSheetsFromSheets ||
    now - lastDataSheetsHydrationAt >= 120_000 ||
    (force && now - lastDataSheetsHydrationAt >= 10_000);
  if (!needAppSyncTab && !needDataSheets) return;

  reportsHydrationInFlight = (async () => {
    try {
      await ensureServerConfigHydrated();
      const gasUrl = resolveValidGasUrl();

      const beforeFingerprints = new Map<string, string>();
      if (options?.broadcastDiffs) {
        for (const r of sharedReportsStore) {
          if (r?.id) {
            beforeFingerprints.set(
              r.id,
              `${r.eodStatus || ''}|${(r.editHistory || []).length}|${r.syncStatus || ''}|${r.qcSignedAt || ''}|${r.qaSignedAt || ''}|${r.cbqlSignedAt || ''}`
            );
          }
        }
      }
      const newlyDeletedIds: string[] = [];

      // 1. Hydrate lossless JSON journal from Master Sheet > App_Bao_Cao_Sync
      if (needAppSyncTab) {
        lastAppSyncSheetHydrationAt = Date.now();
        const syncRows = await fetchSingleSheetRowsWithTimeout(
          RFT_MASTER_SHEET_URL,
          APP_REPORTS_SYNC_TAB,
          '',
          gasUrl || undefined,
          7500
        );

        if (syncRows.length > 0) {
          const validJournalRows = syncRows.filter((r) =>
            Object.keys(r).some((k) => k.trim().toUpperCase() === 'REPORT_ID')
          );
          if (validJournalRows.length > 0) {
            const latestById = new Map<string, Record<string, any>>();
            const deletedInJournal = new Set<string>();
            const hourlyTasksFromJournal = new Map<string, StoredHourlyCbqlSignTask>();
            let draftsHydratedChanged = false;
            let tempSlotsHydratedChanged = false;

            for (const jr of validJournalRows) {
              const repId = getServerRowCell(jr, ['REPORT_ID'], 0);
              if (!repId) continue;
              const action = getServerRowCell(jr, ['ACTION'], 3).toUpperCase();

              if (action === 'HOURLY_TASK' || repId.startsWith('__HOURLY_TASK__::')) {
                const p1 = getServerRowCell(jr, ['PAYLOAD_1'], 4);
                const p2 = getServerRowCell(jr, ['PAYLOAD_2'], 5);
                const p3 = getServerRowCell(jr, ['PAYLOAD_3'], 6);
                const combinedJson = `${p1}${p2}${p3}`.trim();
                if (combinedJson) {
                  try {
                    const parsedTask = JSON.parse(combinedJson) as StoredHourlyCbqlSignTask;
                    if (parsedTask && parsedTask.id) {
                      hourlyTasksFromJournal.set(parsedTask.id, parsedTask);
                    }
                  } catch {
                    // ignore malformed hourly task row
                  }
                }
                continue;
              }

              if (action === 'ACCOUNT_PRESENCE' || repId.startsWith('__PRESENCE__::')) {
                const presKey = normalizeServerUserKey(repId.replace(/^__PRESENCE__::/, ''));
                if (presKey && !isUnsafeObjectKey(presKey)) {
                  const p1 = getServerRowCell(jr, ['PAYLOAD_1'], 4);
                  const p2 = getServerRowCell(jr, ['PAYLOAD_2'], 5);
                  const p3 = getServerRowCell(jr, ['PAYLOAD_3'], 6);
                  const combinedJson = `${p1}${p2}${p3}`.trim();
                  if (combinedJson) {
                    try {
                      const parsedPres = JSON.parse(combinedJson) as StoredAccountPresence;
                      if (parsedPres && typeof parsedPres === 'object') {
                        const prev = accountPresenceStore[presKey];
                        const incMs = Number(parsedPres.lastSeenAtMs || Date.parse(parsedPres.lastSeenAt || '') || 0);
                        const curMs = Number(prev?.lastSeenAtMs || 0);
                        if (!prev || incMs > curMs) {
                          accountPresenceStore[presKey] = {
                            ...parsedPres,
                            userKey: presKey,
                            lastSeenAtMs: incMs,
                          };
                        }
                      }
                    } catch {
                      // ignore malformed presence row
                    }
                  }
                }
                continue;
              }

              if (
                action === 'USER_DRAFT' ||
                action === 'USER_DRAFT_DELETE' ||
                repId.startsWith('__USER_DRAFT__::')
              ) {
                const draftKey = repId.replace(/^__USER_DRAFT__::/, '').trim().toLowerCase();
                if (draftKey && !isUnsafeObjectKey(draftKey)) {
                  if (action === 'USER_DRAFT_DELETE') {
                    // Only clear if journal row is newer than local store
                    const rowUpdated = getServerRowCell(jr, ['UPDATED_AT'], 2);
                    const curMs = userDraftsStore[draftKey]?.updatedAt
                      ? Date.parse(String(userDraftsStore[draftKey].updatedAt))
                      : 0;
                    const delMs = rowUpdated ? Date.parse(rowUpdated) : 0;
                    if (!curMs || !delMs || delMs >= curMs) {
                      delete userDraftsStore[draftKey];
                      draftsHydratedChanged = true;
                    }
                  } else {
                    const p1 = getServerRowCell(jr, ['PAYLOAD_1'], 4);
                    const p2 = getServerRowCell(jr, ['PAYLOAD_2'], 5);
                    const p3 = getServerRowCell(jr, ['PAYLOAD_3'], 6);
                    const combinedJson = `${p1}${p2}${p3}`.trim();
                    if (combinedJson) {
                      try {
                        const parsedDraft = JSON.parse(combinedJson);
                        if (parsedDraft && typeof parsedDraft === 'object') {
                          const existing = userDraftsStore[draftKey];
                          const incMs = parsedDraft.updatedAt ? Date.parse(parsedDraft.updatedAt) : 0;
                          const curMs = existing?.updatedAt ? Date.parse(existing.updatedAt) : 0;
                          if (!existing || incMs > curMs || (draftHasRealServerMeasurements(parsedDraft) && !draftHasRealServerMeasurements(existing))) {
                            userDraftsStore[draftKey] = { ...parsedDraft, userKey: draftKey };
                            draftsHydratedChanged = true;
                          }
                        }
                      } catch {
                        // ignore malformed draft row
                      }
                    }
                  }
                }
                continue;
              }

              if (
                action === 'TEMP_SLOT' ||
                action === 'TEMP_SLOT_DELETE' ||
                repId.startsWith('__TEMP_SLOT__::')
              ) {
                const slotKey = repId.replace(/^__TEMP_SLOT__::/, '').trim();
                const todayDateStr = getVietnamClock().dateStr;
                if (slotKey && !isUnsafeObjectKey(slotKey)) {
                  if (action === 'TEMP_SLOT_DELETE') {
                    const rowUpdated = getServerRowCell(jr, ['UPDATED_AT'], 2);
                    const curMs = tempSlotReportsStore[slotKey]?.updatedAt
                      ? Date.parse(String(tempSlotReportsStore[slotKey].updatedAt))
                      : 0;
                    const delMs = rowUpdated ? Date.parse(rowUpdated) : 0;
                    if (!curMs || !delMs || delMs >= curMs) {
                      delete tempSlotReportsStore[slotKey];
                      tempSlotsHydratedChanged = true;
                    }
                  } else {
                    const p1 = getServerRowCell(jr, ['PAYLOAD_1'], 4);
                    const p2 = getServerRowCell(jr, ['PAYLOAD_2'], 5);
                    const p3 = getServerRowCell(jr, ['PAYLOAD_3'], 6);
                    const combinedJson = `${p1}${p2}${p3}`.trim();
                    if (combinedJson) {
                      try {
                        const parsedEntry = JSON.parse(combinedJson);
                        if (parsedEntry && parsedEntry.report && parsedEntry.dateStr === todayDateStr) {
                          const existing = tempSlotReportsStore[slotKey];
                          const incMs = parsedEntry.updatedAt ? Date.parse(parsedEntry.updatedAt) : 0;
                          const curMs = existing?.updatedAt ? Date.parse(existing.updatedAt) : 0;
                          if (!existing || incMs > curMs) {
                            tempSlotReportsStore[slotKey] = parsedEntry;
                            tempSlotsHydratedChanged = true;
                          }
                        }
                      } catch {
                        // ignore malformed temp slot row
                      }
                    }
                  }
                }
                continue;
              }

              if (action === 'DELETE') {
                latestById.delete(repId);
                deletedInJournal.add(repId);
                continue;
              }
              const p1 = getServerRowCell(jr, ['PAYLOAD_1'], 4);
              const p2 = getServerRowCell(jr, ['PAYLOAD_2'], 5);
              const p3 = getServerRowCell(jr, ['PAYLOAD_3'], 6);
              const combinedJson = `${p1}${p2}${p3}`.trim();
              if (!combinedJson) continue;
              try {
                const parsed = JSON.parse(combinedJson);
                if (parsed && parsed.id) {
                  deletedInJournal.delete(repId);
                  const expanded = expandHydratedSheetReport(parsed);
                  const prev = latestById.get(repId);
                  latestById.set(repId, prev ? mergeServerReportRecords(prev, expanded) : expanded);
                }
              } catch {
                // ignore malformed journal row
              }
            }

            if (hourlyTasksFromJournal.size > 0) {
              let hourlyChanged = false;
              for (const task of hourlyTasksFromJournal.values()) {
                const idx = hourlySignTasks.findIndex((t) => t.id === task.id);
                if (idx === -1) {
                  hourlySignTasks.unshift(task);
                  hourlyChanged = true;
                  if (options?.broadcastDiffs) {
                    broadcastSseEvent(
                      task.status === 'signed' ? 'hourly_cbql_signed' : 'hourly_cbql_remind',
                      { task }
                    );
                  }
                } else {
                  const prevTask = hourlySignTasks[idx];
                  const isIncomingNewer =
                    (task.status === 'signed' && prevTask.status !== 'signed') ||
                    (task.notifiedCbqlAtMs || 0) > (prevTask.notifiedCbqlAtMs || 0) ||
                    (Boolean(task.escalatedQaQcAtMs) && !prevTask.escalatedQaQcAtMs);
                  if (isIncomingNewer) {
                    hourlySignTasks[idx] = { ...prevTask, ...task };
                    hourlyChanged = true;
                    if (options?.broadcastDiffs) {
                      broadcastSseEvent(
                        task.status === 'signed'
                          ? 'hourly_cbql_signed'
                          : task.escalatedQaQcAtMs
                          ? 'hourly_cbql_escalate'
                          : 'hourly_cbql_remind',
                        { task: hourlySignTasks[idx] }
                      );
                    }
                  }
                }
              }
              if (hourlyChanged) {
                saveHourlySignTasks(hourlySignTasks);
              }
            }

            if (deletedInJournal.size > 0) {
              for (const delId of deletedInJournal) {
                if (sharedReportsStore.some((r) => r.id === delId)) {
                  newlyDeletedIds.push(delId);
                }
              }
              const delArr = Array.from(new Set([...deletedReportIdsStore, ...deletedInJournal])).slice(0, 300);
              deletedReportIdsStore = delArr;
              saveDeletedReportIds(deletedReportIdsStore);
              sharedReportsStore = sharedReportsStore.filter((r) => !deletedInJournal.has(r.id));
              saveSharedReportsToDisk(sharedReportsStore);
            }
            if (latestById.size > 0) {
              upsertSharedReports(Array.from(latestById.values()), { allowResurrect: true });
            }
            if (draftsHydratedChanged) {
              saveUserDraftsStore(userDraftsStore);
            }
            if (tempSlotsHydratedChanged) {
              pruneTempSlotReportsStore();
              saveTempSlotReportsStore(tempSlotReportsStore);
            }
          }
        }
      }

      // 2. Hydrate historical reports from all 12 DATA * spreadsheets in parallel via fast GViz first (~1s total)
      if (needDataSheets) {
        lastDataSheetsHydrationAt = Date.now();
        hasHydratedDataSheetsFromSheets = true;
        const allResults = await Promise.all(
          SERVER_RFT_DATA_TARGETS.map(async (target) => {
            const sheetUrl = `https://docs.google.com/spreadsheets/d/${target.spreadsheetId}/edit`;
            // Try fast GViz on gid=0 first (all 12 DATA * files have single primary tab gid=0)
            let rows = await fetchSingleSheetRowsWithTimeout(
              sheetUrl,
              'Trang tính1',
              '0',
              undefined,
              8000
            );
            if ((!rows || rows.length === 0) && gasUrl) {
              rows = await fetchSingleSheetRowsWithTimeout(
                sheetUrl,
                '*',
                '0',
                gasUrl,
                10000
              );
            }
            return mapDataSheetRowsToReports(target, rows);
          })
        );
        const recoveredAll: Array<Record<string, any>> = [];
        for (const arr of allResults) {
          recoveredAll.push(...arr);
        }
        if (recoveredAll.length > 0) {
          upsertSharedReports(recoveredAll);
        }
      }

      restorePendingEodFromHydratedReports();

      if (options?.broadcastDiffs) {
        const newlyChanged = sharedReportsStore.filter((r) => {
          if (!r?.id) return false;
          const prevFp = beforeFingerprints.get(r.id);
          const nextFp = `${r.eodStatus || ''}|${(r.editHistory || []).length}|${r.syncStatus || ''}|${r.qcSignedAt || ''}|${r.qaSignedAt || ''}|${r.cbqlSignedAt || ''}`;
          return prevFp !== nextFp;
        });
        if (newlyChanged.length > 0) {
          broadcastSseEvent('reports_updated', { reports: newlyChanged });
        }
        for (const delId of newlyDeletedIds) {
          broadcastSseEvent('report_deleted', { id: delId });
        }
      }
    } catch (err) {
      console.error('Reports sheet hydration warning:', err);
    } finally {
      reportsHydrationInFlight = null;
    }
  })();

  await reportsHydrationInFlight;
}

app.get('/api/reports', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  const forceRefresh = req.query.refresh === '1';
  await ensureReportsHydratedFromSheets(forceRefresh);
  return res.json({
    ok: true,
    reports: sharedReportsStore,
    deletedIds: deletedReportIdsStore,
  });
});

app.post('/api/reports', async (req, res) => {
  try {
    const body = req.body || {};
    const list: Array<Record<string, any>> = Array.isArray(body.reports)
      ? body.reports
      : body.report && body.report.id
      ? [body.report]
      : [];
    if (list.length === 0) {
      return res.status(400).json({ ok: false, error: 'Thiếu dữ liệu báo cáo.' });
    }
    const upserted = upsertSharedReports(list);
    let persistedToSheet = false;
    if (upserted.length > 0) {
      broadcastSseEvent('reports_updated', { reports: upserted });
      broadcastAccountStatusChanged('report_submitted');
      persistedToSheet = await persistReportActionsToMasterSheet(
        upserted.map((r) => ({
          reportId: String(r.id),
          dateStr: String(r.dateStr || ''),
          action: 'UPSERT' as const,
          report: r,
        }))
      );
    }
    return res.json({
      ok: true,
      persistedToSheet,
      reports: sharedReportsStore,
      deletedIds: deletedReportIdsStore,
      upsertedCount: upserted.length,
    });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Lỗi lưu báo cáo dùng chung.' });
  }
});

app.delete('/api/reports/:id', async (req, res) => {
  try {
    const id = String(req.params.id || '').trim();
    if (!id) {
      return res.status(400).json({ ok: false, error: 'Thiếu mã báo cáo cần xóa.' });
    }
    sharedReportsStore = sharedReportsStore.filter((r) => r.id !== id);
    saveSharedReportsToDisk(sharedReportsStore);
    if (!deletedReportIdsStore.includes(id)) {
      deletedReportIdsStore.unshift(id);
      if (deletedReportIdsStore.length > 300) {
        deletedReportIdsStore = deletedReportIdsStore.slice(0, 300);
      }
      saveDeletedReportIds(deletedReportIdsStore);
    }
    broadcastSseEvent('report_deleted', { id });
    const persistedToSheet = await persistReportActionsToMasterSheet([
      { reportId: id, dateStr: '', action: 'DELETE' },
    ]);
    return res.json({ ok: true, id, persistedToSheet });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Lỗi xóa báo cáo.' });
  }
});

const CLASSIC_PUSH_SW_SCRIPT = `
self.addEventListener('install', function () {
  self.skipWaiting();
});

self.addEventListener('activate', function (event) {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('push', function (event) {
  var payload = {};
  try {
    if (event.data) {
      payload = event.data.json();
    }
  } catch (e) {
    var raw = '';
    try {
      raw = event.data ? event.data.text() : '';
    } catch (e2) {}
    payload = { title: 'QC Report TBS', body: raw };
  }

  var title = String(payload.title || 'QC Report TBS — Xác nhận chốt ca');
  var body = String(payload.body || 'Có báo cáo chốt ca đang chờ QA / CBQL / Thủ kho ký xác nhận.');
  var eodId = String(payload.eodId || '');
  var hourlySignId = String(payload.hourlySignId || '');
  var notifType = String(payload.type || '');
  var targetUrl = String(
    payload.url ||
      (eodId
        ? '/?eodId=' + encodeURIComponent(eodId)
        : hourlySignId
        ? '/?hourlySignId=' + encodeURIComponent(hourlySignId)
        : '/?openNotifications=1')
  );

  var tagValue = String(
    payload.tag ||
      (eodId ? 'rft-eod-' + eodId : hourlySignId ? 'rft-hourly-' + hourlySignId : 'rft-eod-alert')
  );
  var notifDataObj = {
    url: targetUrl,
    eodId: eodId,
    hourlySignId: hourlySignId,
    type: notifType,
    timestamp: Date.now()
  };

  var options = {
    body: body,
    icon: '/pwa-192x192.png',
    badge: '/favicon.png',
    tag: tagValue,
    renotify: true,
    requireInteraction: true,
    vibrate: [250, 100, 250, 100, 350],
    data: notifDataObj,
    actions: [
      {
        action: 'open_sign',
        title: eodId || hourlySignId ? 'Mở & Ký tên' : 'Mở ứng dụng'
      }
    ]
  };

  event.waitUntil(
    self.registration.showNotification(title, options).catch(function () {
      return self.registration.showNotification(title, {
        body: body,
        icon: '/pwa-192x192.png',
        badge: '/favicon.png',
        tag: tagValue,
        data: notifDataObj
      });
    })
  );
});

self.addEventListener('notificationclick', function (event) {
  event.notification.close();
  var notifData = event.notification.data || {};
  var relativeTarget =
    notifData.url ||
    (notifData.eodId
      ? '/?eodId=' + encodeURIComponent(notifData.eodId)
      : notifData.hourlySignId
      ? '/?hourlySignId=' + encodeURIComponent(notifData.hourlySignId)
      : '/?openNotifications=1');
  var fullTargetUrl = new URL(relativeTarget, self.location.origin).href;

  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function (clientList) {
      for (var i = 0; i < clientList.length; i++) {
        var client = clientList[i];
        if (client.url && client.url.indexOf(self.location.origin) === 0) {
          return client.focus().then(function () {
            client.postMessage({
              type: 'OPEN_EOD_SIGN',
              eodId: notifData.eodId || '',
              hourlySignId: notifData.hourlySignId || '',
              notifType: notifData.type || '',
              url: relativeTarget
            });
            return client;
          });
        }
      }
      return self.clients.openWindow(fullTargetUrl);
    })
  );
});
`;

app.get('/sw-push.js', (_req, res) => {
  res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.setHeader('Service-Worker-Allowed', '/');
  res.send(CLASSIC_PUSH_SW_SCRIPT);
});

app.get('/api/notifications/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders?.();

  res.write(`event: connected\ndata: ${JSON.stringify({ ok: true, time: new Date().toISOString() })}\n\n`);
  sseClients.add(res);

  const keepAlive = setInterval(() => {
    try {
      res.write(': ping\n\n');
    } catch {
      clearInterval(keepAlive);
      sseClients.delete(res);
    }
  }, 25000);

  req.on('close', () => {
    clearInterval(keepAlive);
    sseClients.delete(res);
  });
});

app.get('/api/push/vapid-public-key', (_req, res) => {
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.json({ ok: true, publicKey: vapidKeys.publicKey });
});

app.get('/api/push/status', (req, res) => {
  const endpoint = String(req.query.endpoint || '').trim();
  const matched = endpoint ? pushSubscriptions.find((s) => s.endpoint === endpoint) : undefined;
  res.json({
    ok: true,
    isSubscribed: Boolean(matched),
    subscriptionInfo: matched
      ? {
          userName: matched.userName,
          normalizedRole: matched.normalizedRole,
          receiveAllEodAlerts: Boolean(matched.receiveAllEodAlerts),
        }
      : null,
    roleCounts: getSubscriptionRoleCounts(),
    totalDevices: pushSubscriptions.length,
  });
});

app.post('/api/push/subscribe', (req, res) => {
  try {
    const { subscription, userName, role, chucVu, deviceSummary, receiveAllEodAlerts } = req.body || {};
    if (!subscription || typeof subscription.endpoint !== 'string' || !subscription.keys?.p256dh || !subscription.keys?.auth) {
      return res.status(400).json({ ok: false, error: 'Thiếu thông tin PushSubscription hợp lệ.' });
    }

    const cleanName = normAuthName(userName || 'Nhân sự TBS');
    const normalizedRole = normalizePushRoleCategory(role, chucVu);
    const nowIso = new Date().toISOString();
    const id = crypto.createHash('sha1').update(subscription.endpoint).digest('hex').slice(0, 16);

    const nextEntry: StoredPushSubscription = {
      id,
      endpoint: subscription.endpoint,
      subscription,
      userName: cleanName,
      rawRole: String(role || 'QC_User'),
      chucVu: String(chucVu || ''),
      normalizedRole,
      receiveAllEodAlerts: Boolean(receiveAllEodAlerts),
      deviceSummary: String(deviceSummary || ''),
      createdAt: nowIso,
      updatedAt: nowIso,
    };

    const existingIdx = pushSubscriptions.findIndex((s) => s.endpoint === subscription.endpoint);
    if (existingIdx >= 0) {
      nextEntry.createdAt = pushSubscriptions[existingIdx].createdAt || nowIso;
      pushSubscriptions[existingIdx] = nextEntry;
    } else {
      pushSubscriptions.unshift(nextEntry);
    }
    savePushSubscriptions(pushSubscriptions);

    return res.json({
      ok: true,
      normalizedRole,
      roleCounts: getSubscriptionRoleCounts(),
      totalDevices: pushSubscriptions.length,
    });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Không thể đăng ký nhận thông báo.' });
  }
});

app.post('/api/push/unsubscribe', (req, res) => {
  const endpoint = String(req.body?.endpoint || '').trim();
  if (endpoint) {
    pushSubscriptions = pushSubscriptions.filter((s) => s.endpoint !== endpoint);
    savePushSubscriptions(pushSubscriptions);
  }
  return res.json({
    ok: true,
    roleCounts: getSubscriptionRoleCounts(),
    totalDevices: pushSubscriptions.length,
  });
});

app.post('/api/push/test', async (req, res) => {
  try {
    const endpoint = String(req.body?.endpoint || '').trim();
    const userName = normAuthName(req.body?.userName || '');
    let targets = endpoint
      ? pushSubscriptions.filter((s) => s.endpoint === endpoint)
      : [];
    if (targets.length === 0 && userName) {
      const key = compareAuthNameKey(userName);
      targets = pushSubscriptions.filter((s) => compareAuthNameKey(s.userName) === key);
    }

    if (targets.length === 0) {
      return res.status(400).json({
        ok: false,
        error: 'Thiết bị này chưa đăng ký nhận thông báo Web Push. Vui lòng bật nhận thông báo trước!',
      });
    }

    const result = await sendWebPushToSubscriptions(targets, {
      title: '🔔 QC Report TBS — Thử nghiệm thông báo ngoài màn hình',
      body: `Xin chào ${userName || targets[0].userName} (${targets[0].normalizedRole}): Điện thoại đã sẵn sàng nhận thông báo khi QC chốt ca!`,
      tag: `rft-test-${Date.now()}`,
      url: '/?openNotifications=1',
    });

    if (result.sentCount <= 0) {
      return res.status(502).json({
        ok: false,
        ...result,
        error:
          result.prunedEndpoints.length > 0
            ? 'Đăng ký thông báo cũ trên thiết bị đã hết hạn hoặc lệch khóa. Vui lòng bấm "Bật thông báo ngoài máy" lại 1 lần nữa.'
            : `Không gửi được thông báo tới thiết bị (${result.lastError || 'Lỗi kết nối dịch vụ Push'}).`,
      });
    }

    return res.json({ ok: true, ...result });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Không thể gửi thông báo thử.' });
  }
});

app.get('/api/eod-pending', async (_req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  await ensureReportsHydratedFromSheets(false);
  const sorted = pendingEodList
    .slice()
    .sort((a, b) => {
      if (a.status !== b.status) return a.status === 'pending' ? -1 : 1;
      return (b.updatedAt || b.createdAt).localeCompare(a.updatedAt || a.createdAt);
    });
  return res.json({ ok: true, items: sorted, roleCounts: getSubscriptionRoleCounts() });
});

app.post('/api/eod-pending/notify', async (req, res) => {
  try {
    const {
      report,
      dailyReportsSnapshot,
      outputRouting,
      milestoneTime,
      createdBy,
      isReminder = false,
    } = req.body || {};

    if (!report || !report.id) {
      return res.status(400).json({ ok: false, error: 'Thiếu dữ liệu báo cáo chốt ca.' });
    }

    const { label: managerRoleLabel } = resolveManagerPushRole(
      report.managerRoleLabel,
      report.khuVuc,
      report.congDoan
    );

    const statusInfo = computeReportEodStatus(report, managerRoleLabel);
    const updatedReport = {
      ...report,
      managerRoleLabel,
      shiftEndMilestone: milestoneTime || report.shiftEndMilestone || '16:30',
      eodStatus: statusInfo.eodStatus,
    };

    const nowIso = new Date().toISOString();
    const existingIdx = pendingEodList.findIndex((i) => i.id === report.id);
    const nextItem: StoredPendingEodItem = {
      id: report.id,
      report: updatedReport,
      dailyReportsSnapshot: Array.isArray(dailyReportsSnapshot)
        ? dailyReportsSnapshot
        : existingIdx >= 0
        ? pendingEodList[existingIdx].dailyReportsSnapshot
        : undefined,
      outputRouting: outputRouting || (existingIdx >= 0 ? pendingEodList[existingIdx].outputRouting : undefined),
      managerRoleLabel,
      requiredRoles: ['QA', managerRoleLabel === 'Thủ kho' ? 'THU_KHO' : 'CBQL'],
      milestoneTime: milestoneTime || report.shiftEndMilestone || '16:30',
      createdAt: existingIdx >= 0 ? pendingEodList[existingIdx].createdAt : nowIso,
      updatedAt: nowIso,
      createdBy: createdBy || report.qcKiemTra || report.submittedBy || 'QC',
      status: statusInfo.isCompleted ? 'completed' : 'pending',
      pushSentCount: existingIdx >= 0 ? pendingEodList[existingIdx].pushSentCount || 0 : 0,
    };

    let pushStats = {
      targetedDevices: 0,
      sentCount: 0,
      failedCount: 0,
      missingLabels: statusInfo.missingLabels,
      roleCounts: getSubscriptionRoleCounts(),
    };

    if (!statusInfo.isCompleted && statusInfo.missingRoles.length > 0) {
      const targetRoleSet = new Set<string>(statusInfo.missingRoles);
      const assignedNameKeys = new Set<string>(
        [!report.qaPhuTrachSign && report.qaPhuTrach ? compareAuthNameKey(report.qaPhuTrach) : '',
         !report.cbqlXacNhanSign && report.cbqlXacNhan ? compareAuthNameKey(report.cbqlXacNhan) : '']
          .filter(Boolean)
      );

      const targetSubs = pushSubscriptions.filter(
        (sub) =>
          targetRoleSet.has(sub.normalizedRole) ||
          sub.receiveAllEodAlerts === true ||
          assignedNameKeys.has(compareAuthNameKey(sub.userName))
      );

      const locParts = [report.nhaMay, report.lineSanXuat, report.chuyen].filter(Boolean);
      const locationStr = Array.from(new Set(locParts)).join(' - ') || report.chuyen || 'Chuyền QC';
      const codeStr = [report.maGiay, report.maDe, report.maHang].find((v) => String(v || '').trim()) || '';
      const prefix = isReminder ? '⏰ Nhắc ký chốt ca' : '📋 QC đã chốt ca';

      const sendRes = await sendWebPushToSubscriptions(targetSubs, {
        title: `${prefix} (${nextItem.milestoneTime}): Cần ${statusInfo.missingLabels.join(' & ')} ký tên`,
        body: `${locationStr} • ${report.congDoanTen}${codeStr ? ` (${codeStr})` : ''} — QC ${nextItem.createdBy} đã xác nhận. Chạm để vào ký hoàn thành báo cáo ngày!`,
        eodId: report.id,
        tag: `rft-eod-${report.id}`,
        url: `/?eodId=${encodeURIComponent(report.id)}`,
      });

      nextItem.pushSentCount = (nextItem.pushSentCount || 0) + sendRes.sentCount;
      pushStats = {
        targetedDevices: targetSubs.length,
        sentCount: sendRes.sentCount,
        failedCount: sendRes.failedCount,
        missingLabels: statusInfo.missingLabels,
        roleCounts: getSubscriptionRoleCounts(),
      };
    }

    if (existingIdx >= 0) {
      pendingEodList[existingIdx] = nextItem;
    } else {
      pendingEodList.unshift(nextItem);
    }
    savePendingEodList(pendingEodList);
    const upsertedEod = upsertSharedReports([updatedReport]);
    if (upsertedEod.length > 0) {
      await persistReportActionsToMasterSheet(
        upsertedEod.map((r) => ({
          reportId: String(r.id),
          dateStr: String(r.dateStr || ''),
          action: 'UPSERT' as const,
          report: r,
        }))
      );
    }
    recordQcActivityInternal({
      dateStr: updatedReport.dateStr,
      qcName: updatedReport.qcKiemTra || createdBy || 'QC',
      factory: updatedReport.nhaMay,
      line: updatedReport.lineSanXuat,
      chuyen: updatedReport.chuyen,
      stageId: updatedReport.congDoan,
      stageName: updatedReport.congDoanTen,
      isEndOfDay: true,
    });
    broadcastSseEvent('eod_pending_updated', { item: nextItem });
    broadcastSseEvent('reports_updated', { reports: [updatedReport] });

    return res.json({ ok: true, item: nextItem, pushStats });
  } catch (err: any) {
    console.error('EOD notify error:', err);
    return res.status(500).json({ ok: false, error: err?.message || 'Lỗi gửi thông báo chốt ca.' });
  }
});

app.post('/api/eod-pending/qc-heartbeat', (req, res) => {
  try {
    const body = req.body || {};
    recordQcActivityInternal({
      dateStr: body.dateStr,
      qcName: body.qcName,
      factory: body.factory,
      line: body.line,
      chuyen: body.chuyen,
      stageId: body.stageId,
      stageName: body.stageName,
      isEndOfDay: Boolean(body.isEndOfDay),
    });
    return res.json({ ok: true, status: getTodayQcShiftStatus() });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Không ghi nhận được trạng thái QC.' });
  }
});

app.get('/api/eod-pending/qc-reminder-status', (_req, res) => {
  return res.json({ ok: true, ...getTodayQcShiftStatus() });
});

app.post('/api/eod-pending/trigger-qc-reminder', async (req, res) => {
  try {
    const triggeredBy = String(req.body?.triggeredBy || 'Thủ công').trim();
    const force = req.body?.force !== false;
    const result = await dispatchQcDailyReminder({
      triggeredBy,
      force,
    });
    return res.json({ ok: true, ...result });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Không gửi được nhắc nhở QC.' });
  }
});

app.post('/api/eod-pending/:id/sign', async (req, res) => {
  try {
    await ensureServerConfigHydrated();
    const { id } = req.params;
    const { signerRole, signerName, signatureDataUrl, webAppUrl } = req.body || {};

    if (!signerRole || !['qa', 'cbql', 'qc'].includes(signerRole)) {
      return res.status(400).json({ ok: false, error: 'Vai trò ký xác nhận không hợp lệ.' });
    }
    if (!String(signerName || '').trim() || !String(signatureDataUrl || '').startsWith('data:image/')) {
      return res.status(400).json({ ok: false, error: 'Vui lòng nhập họ tên và vẽ chữ ký xác nhận.' });
    }

    const idx = pendingEodList.findIndex((i) => i.id === id);
    if (idx === -1) {
      return res.status(404).json({ ok: false, error: 'Không tìm thấy phiếu chốt ca cần ký này.' });
    }

    const currentItem = pendingEodList[idx];
    const cleanName = normAuthName(signerName);
    const nowTime = new Intl.DateTimeFormat('vi-VN', {
      timeZone: 'Asia/Ho_Chi_Minh',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).format(new Date());

    const nextReport = { ...currentItem.report };
    if (signerRole === 'qc') {
      nextReport.qcKiemTra = cleanName;
      nextReport.qcKiemTraSign = signatureDataUrl;
      nextReport.qcSignedAt = nowTime;
    } else if (signerRole === 'qa') {
      nextReport.qaPhuTrach = cleanName;
      nextReport.qaPhuTrachSign = signatureDataUrl;
      nextReport.qaSignedAt = nowTime;
    } else if (signerRole === 'cbql') {
      nextReport.cbqlXacNhan = cleanName;
      nextReport.cbqlXacNhanSign = signatureDataUrl;
      nextReport.cbqlSignedAt = nowTime;
      nextReport.toTruongCuoiNgay = cleanName;
      nextReport.toTruongCuoiNgaySign = signatureDataUrl;
    }

    const roleDisplay =
      signerRole === 'qc'
        ? 'QC'
        : signerRole === 'qa'
        ? 'QA'
        : currentItem.managerRoleLabel || 'CBQL';

    nextReport.editHistory = [
      ...(Array.isArray(nextReport.editHistory) ? nextReport.editHistory : []),
      {
        timestamp: formatServerLoginTimestamp(),
        user: cleanName,
        action: `${roleDisplay} ký xác nhận chốt ca từ xa`,
        note: `Đã ký lúc ${nowTime}`,
      },
    ];

    const statusInfo = computeReportEodStatus(nextReport, currentItem.managerRoleLabel);
    nextReport.eodStatus = statusInfo.eodStatus;

    let updatedSnapshot = Array.isArray(currentItem.dailyReportsSnapshot)
      ? currentItem.dailyReportsSnapshot.map((row) =>
          row.id === nextReport.id
            ? {
                ...row,
                qcKiemTra: nextReport.qcKiemTra,
                qcKiemTraSign: nextReport.qcKiemTraSign,
                qcSignedAt: nextReport.qcSignedAt,
                qaPhuTrach: nextReport.qaPhuTrach,
                qaPhuTrachSign: nextReport.qaPhuTrachSign,
                qaSignedAt: nextReport.qaSignedAt,
                cbqlXacNhan: nextReport.cbqlXacNhan,
                cbqlXacNhanSign: nextReport.cbqlXacNhanSign,
                cbqlSignedAt: nextReport.cbqlSignedAt,
                toTruongCuoiNgay: nextReport.toTruongCuoiNgay,
                toTruongCuoiNgaySign: nextReport.toTruongCuoiNgaySign,
                eodStatus: nextReport.eodStatus,
              }
            : row
        )
      : undefined;

    if (updatedSnapshot && updatedSnapshot.length > 0 && !updatedSnapshot.some((r) => r.id === nextReport.id)) {
      updatedSnapshot.push(nextReport);
    }

    const updatedItem: StoredPendingEodItem = {
      ...currentItem,
      report: nextReport,
      dailyReportsSnapshot: updatedSnapshot,
      updatedAt: new Date().toISOString(),
      status: statusInfo.isCompleted ? 'completed' : 'pending',
    };

    pendingEodList[idx] = updatedItem;
    savePendingEodList(pendingEodList);
    const upsertedSigned = upsertSharedReports([nextReport]);
    if (upsertedSigned.length > 0) {
      await persistReportActionsToMasterSheet(
        upsertedSigned.map((r) => ({
          reportId: String(r.id),
          dateStr: String(r.dateStr || ''),
          action: 'UPSERT' as const,
          report: r,
        }))
      );
    }

    let gasSynced = false;
    const resolvedGasUrl = resolveValidGasUrl(webAppUrl);
    if (resolvedGasUrl) {
      try {
        const gasPayload = {
          action: 'submitReport',
          ...nextReport,
          ...(currentItem.outputRouting || {}),
          pdfFolderUrl:
            currentItem.outputRouting?.pdfFolderUrl ||
            sharedServerConfig.pdfFolderUrl ||
            DEFAULT_PDF_ARCHIVE_FOLDER_URL,
          generateDailyPdf: statusInfo.isCompleted,
          ...(updatedSnapshot && updatedSnapshot.length > 0 ? { dailyReports: updatedSnapshot } : {}),
        };
        const gasRes = await fetch(resolvedGasUrl, {
          method: 'POST',
          redirect: 'follow',
          headers: { 'Content-Type': 'text/plain;charset=utf-8' },
          body: JSON.stringify(gasPayload),
        });
        const gasTxt = await gasRes.text().catch(() => '');
        gasSynced = gasRes.ok && !gasTxt.includes('"status":"error"') && !gasTxt.trim().startsWith('<!DOCTYPE html');
      } catch {
        gasSynced = false;
      }
    }

    // Push completion alert or notify remaining signer
    if (statusInfo.isCompleted) {
      const targetSubs = pushSubscriptions.filter(
        (s) =>
          compareAuthNameKey(s.userName) === compareAuthNameKey(updatedItem.createdBy) ||
          s.normalizedRole === 'QC' ||
          s.normalizedRole === 'QA' ||
          s.normalizedRole === (updatedItem.managerRoleLabel === 'Thủ kho' ? 'THU_KHO' : 'CBQL') ||
          s.receiveAllEodAlerts === true
      );
      await sendWebPushToSubscriptions(targetSubs, {
        title: `✅ Hoàn thành báo cáo ngày (${updatedItem.milestoneTime})`,
        body: `${nextReport.chuyen} (${nextReport.congDoanTen}) đã đủ 3 chữ ký (QC, QA, ${updatedItem.managerRoleLabel})!`,
        eodId: nextReport.id,
        tag: `rft-eod-done-${nextReport.id}`,
        url: `/?eodId=${encodeURIComponent(nextReport.id)}`,
      });
    }

    broadcastSseEvent('eod_pending_updated', { item: updatedItem });
    broadcastSseEvent('reports_updated', { reports: [nextReport] });

    return res.json({
      ok: true,
      item: updatedItem,
      isCompleted: statusInfo.isCompleted,
      gasSynced,
    });
  } catch (err: any) {
    console.error('EOD sign error:', err);
    return res.status(500).json({ ok: false, error: err?.message || 'Lỗi lưu chữ ký chốt ca.' });
  }
});

// ============================================================================
// HOURLY CBQL SIGN-OFF REMINDER & 10-MINUTE QA/QC ESCALATION ENGINE
// ============================================================================
const HOURLY_STATIONS_FILE = '/tmp/rft_hourly_stations.json';
const HOURLY_SIGN_TASKS_FILE = '/tmp/rft_hourly_sign_tasks.json';
const HOURLY_ESCALATION_MS = 10 * 60 * 1000; // 10 phút

interface StoredActiveHourlyStation {
  key: string; // `${dateStr}|${chuyen}|${stageId}`
  dateStr: string;
  factory: string;
  line: string;
  chuyen: string;
  stageId: string;
  stageName: string;
  qcName: string;
  qaName: string;
  cbqlName: string;
  currentSlot: string;
  updatedAt: string;
}

interface StoredHourlyCbqlSignTask {
  id: string;
  dateStr: string;
  factory: string;
  line: string;
  chuyen: string;
  stageId: string;
  stageName: string;
  timeSlot: string;
  qcName: string;
  qaName: string;
  cbqlName: string;
  status: 'waiting_cbql' | 'signed';
  notifiedCbqlAt: string;
  notifiedCbqlAtMs: number;
  escalatedQaQcAt?: string;
  escalatedQaQcAtMs?: number;
  signedAt?: string;
  signedByName?: string;
  cbqlSignatureDataUrl?: string;
  triggerSource: 'scheduled_hourly' | 'manual_request';
}

const HOURLY_SIGN_MILESTONES: Array<{ slot: string; triggerMinute: number }> = [
  { slot: '7h30 - 8h30', triggerMinute: 8 * 60 + 30 },
  { slot: '8h30 - 9h30', triggerMinute: 9 * 60 + 30 },
  { slot: '9h30 - 10h30', triggerMinute: 10 * 60 + 30 },
  { slot: '10h30 - 11h30', triggerMinute: 11 * 60 + 30 },
  { slot: '12h30 - 13h30', triggerMinute: 13 * 60 + 30 },
  { slot: '13h30 - 14h30', triggerMinute: 14 * 60 + 30 },
  { slot: '14h30 - 15h30', triggerMinute: 15 * 60 + 30 },
  { slot: '15h30 - 16h30', triggerMinute: 16 * 60 + 30 },
  { slot: '16h30 - 18h00', triggerMinute: 18 * 60 + 0 },
  { slot: '18h00 - 19h00', triggerMinute: 19 * 60 + 0 },
  { slot: '19h00 - 20h00', triggerMinute: 20 * 60 + 0 },
];

function isInputOrEodOnlyStageServer(stageId?: string, factory?: string): boolean {
  const s = String(stageId || '').trim();
  if (['VatTu', 'VTGiaCong', 'VTTheuGiaCong', 'Tem', 'BanTrai', 'DeGiaCong'].includes(s)) return true;
  if (s === 'DeBTP' && String(factory || '').trim() !== 'Hoàn thiện đế') return true;
  return false;
}

function loadActiveHourlyStations(): StoredActiveHourlyStation[] {
  try {
    if (fs.existsSync(HOURLY_STATIONS_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(HOURLY_STATIONS_FILE, 'utf8'));
      if (Array.isArray(parsed)) {
        const { dateStr } = getVietnamClock();
        return parsed.filter((s) => s?.dateStr === dateStr);
      }
    }
  } catch {
    // ignore
  }
  return [];
}

function saveActiveHourlyStations(list: StoredActiveHourlyStation[]) {
  try {
    fs.writeFileSync(HOURLY_STATIONS_FILE, JSON.stringify(list.slice(0, 200), null, 2), 'utf8');
  } catch {
    // ignore
  }
}

function loadHourlySignTasks(): StoredHourlyCbqlSignTask[] {
  try {
    if (fs.existsSync(HOURLY_SIGN_TASKS_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(HOURLY_SIGN_TASKS_FILE, 'utf8'));
      if (Array.isArray(parsed)) {
        const cutoff = Date.now() - 3 * 24 * 60 * 60 * 1000;
        return parsed.filter((t) => !t?.notifiedCbqlAtMs || t.notifiedCbqlAtMs >= cutoff);
      }
    }
  } catch {
    // ignore
  }
  return [];
}

function saveHourlySignTasks(list: StoredHourlyCbqlSignTask[]) {
  try {
    fs.writeFileSync(HOURLY_SIGN_TASKS_FILE, JSON.stringify(list.slice(0, 300), null, 2), 'utf8');
  } catch {
    // ignore
  }
}

let activeHourlyStations: StoredActiveHourlyStation[] = loadActiveHourlyStations();
let hourlySignTasks: StoredHourlyCbqlSignTask[] = loadHourlySignTasks();

function buildHourlyTaskId(dateStr: string, chuyen: string, stageId: string, timeSlot: string): string {
  const hash = crypto
    .createHash('sha1')
    .update(`${dateStr}|${chuyen}|${stageId}|${timeSlot}`)
    .digest('hex')
    .slice(0, 10);
  return `HS-${dateStr}-${hash}`;
}

function getTodayHourlyTasks(): StoredHourlyCbqlSignTask[] {
  const { dateStr } = getVietnamClock();
  return hourlySignTasks
    .filter((t) => t.dateStr === dateStr)
    .sort((a, b) => {
      if (a.status !== b.status) return a.status === 'waiting_cbql' ? -1 : 1;
      return (b.notifiedCbqlAtMs || 0) - (a.notifiedCbqlAtMs || 0);
    });
}

async function dispatchHourlyCbqlReminder(task: StoredHourlyCbqlSignTask) {
  const cbqlKey = compareAuthNameKey(task.cbqlName);
  const targetSubs = pushSubscriptions.filter(
    (s) =>
      s.normalizedRole === 'CBQL' ||
      s.receiveAllEodAlerts === true ||
      (cbqlKey && compareAuthNameKey(s.userName) === cbqlKey)
  );

  const locStr = [task.factory, task.line, task.chuyen].filter(Boolean).join(' • ') || task.chuyen;
  const cbqlLabel = task.cbqlName ? `CBQL ${task.cbqlName}` : 'CBQL';

  const sendRes = await sendWebPushToSubscriptions(targetSubs, {
    title: `✍️ Đến giờ CBQL ký xác nhận (${task.timeSlot})`,
    body: `${locStr} (${task.stageName}): Đã đến khung giờ ${task.timeSlot}. Mời ${cbqlLabel} mở app hoặc chạm để ký tên xác nhận!`,
    type: 'hourly_cbql_remind',
    hourlySignId: task.id,
    tag: `rft-hourly-cbql-${task.id}`,
    url: `/?hourlySignId=${encodeURIComponent(task.id)}`,
  });

  broadcastSseEvent('hourly_cbql_remind', { task });
  return { targetedDevices: targetSubs.length, sentCount: sendRes.sentCount, failedCount: sendRes.failedCount };
}

async function dispatchHourlyQaQcEscalation(task: StoredHourlyCbqlSignTask) {
  const qaKey = compareAuthNameKey(task.qaName);
  const qcKey = compareAuthNameKey(task.qcName);
  const targetSubs = pushSubscriptions.filter(
    (s) =>
      s.normalizedRole === 'QA' ||
      s.normalizedRole === 'QC' ||
      s.receiveAllEodAlerts === true ||
      (qaKey && compareAuthNameKey(s.userName) === qaKey) ||
      (qcKey && compareAuthNameKey(s.userName) === qcKey)
  );

  const locStr = [task.factory, task.line, task.chuyen].filter(Boolean).join(' • ') || task.chuyen;
  const cbqlLabel = task.cbqlName || 'CBQL';

  const sendRes = await sendWebPushToSubscriptions(targetSubs, {
    title: `⚠️ Quá 10 phút CBQL chưa ký tên (${task.timeSlot})`,
    body: `${locStr} (${task.stageName}): ${cbqlLabel} chưa ký xác nhận sau 10 phút. QA (${task.qaName || 'QA'}) & QC (${task.qcName || 'QC'}) vui lòng nhắc CBQL ký tên ngay!`,
    type: 'hourly_cbql_escalate',
    hourlySignId: task.id,
    tag: `rft-hourly-escalate-${task.id}`,
    url: `/?hourlySignId=${encodeURIComponent(task.id)}`,
  });

  broadcastSseEvent('hourly_cbql_escalate', { task });
  return { targetedDevices: targetSubs.length, sentCount: sendRes.sentCount, failedCount: sendRes.failedCount };
}

let isRunningHourlyCbqlScheduler = false;
async function runHourlyCbqlNotificationScheduler() {
  if (isRunningHourlyCbqlScheduler) return;
  isRunningHourlyCbqlScheduler = true;
  try {
    const vnNow = getVietnamClock();
    const todayStr = vnNow.dateStr;
    const skipSunday = sharedServerConfig.qcReminderSkipSunday !== false;

    // 1. Tạo thông báo hàng giờ cho các trạm đang hoạt động (trừ công đoạn Đầu vào chỉ ký cuối ca)
    if (!(skipSunday && vnNow.weekdaySun0 === 0)) {
      const todayStations = activeHourlyStations.filter(
        (st) => st.dateStr === todayStr && !isInputOrEodOnlyStageServer(st.stageId, st.factory)
      );

      for (const milestone of HOURLY_SIGN_MILESTONES) {
        if (vnNow.minutesOfDay < milestone.triggerMinute || vnNow.minutesOfDay > milestone.triggerMinute + 50) {
          continue;
        }
        for (const st of todayStations) {
          const taskId = buildHourlyTaskId(todayStr, st.chuyen, st.stageId, milestone.slot);
          const existing = hourlySignTasks.find((t) => t.id === taskId);
          if (existing) continue;

          const nowMs = Date.now();
          const newTask: StoredHourlyCbqlSignTask = {
            id: taskId,
            dateStr: todayStr,
            factory: st.factory,
            line: st.line,
            chuyen: st.chuyen,
            stageId: st.stageId,
            stageName: st.stageName,
            timeSlot: milestone.slot,
            qcName: st.qcName,
            qaName: st.qaName,
            cbqlName: st.cbqlName,
            status: 'waiting_cbql',
            notifiedCbqlAt: new Date(nowMs).toISOString(),
            notifiedCbqlAtMs: nowMs,
            triggerSource: 'scheduled_hourly',
          };
          hourlySignTasks.unshift(newTask);
          saveHourlySignTasks(hourlySignTasks);
          await dispatchHourlyCbqlReminder(newTask);
        }
      }
    }

    // 2. Kiểm tra mốc 10 phút: nếu CBQL chưa ký -> nhắc QA & QC đôn đốc CBQL
    const nowMs = Date.now();
    for (let i = 0; i < hourlySignTasks.length; i++) {
      const task = hourlySignTasks[i];
      if (task.dateStr !== todayStr) continue;
      if (task.status !== 'waiting_cbql') continue;
      if (task.escalatedQaQcAtMs) continue;
      if (nowMs - task.notifiedCbqlAtMs >= HOURLY_ESCALATION_MS) {
        const updated: StoredHourlyCbqlSignTask = {
          ...task,
          escalatedQaQcAt: new Date(nowMs).toISOString(),
          escalatedQaQcAtMs: nowMs,
        };
        hourlySignTasks[i] = updated;
        saveHourlySignTasks(hourlySignTasks);
        await dispatchHourlyQaQcEscalation(updated);
      }
    }
  } catch (err) {
    console.error('Hourly CBQL scheduler error:', err);
  } finally {
    isRunningHourlyCbqlScheduler = false;
  }
}

app.get('/api/hourly-sign/pending', (_req, res) => {
  return res.json({
    ok: true,
    tasks: getTodayHourlyTasks(),
    activeStationsCount: activeHourlyStations.length,
  });
});

app.post('/api/hourly-sign/register', (req, res) => {
  try {
    const vnNow = getVietnamClock();
    const body = req.body || {};
    const dateStr = String(body.dateStr || vnNow.dateStr).trim() || vnNow.dateStr;
    const chuyen = String(body.chuyen || '').trim();
    const stageId = String(body.stageId || '').trim();
    const factory = String(body.factory || '').trim();

    if (!chuyen || !stageId) {
      return res.json({ ok: true, tasks: getTodayHourlyTasks() });
    }

    if (!isInputOrEodOnlyStageServer(stageId, factory)) {
      const key = `${dateStr}|${chuyen}|${stageId}`;
      const nextStation: StoredActiveHourlyStation = {
        key,
        dateStr,
        factory,
        line: String(body.line || '').trim(),
        chuyen,
        stageId,
        stageName: String(body.stageName || stageId).trim(),
        qcName: normAuthName(body.qcName || ''),
        qaName: normAuthName(body.qaName || ''),
        cbqlName: normAuthName(body.cbqlName || ''),
        currentSlot: String(body.timeSlot || '').trim(),
        updatedAt: new Date().toISOString(),
      };

      const idx = activeHourlyStations.findIndex((s) => s.key === key);
      if (idx >= 0) {
        activeHourlyStations[idx] = nextStation;
      } else {
        activeHourlyStations.unshift(nextStation);
      }
      saveActiveHourlyStations(activeHourlyStations);
    }

    return res.json({ ok: true, tasks: getTodayHourlyTasks() });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Lỗi đăng ký trạm theo giờ.' });
  }
});

app.post('/api/hourly-sign/request-now', async (req, res) => {
  try {
    const vnNow = getVietnamClock();
    const body = req.body || {};
    const dateStr = String(body.dateStr || vnNow.dateStr).trim() || vnNow.dateStr;
    const factory = String(body.factory || '').trim();
    const line = String(body.line || '').trim();
    const chuyen = String(body.chuyen || '').trim();
    const stageId = String(body.stageId || '').trim();
    const stageName = String(body.stageName || stageId).trim();
    const timeSlot = String(body.timeSlot || '7h30 - 8h30').trim();
    const qcName = normAuthName(body.qcName || '');
    const qaName = normAuthName(body.qaName || '');
    const cbqlName = normAuthName(body.cbqlName || '');

    if (!chuyen || !stageId) {
      return res.status(400).json({ ok: false, error: 'Thiếu thông tin vị trí kiểm hoặc công đoạn.' });
    }
    if (isInputOrEodOnlyStageServer(stageId, factory)) {
      return res.status(400).json({ ok: false, error: 'Khu vực Đầu vào không yêu cầu CBQL ký hàng giờ.' });
    }

    const id = buildHourlyTaskId(dateStr, chuyen, stageId, timeSlot);
    const nowMs = Date.now();
    const existingIdx = hourlySignTasks.findIndex((t) => t.id === id);

    let task: StoredHourlyCbqlSignTask;
    if (existingIdx >= 0) {
      task = {
        ...hourlySignTasks[existingIdx],
        factory: factory || hourlySignTasks[existingIdx].factory,
        line: line || hourlySignTasks[existingIdx].line,
        stageName: stageName || hourlySignTasks[existingIdx].stageName,
        qcName: qcName || hourlySignTasks[existingIdx].qcName,
        qaName: qaName || hourlySignTasks[existingIdx].qaName,
        cbqlName: cbqlName || hourlySignTasks[existingIdx].cbqlName,
        status: 'waiting_cbql',
        notifiedCbqlAt: new Date(nowMs).toISOString(),
        notifiedCbqlAtMs: nowMs,
        escalatedQaQcAt: undefined,
        escalatedQaQcAtMs: undefined,
        signedAt: undefined,
        signedByName: undefined,
        cbqlSignatureDataUrl: undefined,
        triggerSource: 'manual_request',
      };
      hourlySignTasks[existingIdx] = task;
    } else {
      task = {
        id,
        dateStr,
        factory,
        line,
        chuyen,
        stageId,
        stageName,
        timeSlot,
        qcName,
        qaName,
        cbqlName,
        status: 'waiting_cbql',
        notifiedCbqlAt: new Date(nowMs).toISOString(),
        notifiedCbqlAtMs: nowMs,
        triggerSource: 'manual_request',
      };
      hourlySignTasks.unshift(task);
    }
    saveHourlySignTasks(hourlySignTasks);
    await persistHourlyTasksToMasterSheet([task]);

    const pushStats = await dispatchHourlyCbqlReminder(task);
    return res.json({ ok: true, task, pushStats, tasks: getTodayHourlyTasks() });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Không gửi được thông báo CBQL ký giờ.' });
  }
});

app.post('/api/hourly-sign/mark-signed', async (req, res) => {
  try {
    const vnNow = getVietnamClock();
    const body = req.body || {};
    const dateStr = String(body.dateStr || vnNow.dateStr).trim() || vnNow.dateStr;
    const chuyen = String(body.chuyen || '').trim();
    const stageId = String(body.stageId || '').trim();
    const timeSlot = String(body.timeSlot || '').trim();
    const signedByName = normAuthName(body.signedByName || 'CBQL');
    const signatureDataUrl = String(body.signatureDataUrl || '');

    const nowTime = new Intl.DateTimeFormat('vi-VN', {
      timeZone: 'Asia/Ho_Chi_Minh',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).format(new Date());

    const id = buildHourlyTaskId(dateStr, chuyen, stageId, timeSlot);
    let updatedTask: StoredHourlyCbqlSignTask | null = null;
    const idx = hourlySignTasks.findIndex((t) => t.id === id);

    if (idx >= 0) {
      updatedTask = {
        ...hourlySignTasks[idx],
        status: 'signed',
        signedAt: nowTime,
        signedByName: signedByName || hourlySignTasks[idx].cbqlName,
        ...(signatureDataUrl ? { cbqlSignatureDataUrl: signatureDataUrl } : {}),
      };
      hourlySignTasks[idx] = updatedTask;
    } else if (chuyen && stageId && timeSlot) {
      const nowMs = Date.now();
      updatedTask = {
        id,
        dateStr,
        factory: String(body.factory || ''),
        line: String(body.line || ''),
        chuyen,
        stageId,
        stageName: String(body.stageName || stageId),
        timeSlot,
        qcName: normAuthName(body.qcName || ''),
        qaName: normAuthName(body.qaName || ''),
        cbqlName: signedByName,
        status: 'signed',
        notifiedCbqlAt: new Date(nowMs).toISOString(),
        notifiedCbqlAtMs: nowMs,
        signedAt: nowTime,
        signedByName,
        ...(signatureDataUrl ? { cbqlSignatureDataUrl: signatureDataUrl } : {}),
        triggerSource: 'scheduled_hourly',
      };
      hourlySignTasks.unshift(updatedTask);
    }

    if (updatedTask) {
      saveHourlySignTasks(hourlySignTasks);
      await persistHourlyTasksToMasterSheet([updatedTask]);
      broadcastSseEvent('hourly_cbql_signed', { task: updatedTask });
    }

    return res.json({ ok: true, task: updatedTask, tasks: getTodayHourlyTasks() });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Lỗi cập nhật trạng thái ký giờ CBQL.' });
  }
});

app.post('/api/hourly-sign/:id/sign', async (req, res) => {
  try {
    const { id } = req.params;
    const { signerName, signatureDataUrl } = req.body || {};
    if (!String(signerName || '').trim() || !String(signatureDataUrl || '').startsWith('data:image/')) {
      return res.status(400).json({ ok: false, error: 'Vui lòng nhập họ tên và vẽ chữ ký CBQL.' });
    }

    const idx = hourlySignTasks.findIndex((t) => t.id === id);
    if (idx === -1) {
      return res.status(404).json({ ok: false, error: 'Không tìm thấy yêu cầu ký giờ này.' });
    }

    const cleanName = normAuthName(signerName);
    const nowTime = new Intl.DateTimeFormat('vi-VN', {
      timeZone: 'Asia/Ho_Chi_Minh',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).format(new Date());

    const updatedTask: StoredHourlyCbqlSignTask = {
      ...hourlySignTasks[idx],
      cbqlName: cleanName,
      status: 'signed',
      signedAt: nowTime,
      signedByName: cleanName,
      cbqlSignatureDataUrl: signatureDataUrl,
    };
    hourlySignTasks[idx] = updatedTask;
    saveHourlySignTasks(hourlySignTasks);
    await persistHourlyTasksToMasterSheet([updatedTask]);

    const qcKey = compareAuthNameKey(updatedTask.qcName);
    const qaKey = compareAuthNameKey(updatedTask.qaName);
    const targetSubs = pushSubscriptions.filter(
      (s) =>
        (qcKey && compareAuthNameKey(s.userName) === qcKey) ||
        (qaKey && compareAuthNameKey(s.userName) === qaKey) ||
        s.normalizedRole === 'QC' ||
        s.normalizedRole === 'QA'
    );
    await sendWebPushToSubscriptions(targetSubs, {
      title: `✅ CBQL đã ký xác nhận giờ (${updatedTask.timeSlot})`,
      body: `${updatedTask.chuyen} (${updatedTask.stageName}): CBQL ${cleanName} đã ký lúc ${nowTime}. Chữ ký đã tự động đồng bộ về máy QC!`,
      type: 'hourly_cbql_signed',
      hourlySignId: updatedTask.id,
      tag: `rft-hourly-signed-${updatedTask.id}`,
      url: `/?hourlySignId=${encodeURIComponent(updatedTask.id)}`,
    });

    broadcastSseEvent('hourly_cbql_signed', { task: updatedTask });
    return res.json({ ok: true, task: updatedTask, tasks: getTodayHourlyTasks() });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Lỗi lưu chữ ký giờ CBQL.' });
  }
});

// ============================================================================
// DAILY AFTER-20:00 INCOMPLETE LINE AUDIT (SCOPED BY QA / FULL FOR ADMIN)
// ============================================================================
const ROSTER_SNAPSHOT_FILE = '/tmp/rft_roster_snapshot.json';
const AFTER_20H_AUDIT_LOG_FILE = '/tmp/rft_after_20h_audit_log.json';
const SYNCED_COMPLETED_STATIONS_FILE = '/tmp/rft_synced_completed_stations.json';

interface StoredRosterSnapshot {
  updatedAtMs: number;
  entries: FactoryLocationEntry[];
}

interface StoredSyncedCompletedStation {
  dateStr: string;
  factory: string;
  line: string;
  chuyen: string;
  stageId: string;
}

interface After20hAuditLog {
  dateStr: string;
  dispatchedAt?: string;
}

interface ServerIncompleteStationDetail {
  viTri: string;
  qcName: string;
  reason: 'no_report' | 'not_closed' | 'waiting_signatures';
  reasonLabel: string;
}

interface ServerIncompleteLineAuditItem {
  key: string;
  factory: string;
  line: string;
  qaName: string;
  totalStations: number;
  completedStations: number;
  incompleteStations: ServerIncompleteStationDetail[];
  statusLabel: string;
}

interface ServerAfter20hQaAuditSummary {
  qaName: string;
  totalManagedLines: number;
  completedManagedLines: number;
  incompleteLines: ServerIncompleteLineAuditItem[];
}

interface ServerAfter20hIncompleteAuditReport {
  dateStr: string;
  displayDate: string;
  currentTimeVn: string;
  isAfter20h: boolean;
  generatedAt: string;
  triggeredBy?: string;
  totalLines: number;
  completedLinesCount: number;
  incompleteLines: ServerIncompleteLineAuditItem[];
  byQa: ServerAfter20hQaAuditSummary[];
}

function loadRosterSnapshot(): StoredRosterSnapshot {
  try {
    if (fs.existsSync(ROSTER_SNAPSHOT_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(ROSTER_SNAPSHOT_FILE, 'utf8'));
      if (parsed && Array.isArray(parsed.entries) && parsed.entries.length > 0) {
        return parsed;
      }
    }
  } catch {
    // ignore
  }
  return { updatedAtMs: 0, entries: [] };
}

function saveRosterSnapshot(snapshot: StoredRosterSnapshot) {
  try {
    fs.writeFileSync(ROSTER_SNAPSHOT_FILE, JSON.stringify(snapshot, null, 2), 'utf8');
  } catch {
    // ignore
  }
}

function loadSyncedCompletedStations(): StoredSyncedCompletedStation[] {
  try {
    if (fs.existsSync(SYNCED_COMPLETED_STATIONS_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(SYNCED_COMPLETED_STATIONS_FILE, 'utf8'));
      if (Array.isArray(parsed)) return parsed;
    }
  } catch {
    // ignore
  }
  return [];
}

function saveSyncedCompletedStations(list: StoredSyncedCompletedStation[]) {
  try {
    fs.writeFileSync(SYNCED_COMPLETED_STATIONS_FILE, JSON.stringify(list.slice(0, 400), null, 2), 'utf8');
  } catch {
    // ignore
  }
}

function loadAfter20hAuditLog(): After20hAuditLog {
  const { dateStr } = getVietnamClock();
  try {
    if (fs.existsSync(AFTER_20H_AUDIT_LOG_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(AFTER_20H_AUDIT_LOG_FILE, 'utf8'));
      if (parsed && parsed.dateStr === dateStr) return parsed;
    }
  } catch {
    // ignore
  }
  return { dateStr };
}

function saveAfter20hAuditLog(log: After20hAuditLog) {
  try {
    fs.writeFileSync(AFTER_20H_AUDIT_LOG_FILE, JSON.stringify(log, null, 2), 'utf8');
  } catch {
    // ignore
  }
}

let rosterSnapshotCache: StoredRosterSnapshot = loadRosterSnapshot();
let syncedCompletedStations: StoredSyncedCompletedStation[] = loadSyncedCompletedStations();
let after20hAuditLog: After20hAuditLog = loadAfter20hAuditLog();

async function getAuditRosterEntries(): Promise<FactoryLocationEntry[]> {
  const now = Date.now();
  if (rosterSnapshotCache.entries.length > 0 && now - rosterSnapshotCache.updatedAtMs < 5 * 60_000) {
    return rosterSnapshotCache.entries;
  }

  try {
    await ensureServerConfigHydrated();
    const gasUrl = resolveValidGasUrl();
    const configuredSheetUrl =
      sharedServerConfig.sheetSourceUrl &&
      sharedServerConfig.sheetSourceUrl.includes('docs.google.com/spreadsheets/d/')
        ? sharedServerConfig.sheetSourceUrl
        : RFT_MASTER_SHEET_URL;

    let rosterRows = await fetchServerSheetRowsByTabOrGid(
      configuredSheetUrl,
      'Danh sách',
      '0',
      gasUrl || undefined
    );
    if (rosterRows.length === 0 && configuredSheetUrl !== RFT_MASTER_SHEET_URL) {
      rosterRows = await fetchServerSheetRowsByTabOrGid(
        RFT_MASTER_SHEET_URL,
        'Danh sách',
        '0',
        gasUrl || undefined
      );
    }

    if (rosterRows.length > 0) {
      const parsedEntries: FactoryLocationEntry[] = [];
      rosterRows.forEach((row) => {
        const usedKeys = new Set<string>();
        const nhaMayRes = getServerRowCellExactFirst(row, ['Nhà máy', 'Nha may', 'Factory'], 0, usedKeys);
        if (nhaMayRes.matchedKey) usedKeys.add(nhaMayRes.matchedKey);
        const lineRes = getServerRowCellExactFirst(row, ['Line', 'Chuyền', 'Chuyen'], 1, usedKeys);
        if (lineRes.matchedKey) usedKeys.add(lineRes.matchedKey);
        const viTriRes = getServerRowCellExactFirst(row, ['Vị trí', 'Vi tri', 'Trạm', 'Station'], 2, usedKeys);
        if (viTriRes.matchedKey) usedKeys.add(viTriRes.matchedKey);
        const qaRes = getServerRowCellExactFirst(row, ['QA', 'NV QA', 'QA phụ trách'], 3, usedKeys);
        if (qaRes.matchedKey) usedKeys.add(qaRes.matchedKey);
        const qcRes = getServerRowCellExactFirst(row, ['QC', 'CN QC', 'QC kiểm tra'], 4, usedKeys);
        if (qcRes.matchedKey) usedKeys.add(qcRes.matchedKey);

        const viTri = viTriRes.value;
        const line = lineRes.value;
        if (!viTri && !line) return;

        const nhaMay =
          nhaMayRes.value ||
          (viTri.includes('TS1')
            ? 'Thoại Sơn 1'
            : viTri.includes('TS2')
            ? 'Thoại Sơn 2'
            : 'Hoàn thiện đế');

        parsedEntries.push({
          nhaMay,
          line: line || viTri,
          viTri: viTri || line,
          qa: qaRes.value,
          qc: qcRes.value,
        });
      });

      if (parsedEntries.length > 0) {
        rosterSnapshotCache = { updatedAtMs: now, entries: parsedEntries };
        saveRosterSnapshot(rosterSnapshotCache);
        return parsedEntries;
      }
    }
  } catch {
    // fallback below
  }

  return rosterSnapshotCache.entries.length > 0
    ? rosterSnapshotCache.entries
    : FACTORY_MASTER_DATA;
}

async function computeTodayLineAuditReport(
  triggeredBy?: string
): Promise<ServerAfter20hIncompleteAuditReport> {
  const vnNow = getVietnamClock();
  const todayStr = vnNow.dateStr;
  const roster = await getAuditRosterEntries();

  const todayEods = pendingEodList.filter((item) => {
    const repDate = String(item.report?.dateStr || '').trim();
    if (repDate === todayStr) return true;
    const createdVn = item.createdAt ? getVietnamClock(new Date(item.createdAt)).dateStr : '';
    return createdVn === todayStr;
  });

  const todayActivities = qcDailyActivities.filter((a) => a.dateStr === todayStr);
  const todaySyncedCompleted = syncedCompletedStations.filter((s) => s.dateStr === todayStr);

  // Nhóm các trạm theo từng Chuyền (Factory + Line)
  const lineMap = new Map<
    string,
    {
      key: string;
      factory: string;
      line: string;
      qaNames: Set<string>;
      stations: Array<{ viTri: string; qcName: string; qaName: string }>;
    }
  >();

  roster.forEach((entry) => {
    const factory = String(entry.nhaMay || '').trim() || 'Khác';
    const line = String(entry.line || entry.viTri || '').trim();
    const viTri = String(entry.viTri || entry.line || '').trim();
    if (!line || !viTri) return;

    const lineKey = `${normAccentFreeUpper(factory)}||${normAccentFreeUpper(line)}`;
    if (!lineMap.has(lineKey)) {
      lineMap.set(lineKey, {
        key: lineKey,
        factory,
        line,
        qaNames: new Set<string>(),
        stations: [],
      });
    }
    const grp = lineMap.get(lineKey)!;
    splitPersonnelCellNames(entry.qa).forEach((q) => grp.qaNames.add(q));

    if (!grp.stations.some((st) => normAccentFreeUpper(st.viTri) === normAccentFreeUpper(viTri))) {
      grp.stations.push({
        viTri,
        qcName: String(entry.qc || '').trim(),
        qaName: String(entry.qa || '').trim(),
      });
    }
  });

  const incompleteLines: ServerIncompleteLineAuditItem[] = [];
  const qaBucketMap = new Map<
    string,
    {
      qaDisplayName: string;
      totalManagedLines: number;
      completedManagedLines: number;
      incompleteLines: ServerIncompleteLineAuditItem[];
    }
  >();

  let totalLines = 0;
  let completedLinesCount = 0;

  for (const grp of lineMap.values()) {
    totalLines++;
    const lineNorm = normAccentFreeUpper(grp.line);
    const factoryNorm = normAccentFreeUpper(grp.factory);
    const incompleteStations: ServerIncompleteStationDetail[] = [];
    let completedStations = 0;

    for (const st of grp.stations) {
      const viTriNorm = normAccentFreeUpper(st.viTri);

      const isSyncedDone = todaySyncedCompleted.some(
        (sc) =>
          normAccentFreeUpper(sc.chuyen) === viTriNorm ||
          (normAccentFreeUpper(sc.line) === lineNorm &&
            (!sc.factory || normAccentFreeUpper(sc.factory) === factoryNorm))
      );

      const matchedEods = todayEods.filter((item) => {
        const repChuyenNorm = normAccentFreeUpper(item.report?.chuyen);
        const repLineNorm = normAccentFreeUpper(item.report?.lineSanXuat);
        return repChuyenNorm === viTriNorm || (repLineNorm && repLineNorm === lineNorm && !repChuyenNorm);
      });

      const hasCompletedEod =
        isSyncedDone ||
        matchedEods.some(
          (item) =>
            item.status === 'completed' ||
            (Boolean(item.report?.qcKiemTraSign || item.report?.qcSignedAt) &&
              Boolean(item.report?.qaPhuTrachSign || item.report?.qaSignedAt) &&
              Boolean(item.report?.cbqlXacNhanSign || item.report?.cbqlSignedAt))
        );

      if (hasCompletedEod) {
        completedStations++;
        continue;
      }

      const pendingEod = matchedEods.find((item) => item.status === 'pending');
      if (pendingEod) {
        incompleteStations.push({
          viTri: st.viTri,
          qcName: st.qcName || pendingEod.createdBy || '',
          reason: 'waiting_signatures',
          reasonLabel: String(pendingEod.report?.eodStatus || 'Chờ ký đủ chữ ký chốt ca'),
        });
        continue;
      }

      const hasUnclosedActivity = todayActivities.some((act) => {
        const actChuyenNorm = normAccentFreeUpper(act.chuyen);
        return actChuyenNorm === viTriNorm && !act.hasClosedShift;
      });

      if (hasUnclosedActivity) {
        incompleteStations.push({
          viTri: st.viTri,
          qcName: st.qcName,
          reason: 'not_closed',
          reasonLabel: 'Đã nhập giờ nhưng chưa chốt ca',
        });
      } else {
        incompleteStations.push({
          viTri: st.viTri,
          qcName: st.qcName,
          reason: 'no_report',
          reasonLabel: 'Chưa gửi báo cáo hoàn thành',
        });
      }
    }

    const qaDisplay = Array.from(grp.qaNames).join(', ') || 'Chưa phân công QA';
    const isLineDone = incompleteStations.length === 0;
    if (isLineDone) {
      completedLinesCount++;
    } else {
      const reasonsSet = new Set(incompleteStations.map((s) => s.reason));
      const statusLabel =
        reasonsSet.size === 1 && reasonsSet.has('waiting_signatures')
          ? 'Chờ ký xác nhận chốt ca'
          : completedStations > 0
          ? `Còn ${incompleteStations.length}/${grp.stations.length} vị trí chưa xong`
          : 'Chưa hoàn thành báo cáo ngày';

      incompleteLines.push({
        key: grp.key,
        factory: grp.factory,
        line: grp.line,
        qaName: qaDisplay,
        totalStations: grp.stations.length,
        completedStations,
        incompleteStations,
        statusLabel,
      });
    }

    // Phân bổ chính xác từng chuyền về cho QA quản lý chuyền đó
    grp.qaNames.forEach((singleQa) => {
      const qaKey = compareAuthNameKey(singleQa);
      if (!qaKey) return;
      if (!qaBucketMap.has(qaKey)) {
        qaBucketMap.set(qaKey, {
          qaDisplayName: singleQa,
          totalManagedLines: 0,
          completedManagedLines: 0,
          incompleteLines: [],
        });
      }
      const bucket = qaBucketMap.get(qaKey)!;
      bucket.totalManagedLines++;
      if (isLineDone) {
        bucket.completedManagedLines++;
      } else {
        bucket.incompleteLines.push({
          key: grp.key,
          factory: grp.factory,
          line: grp.line,
          qaName: singleQa,
          totalStations: grp.stations.length,
          completedStations,
          incompleteStations,
          statusLabel:
            completedStations > 0
              ? `Còn ${incompleteStations.length}/${grp.stations.length} vị trí chưa xong`
              : 'Chưa hoàn thành báo cáo ngày',
        });
      }
    });
  }

  const byQa: ServerAfter20hQaAuditSummary[] = Array.from(qaBucketMap.values())
    .map((b) => ({
      qaName: b.qaDisplayName,
      totalManagedLines: b.totalManagedLines,
      completedManagedLines: b.completedManagedLines,
      incompleteLines: b.incompleteLines,
    }))
    .sort((a, b) => a.qaName.localeCompare(b.qaName, 'vi'));

  return {
    dateStr: todayStr,
    displayDate: vnNow.displayDate,
    currentTimeVn: vnNow.hhmm,
    isAfter20h: vnNow.minutesOfDay >= 20 * 60,
    generatedAt: new Date().toISOString(),
    triggeredBy,
    totalLines,
    completedLinesCount,
    incompleteLines,
    byQa,
  };
}

async function dispatchAfter20hIncompleteAudit(options?: {
  triggeredBy?: string;
  force?: boolean;
}) {
  const report = await computeTodayLineAuditReport(options?.triggeredBy || 'Lịch tự động sau 20h00');
  let qaSentCount = 0;
  let adminSentCount = 0;
  let qaTargetedDevices = 0;
  let adminTargetedDevices = 0;

  // 1. Gửi cho từng tài khoản QA đúng các chuyền thuộc quản lý của QA đó chưa hoàn thành
  for (const qaSummary of report.byQa) {
    if (qaSummary.incompleteLines.length === 0) continue;
    const qaKey = compareAuthNameKey(qaSummary.qaName);
    const qaSubs = pushSubscriptions.filter(
      (s) => s.normalizedRole === 'QA' && compareAuthNameKey(s.userName) === qaKey
    );
    qaTargetedDevices += qaSubs.length;
    if (qaSubs.length === 0) continue;

    const linesPreview = qaSummary.incompleteLines
      .slice(0, 3)
      .map(
        (l) =>
          `${l.line} (${l.factory}): ${l.incompleteStations
            .slice(0, 3)
            .map((st) => st.viTri)
            .join(', ')}${l.incompleteStations.length > 3 ? ` +${l.incompleteStations.length - 3}` : ''}`
      )
      .join(' | ');
    const moreText =
      qaSummary.incompleteLines.length > 3
        ? ` và ${qaSummary.incompleteLines.length - 3} chuyền khác.`
        : '.';

    const pushRes = await sendWebPushToSubscriptions(qaSubs, {
      title: `📋 Sau 20h00: Còn ${qaSummary.incompleteLines.length}/${qaSummary.totalManagedLines} chuyền của bạn chưa xong (${report.displayDate})`,
      body: `QA ${qaSummary.qaName}: ${linesPreview}${moreText} Chạm để xem & đôn đốc hoàn thành báo cáo!`,
      type: 'daily_20h_audit',
      tag: `rft-20h-qa-${report.dateStr}-${qaKey}`,
      url: '/?openDailyAudit=1',
    });
    qaSentCount += pushRes.sentCount;
  }

  // 2. Gửi FULL toàn bộ cho Admin
  const adminSubs = pushSubscriptions.filter((s) => s.normalizedRole === 'ADMIN');
  adminTargetedDevices = adminSubs.length;
  if (adminSubs.length > 0 && (report.incompleteLines.length > 0 || options?.force)) {
    const adminBody =
      report.incompleteLines.length > 0
        ? `Toàn hệ thống còn ${report.incompleteLines.length}/${report.totalLines} chuyền chưa hoàn thành báo cáo: ${report.incompleteLines
            .slice(0, 4)
            .map((l) => `${l.line} (${l.factory})`)
            .join(', ')}${
            report.incompleteLines.length > 4
              ? ` +${report.incompleteLines.length - 4} chuyền khác`
              : ''
          }.`
        : `Tuyệt vời! Toàn bộ ${report.totalLines}/${report.totalLines} chuyền đã hoàn thành báo cáo ngày ${report.displayDate}.`;

    const pushAdminRes = await sendWebPushToSubscriptions(adminSubs, {
      title:
        report.incompleteLines.length > 0
          ? `📊 Tổng hợp sau 20h00 (Full): ${report.incompleteLines.length}/${report.totalLines} chuyền chưa xong`
          : `✅ Tổng hợp sau 20h00 (Full): 100% chuyền đã hoàn thành`,
      body: adminBody,
      type: 'daily_20h_audit',
      tag: `rft-20h-admin-${report.dateStr}`,
      url: '/?openDailyAudit=1',
    });
    adminSentCount = pushAdminRes.sentCount;
  }

  const nowIso = new Date().toISOString();
  after20hAuditLog = {
    dateStr: report.dateStr,
    dispatchedAt: nowIso,
  };
  saveAfter20hAuditLog(after20hAuditLog);

  broadcastSseEvent('daily_20h_incomplete_audit', { report });

  return {
    report,
    qaSentCount,
    adminSentCount,
    qaTargetedDevices,
    adminTargetedDevices,
  };
}

let isRunningAfter20hAudit = false;
async function runAfter20hIncompleteAuditScheduler() {
  if (isRunningAfter20hAudit) return;
  isRunningAfter20hAudit = true;
  try {
    const vnNow = getVietnamClock();
    if (after20hAuditLog.dateStr !== vnNow.dateStr) {
      after20hAuditLog = { dateStr: vnNow.dateStr };
      saveAfter20hAuditLog(after20hAuditLog);
    }

    // Chỉ chạy sau 20h00 giờ Việt Nam và gửi 1 lần tự động mỗi ngày
    if (vnNow.minutesOfDay < 20 * 60) return;
    if (after20hAuditLog.dispatchedAt) return;

    const skipSunday = sharedServerConfig.qcReminderSkipSunday !== false;
    const hasActivityToday =
      qcDailyActivities.some((a) => a.dateStr === vnNow.dateStr) ||
      pendingEodList.some((p) => String(p.report?.dateStr || '') === vnNow.dateStr);
    if (skipSunday && vnNow.weekdaySun0 === 0 && !hasActivityToday) return;

    await dispatchAfter20hIncompleteAudit({
      triggeredBy: 'Hệ thống tự động sau 20h00',
    });
  } catch (err) {
    console.error('After-20h incomplete audit scheduler error:', err);
  } finally {
    isRunningAfter20hAudit = false;
  }
}

app.get('/api/daily-audit/after-20h', async (_req, res) => {
  try {
    const report = await computeTodayLineAuditReport();
    return res.json({
      ok: true,
      report,
      lastDispatchedAt: after20hAuditLog.dateStr === report.dateStr ? after20hAuditLog.dispatchedAt || null : null,
    });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Không kiểm tra được báo cáo sau 20h00.' });
  }
});

app.post('/api/daily-audit/after-20h/trigger', async (req, res) => {
  try {
    const triggeredBy = String(req.body?.triggeredBy || 'Kiểm tra thủ công').trim();
    const result = await dispatchAfter20hIncompleteAudit({
      triggeredBy,
      force: true,
    });
    return res.json({ ok: true, ...result });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Không gửi được báo cáo sau 20h00.' });
  }
});

app.post('/api/daily-audit/sync-state', (req, res) => {
  try {
    const body = req.body || {};
    if (Array.isArray(body.roster) && body.roster.length > 0) {
      const cleanedRoster: FactoryLocationEntry[] = body.roster
        .filter((r: any) => r && (r.viTri || r.line))
        .map((r: any) => ({
          nhaMay: String(r.nhaMay || ''),
          line: String(r.line || r.viTri || ''),
          viTri: String(r.viTri || r.line || ''),
          qa: String(r.qa || ''),
          qc: String(r.qc || ''),
          ...(r.cbql ? { cbql: String(r.cbql) } : {}),
          ...(r.thuKho ? { thuKho: String(r.thuKho) } : {}),
        }));
      if (cleanedRoster.length > 0) {
        rosterSnapshotCache = { updatedAtMs: Date.now(), entries: cleanedRoster };
        saveRosterSnapshot(rosterSnapshotCache);
      }
    }

    if (Array.isArray(body.completedStations) && body.completedStations.length > 0) {
      const existingKeys = new Set(
        syncedCompletedStations.map((s) => `${s.dateStr}|${s.chuyen}|${s.stageId}`)
      );
      let changed = false;
      body.completedStations.forEach((s: any) => {
        const dateStr = String(s?.dateStr || '').trim();
        const chuyen = String(s?.chuyen || '').trim();
        const stageId = String(s?.stageId || '').trim();
        if (!dateStr || !chuyen) return;
        const k = `${dateStr}|${chuyen}|${stageId}`;
        if (!existingKeys.has(k)) {
          existingKeys.add(k);
          syncedCompletedStations.unshift({
            dateStr,
            factory: String(s.factory || ''),
            line: String(s.line || ''),
            chuyen,
            stageId,
          });
          changed = true;
        }
      });
      if (changed) {
        const todayStr = getVietnamClock().dateStr;
        syncedCompletedStations = syncedCompletedStations.filter((s) => s.dateStr === todayStr);
        saveSyncedCompletedStations(syncedCompletedStations);
      }
    }

    return res.json({ ok: true });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Lỗi đồng bộ trạng thái chuyền.' });
  }
});

// =============================================================================
// IN-PROGRESS REPORT FORM DRAFTS (PER-ACCOUNT, SURVIVES LOGOUT / RELOAD / RESTART)
// =============================================================================
const DRAFTS_FILE = path.join(os.tmpdir(), 'qc_report_tbs_user_drafts_v1.json');
const QUALITY_ALERTS_FILE = path.join(os.tmpdir(), 'qc_report_tbs_quality_alerts_v1.json');
const ACCOUNT_PRESENCE_FILE = path.join(os.tmpdir(), 'qc_report_tbs_account_presence_v1.json');

const SESSION_TOKEN_SECRET =
  (process.env.SESSION_SIGNING_SECRET || '').trim() || crypto.randomBytes(32).toString('hex');

function isUnsafeObjectKey(key: string): boolean {
  return key === '__proto__' || key === 'constructor' || key === 'prototype';
}

function normalizeServerUserKey(usernameOrName: string): string {
  return String(usernameOrName || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/đ/g, 'd')
    .replace(/Đ/g, 'D')
    .replace(/\s+/g, '_')
    .replace(/[^a-zA-Z0-9_-]/g, '')
    .toLowerCase()
    .trim();
}

function isServerAdminRole(role?: string): boolean {
  const upper = normAccentFreeUpper(role);
  return (
    upper === 'ADMIN' ||
    upper === 'SUPER_ADMIN' ||
    upper === 'SUPER ADMIN' ||
    upper === 'QUAN TRI' ||
    upper === 'QUAN TRI VIEN'
  );
}

function isServerManagementMonitorRole(role?: string, chucVu?: string): boolean {
  if (isServerAdminRole(role)) return true;
  const r = normAccentFreeUpper(role);
  const cv = normAccentFreeUpper(chucVu);
  const combined = `${r} | ${cv}`;
  return (
    r === 'TP_LEADER' ||
    r === 'T.TEAM_LEADER' ||
    r === 'MANAGER' ||
    r === 'PGD' ||
    r === 'TP' ||
    r === 'QPS' ||
    r === 'IPL' ||
    /\bQPS\b/.test(combined) ||
    /\bIPL\b/.test(combined) ||
    combined.includes('IMPLANT') ||
    combined.includes('INPLANT') ||
    combined.includes('IN-PLANT') ||
    /\bTP\b/.test(combined) ||
    combined.includes('TP.QLCL') ||
    combined.includes('TRUONG PHONG') ||
    combined.includes('T.TEAM') ||
    combined.includes('T TEAM') ||
    combined.includes('TRUONG TEAM') ||
    combined.includes('TEAM LEADER') ||
    /\bPGD\b/.test(combined) ||
    combined.includes('PHO GIAM DOC') ||
    /\bTGD\b/.test(combined) ||
    combined.includes('TONG GIAM DOC') ||
    combined.includes('BAN GIAM DOC')
  );
}

function classifyMonitorRoleCategory(
  role?: string,
  chucVu?: string
): 'ADMIN' | 'MANAGEMENT' | 'QA' | 'QC' | 'CBQL' | 'THU_KHO' {
  if (isServerAdminRole(role)) return 'ADMIN';
  if (isServerManagementMonitorRole(role, chucVu)) return 'MANAGEMENT';
  const pushCat = normalizePushRoleCategory(role, chucVu);
  if (pushCat === 'ADMIN') return 'ADMIN';
  if (pushCat === 'QA') return 'QA';
  if (pushCat === 'THU_KHO') return 'THU_KHO';
  if (pushCat === 'CBQL') return 'CBQL';
  return 'QC';
}

interface SignedSessionPayload {
  userKey: string;
  fullName: string;
  role: string;
  chucVu: string;
  exp: number;
}

function signServerSessionToken(user: { fullName: string; role: string; chucVu?: string }): string {
  const payload: SignedSessionPayload = {
    userKey: normalizeServerUserKey(user.fullName),
    fullName: String(user.fullName || '').trim(),
    role: String(user.role || '').trim(),
    chucVu: String(user.chucVu || '').trim(),
    exp: Date.now() + 7 * 24 * 60 * 60 * 1000,
  };
  const payloadB64 = Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
  const sig = crypto
    .createHmac('sha256', SESSION_TOKEN_SECRET)
    .update(payloadB64)
    .digest('base64url');
  return `${payloadB64}.${sig}`;
}

function verifyServerSessionToken(token: string): SignedSessionPayload | null {
  try {
    const clean = String(token || '').trim();
    const dot = clean.indexOf('.');
    if (dot <= 0) return null;
    const payloadB64 = clean.slice(0, dot);
    const sig = clean.slice(dot + 1);
    const expectedSig = crypto
      .createHmac('sha256', SESSION_TOKEN_SECRET)
      .update(payloadB64)
      .digest('base64url');
    const a = Buffer.from(sig, 'utf8');
    const b = Buffer.from(expectedSig, 'utf8');
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
    const parsed = JSON.parse(Buffer.from(payloadB64, 'base64url').toString('utf8')) as SignedSessionPayload;
    if (!parsed || !parsed.fullName || typeof parsed.exp !== 'number' || parsed.exp < Date.now()) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

interface SafeTkMkAccountInfo {
  userKey: string;
  fullName: string;
  chucVu: string;
  role: string;
  roleCategory: 'ADMIN' | 'MANAGEMENT' | 'QA' | 'QC' | 'CBQL' | 'THU_KHO';
  accountStatus: string;
  avatarUrl: string;
}

let safeTkMkAccountsCache: SafeTkMkAccountInfo[] = [];
let safeTkMkAccountsFetchedAt = 0;

function updateSafeTkMkCacheFromRows(rows: Array<Record<string, any>>): void {
  if (!Array.isArray(rows) || rows.length === 0) return;
  const byKey = new Map<string, SafeTkMkAccountInfo>();
  for (const row of rows) {
    const fullName = normAuthName(getServerRowCell(row, ['TÊN NGƯỜI DÙNG', 'TEN NGUOI DUNG'], 0));
    if (!fullName) continue;
    const userKey = normalizeServerUserKey(fullName);
    if (!userKey || isUnsafeObjectKey(userKey)) continue;
    const chucVu = getServerRowCell(row, ['CHỨC VỤ', 'CHUC VU'], 1);
    const role = getServerRowCell(row, ['Vai trò', 'Vai tro'], 3) || 'QC_User';
    const status = getServerRowCell(row, ['Trạng thái', 'Trang thai'], 5) || 'ACTIVE';
    const rawAvatarUrl = getServerRowCell(row, ['Hình ảnh TK', 'Hinh anh TK'], 8);
    byKey.set(userKey, {
      userKey,
      fullName,
      chucVu: chucVu || role,
      role,
      roleCategory: classifyMonitorRoleCategory(role, chucVu),
      accountStatus: status.toUpperCase(),
      avatarUrl: convertServerAvatarUrl(rawAvatarUrl),
    });
  }
  if (byKey.size > 0) {
    safeTkMkAccountsCache = Array.from(byKey.values());
    safeTkMkAccountsFetchedAt = Date.now();
  }
}

async function ensureSafeTkMkAccountsCached(
  forceRefresh = false,
  overrideGasUrl?: string,
  overrideSheetUrl?: string
): Promise<SafeTkMkAccountInfo[]> {
  if (!forceRefresh && safeTkMkAccountsCache.length > 0 && Date.now() - safeTkMkAccountsFetchedAt < 120_000) {
    return safeTkMkAccountsCache;
  }
  const fetchTask = (async () => {
    try {
      await ensureServerConfigHydrated();
      const validOverrideGas = isValidGasExecUrl(overrideGasUrl);
      const gasUrl = validOverrideGas || resolveValidGasUrl();
      const configuredSheetUrl =
        overrideSheetUrl && overrideSheetUrl.includes('docs.google.com/spreadsheets/d/')
          ? overrideSheetUrl.trim()
          : sharedServerConfig?.sheetSourceUrl &&
            sharedServerConfig.sheetSourceUrl.includes('docs.google.com/spreadsheets/d/')
          ? sharedServerConfig.sheetSourceUrl.trim()
          : RFT_MASTER_SHEET_URL;
      let rows = await fetchServerSheetRowsByTabOrGid(
        configuredSheetUrl,
        TK_MK_SHEET_NAME,
        TK_MK_GID,
        gasUrl || undefined
      );
      if (rows.length === 0 && configuredSheetUrl !== RFT_MASTER_SHEET_URL) {
        rows = await fetchServerSheetRowsByTabOrGid(
          RFT_MASTER_SHEET_URL,
          TK_MK_SHEET_NAME,
          TK_MK_GID,
          gasUrl || undefined
        );
      }
      if (rows.length > 0) {
        updateSafeTkMkCacheFromRows(rows);
      }
    } catch {
      // keep existing cache if network fails
    }
    return safeTkMkAccountsCache;
  })();

  // If we already have cached accounts and not forcing refresh, cap wait at 2.5s; otherwise cap at 6s so HTTP never times out
  const timeoutMs = safeTkMkAccountsCache.length > 0 && !forceRefresh ? 2500 : 6000;
  await Promise.race([
    fetchTask,
    new Promise<void>((resolve) => setTimeout(resolve, timeoutMs)),
  ]);
  return safeTkMkAccountsCache;
}

interface StoredAccountPresence {
  userKey: string;
  fullName: string;
  role: string;
  chucVu: string;
  activeTab: string;
  deviceSummary: string;
  lastSeenAt: string;
  lastSeenAtMs: number;
  lastLoginAt: string;
  lastLogoutAt: string;
  explicitlyLoggedOut: boolean;
}

function loadAccountPresenceStore(): Record<string, StoredAccountPresence> {
  try {
    if (fs.existsSync(ACCOUNT_PRESENCE_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(ACCOUNT_PRESENCE_FILE, 'utf8'));
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return parsed as Record<string, StoredAccountPresence>;
      }
    }
  } catch {
    // ignore corrupt cache
  }
  return {};
}

function saveAccountPresenceStore(store: Record<string, StoredAccountPresence>) {
  try {
    fs.writeFileSync(ACCOUNT_PRESENCE_FILE, JSON.stringify(store), 'utf8');
  } catch {
    // ignore disk error
  }
}

let accountPresenceStore: Record<string, StoredAccountPresence> = loadAccountPresenceStore();
const wsClientUserMap = new Map<WebSocket, { userKey: string; fullName: string }>();

let lastAccountBroadcastAt = 0;
function broadcastAccountStatusChanged(reason: string, userKey?: string): void {
  const now = Date.now();
  if (reason === 'heartbeat' && now - lastAccountBroadcastAt < 2500) return;
  lastAccountBroadcastAt = now;
  broadcastSseEvent('account_status_updated', {
    reason,
    userKey: userKey || '',
    updatedAt: new Date(now).toISOString(),
  });
}

function recordAccountLoginPresence(
  user: { fullName: string; role?: string; chucVu?: string },
  deviceSummary?: string
): void {
  const userKey = normalizeServerUserKey(user.fullName);
  if (!userKey || isUnsafeObjectKey(userKey)) return;
  const nowIso = new Date().toISOString();
  const prev = accountPresenceStore[userKey];
  accountPresenceStore[userKey] = {
    userKey,
    fullName: String(user.fullName || prev?.fullName || '').trim(),
    role: String(user.role || prev?.role || '').trim(),
    chucVu: String(user.chucVu || prev?.chucVu || '').trim(),
    activeTab: prev?.activeTab || 'report',
    deviceSummary: String(deviceSummary || prev?.deviceSummary || '').trim().slice(0, 120),
    lastSeenAt: nowIso,
    lastSeenAtMs: Date.now(),
    lastLoginAt: nowIso,
    lastLogoutAt: prev?.lastLogoutAt || '',
    explicitlyLoggedOut: false,
  };
  saveAccountPresenceStore(accountPresenceStore);
  broadcastAccountStatusChanged('login', userKey);
}

function hasActiveWsForUser(userKey: string): boolean {
  for (const [sock, meta] of wsClientUserMap.entries()) {
    if (meta.userKey === userKey && sock.readyState === WebSocket.OPEN) {
      return true;
    }
  }
  return false;
}

function draftHasRealServerMeasurements(draft: any): boolean {
  if (!draft || typeof draft !== 'object') return false;
  if (Number(draft.tongSoLuongDoikiem || 0) > 0) return true;
  if (Number(draft.tongSoLuongDoiKiemKhongDat || 0) > 0) return true;
  if (Number(draft.materialFailedCount || 0) > 0) return true;
  if (draft.isEODConfirmed) return true;
  if (draft.qcSignatureData || draft.qaSignatureData || draft.cbqlSignatureData) return true;
  if (
    Array.isArray(draft.defects) &&
    draft.defects.some(
      (d: any) =>
        Number(d?.quantity || 0) > 0 ||
        (d?.maSoKiemBreakdown &&
          Object.values(d.maSoKiemBreakdown).some((v) => Number(v || 0) > 0))
    )
  ) {
    return true;
  }
  if (
    Array.isArray(draft.materialEntries) &&
    draft.materialEntries.some(
      (e: any) =>
        Number(e?.tongSoLuongNhap || 0) > 0 ||
        Number(e?.soLuongKiem || 0) > 0 ||
        Number(e?.soLuongLoi || 0) > 0 ||
        Number(e?.capDoNang || 0) > 0 ||
        Number(e?.capDoNhe || 0) > 0 ||
        String(e?.tenVatTu || '').trim().length > 0 ||
        String(e?.maSoVatTu || '').trim().length > 0
    )
  ) {
    return true;
  }
  return false;
}

async function persistJournalItemToMasterSheet(params: {
  journalId: string;
  dateStr: string;
  action: 'USER_DRAFT' | 'USER_DRAFT_DELETE' | 'TEMP_SLOT' | 'TEMP_SLOT_DELETE';
  payloadObj?: Record<string, any>;
}): Promise<boolean> {
  try {
    await ensureServerConfigHydrated();
    const gasUrl = resolveValidGasUrl();
    if (!gasUrl) return false;

    let payloadStr = '';
    if (params.payloadObj) {
      // Compact copy: keep active defects & essential fields within Google Sheet cell limit
      const compact = { ...params.payloadObj };
      if (Array.isArray(compact.defects)) {
        compact.defects = compact.defects.filter(
          (d: any) =>
            Number(d?.quantity || 0) > 0 ||
            (d?.maSoKiemBreakdown && Object.values(d.maSoKiemBreakdown).some((v) => Number(v || 0) > 0))
        );
      }
      if (compact.report && Array.isArray(compact.report.defects)) {
        compact.report = {
          ...compact.report,
          defects: compact.report.defects.filter(
            (d: any) =>
              Number(d?.quantity || 0) > 0 ||
              (d?.maSoKiemBreakdown && Object.values(d.maSoKiemBreakdown).some((v) => Number(v || 0) > 0))
          ),
        };
      }
      payloadStr = JSON.stringify(compact);
    }

    let semanticStr = payloadStr;
    if (params.payloadObj) {
      try {
        const parsedForHash = JSON.parse(payloadStr);
        delete parsedForHash.updatedAt;
        if (parsedForHash.report && typeof parsedForHash.report === 'object') {
          delete parsedForHash.report.timestamp;
          delete parsedForHash.report.submittedAt;
        }
        semanticStr = JSON.stringify(parsedForHash);
      } catch {
        semanticStr = payloadStr;
      }
    }

    const contentHash = crypto
      .createHash('sha1')
      .update(`${params.action}|${semanticStr}`)
      .digest('hex');
    if (lastPersistedReportHashes.get(params.journalId) === contentHash) {
      return true;
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 12000);
    const res = await fetch(gasUrl, {
      method: 'POST',
      redirect: 'follow',
      signal: controller.signal,
      headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: JSON.stringify({
        action: 'submitReport',
        outputUrl: RFT_MASTER_SHEET_URL,
        outputSheetName: APP_REPORTS_SYNC_TAB,
        headers: ['REPORT_ID', 'DATE_STR', 'UPDATED_AT', 'ACTION', 'PAYLOAD_1', 'PAYLOAD_2', 'PAYLOAD_3'],
        rows: [
          {
            REPORT_ID: params.journalId,
            DATE_STR: params.dateStr,
            UPDATED_AT: new Date().toISOString(),
            ACTION: params.action,
            PAYLOAD_1: payloadStr.slice(0, SHEET_CELL_CHUNK_LIMIT),
            PAYLOAD_2: payloadStr.slice(SHEET_CELL_CHUNK_LIMIT, SHEET_CELL_CHUNK_LIMIT * 2),
            PAYLOAD_3: payloadStr.slice(SHEET_CELL_CHUNK_LIMIT * 2, SHEET_CELL_CHUNK_LIMIT * 3),
          },
        ],
      }),
    }).finally(() => clearTimeout(timer));
    const txt = await res.text().catch(() => '');
    if (res.ok && !txt.trim().startsWith('<!DOCTYPE html') && !txt.includes('"status":"error"')) {
      lastPersistedReportHashes.set(params.journalId, contentHash);
      return true;
    }
  } catch {
    // non-blocking journal backup
  }
  return false;
}

const pendingSheetBackupTimers = new Map<string, ReturnType<typeof setTimeout>>();
function scheduleJournalBackup(
  key: string,
  fn: () => Promise<boolean>,
  delayMs = 4500
): void {
  const existing = pendingSheetBackupTimers.get(key);
  if (existing) clearTimeout(existing);
  const timer = setTimeout(() => {
    pendingSheetBackupTimers.delete(key);
    void fn();
  }, delayMs);
  timer.unref?.();
  pendingSheetBackupTimers.set(key, timer);
}

function loadUserDraftsStore(): Record<string, any> {
  try {
    if (fs.existsSync(DRAFTS_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(DRAFTS_FILE, 'utf8'));
      if (parsed && typeof parsed === 'object') return parsed;
    }
  } catch {
    // ignore corrupt cache
  }
  return {};
}

function saveUserDraftsStore(store: Record<string, any>) {
  try {
    fs.writeFileSync(DRAFTS_FILE, JSON.stringify(store), 'utf8');
  } catch {
    // ignore disk error
  }
}

let userDraftsStore: Record<string, any> = loadUserDraftsStore();

app.get('/api/drafts', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  const userKey = normalizeServerUserKey(String(req.query.userKey || ''));
  if (!userKey || isUnsafeObjectKey(userKey)) {
    return res.json({ ok: true, draft: null });
  }
  // Always hydrate App_Bao_Cao_Sync (3s cooldown) so drafts written on Vercel appear immediately in AI Studio
  await ensureReportsHydratedFromSheets(false);
  return res.json({ ok: true, draft: userDraftsStore[userKey] || null });
});

app.post('/api/drafts', (req, res) => {
  try {
    const userKey = normalizeServerUserKey(String(req.body?.userKey || ''));
    const draft = req.body?.draft;
    const forceOverwrite = Boolean(req.body?.forceOverwrite);
    if (!userKey || isUnsafeObjectKey(userKey) || !draft || typeof draft !== 'object' || Array.isArray(draft)) {
      return res.status(400).json({ ok: false, error: 'Thiếu thông tin bản nháp.' });
    }

    const todayStr = getVietnamClock().dateStr;
    const existing = userDraftsStore[userKey];
    const incomingHasData = draftHasRealServerMeasurements(draft);
    const existingHasData = existing && existing.dateStr === todayStr && draftHasRealServerMeasurements(existing);

    // Protect existing same-day working data from accidental empty overwrite during page mount/re-login
    if (!forceOverwrite && existingHasData && !incomingHasData) {
      return res.json({ ok: true, preservedExisting: true, draft: existing });
    }

    const savedDraft = {
      ...draft,
      userKey,
      dateStr: String(draft.dateStr || todayStr).slice(0, 10) || todayStr,
      updatedAt: draft.updatedAt || new Date().toISOString(),
    };
    userDraftsStore[userKey] = savedDraft;
    saveUserDraftsStore(userDraftsStore);

    // Persist draft durably to Master Sheet journal (debounced + semantically deduplicated) so Vercel & Cloud Run stay in sync
    const hasMeaningfulContext =
      incomingHasData ||
      Boolean(String(savedDraft.maGiay || '').trim()) ||
      Boolean(String(savedDraft.maDe || '').trim()) ||
      Boolean(String(savedDraft.maSap || '').trim()) ||
      (Boolean(String(savedDraft.chuyen || '').trim()) && Boolean(String(savedDraft.stageId || '').trim()));
    if (hasMeaningfulContext) {
      scheduleJournalBackup(
        `draft::${userKey}`,
        () =>
          persistJournalItemToMasterSheet({
            journalId: `__USER_DRAFT__::${userKey}`,
            dateStr: savedDraft.dateStr,
            action: 'USER_DRAFT',
            payloadObj: savedDraft,
          }),
        1800
      );
    }

    broadcastAccountStatusChanged('draft_updated', userKey);
    return res.json({ ok: true, draft: savedDraft });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Không lưu được bản nháp.' });
  }
});

app.delete('/api/drafts', (req, res) => {
  const userKey = normalizeServerUserKey(String(req.query.userKey || req.body?.userKey || ''));
  if (userKey && !isUnsafeObjectKey(userKey) && userDraftsStore[userKey]) {
    delete userDraftsStore[userKey];
    saveUserDraftsStore(userDraftsStore);
    scheduleJournalBackup(
      `draft::${userKey}`,
      () =>
        persistJournalItemToMasterSheet({
          journalId: `__USER_DRAFT__::${userKey}`,
          dateStr: getVietnamClock().dateStr,
          action: 'USER_DRAFT_DELETE',
        }),
      1500
    );
    broadcastAccountStatusChanged('draft_cleared', userKey);
  }
  return res.json({ ok: true });
});

// =============================================================================
// SHARED MULTI-SLOT TEMPORARY REPORTS (VISIBLE TO ALL ACCOUNTS IN REAL TIME)
// =============================================================================
const TEMP_SLOT_REPORTS_FILE = path.join(os.tmpdir(), 'qc_report_tbs_temp_slot_reports_v1.json');

interface StoredTempSlotReportEntry {
  slotKey: string;
  dateStr: string;
  updatedAt: string;
  updatedBy: string;
  report: any;
}

function loadTempSlotReportsStore(): Record<string, StoredTempSlotReportEntry> {
  try {
    if (fs.existsSync(TEMP_SLOT_REPORTS_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(TEMP_SLOT_REPORTS_FILE, 'utf8'));
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return parsed as Record<string, StoredTempSlotReportEntry>;
      }
    }
  } catch {
    // ignore corrupt cache
  }
  return {};
}

function saveTempSlotReportsStore(store: Record<string, StoredTempSlotReportEntry>) {
  try {
    fs.writeFileSync(TEMP_SLOT_REPORTS_FILE, JSON.stringify(store), 'utf8');
  } catch {
    // ignore disk error
  }
}

let tempSlotReportsStore: Record<string, StoredTempSlotReportEntry> = loadTempSlotReportsStore();

function pruneTempSlotReportsStore(): void {
  const todayStr = getVietnamClock().dateStr;
  const entries = Object.entries(tempSlotReportsStore).filter(
    ([, item]) => item && item.dateStr === todayStr && item.report && typeof item.report === 'object'
  );
  // Cap at 500 most recently updated active slot entries for today
  entries.sort((a, b) => String(b[1].updatedAt || '').localeCompare(String(a[1].updatedAt || '')));
  const next: Record<string, StoredTempSlotReportEntry> = {};
  for (const [k, v] of entries.slice(0, 500)) {
    next[k] = v;
  }
  tempSlotReportsStore = next;
}

function getTodayTempSlotReports(dateStrOverride?: string): any[] {
  pruneTempSlotReportsStore();
  const targetDate = String(dateStrOverride || '').trim() || getVietnamClock().dateStr;
  return Object.values(tempSlotReportsStore)
    .filter((entry) => entry && entry.dateStr === targetDate && entry.report)
    .map((entry) => entry.report);
}

app.get('/api/temp-slot-reports', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  const dateStr = String(req.query.dateStr || '').trim();
  // Always hydrate App_Bao_Cao_Sync (3s cooldown) so live Vercel temp slots appear immediately in AI Studio
  await ensureReportsHydratedFromSheets(false);
  return res.json({
    ok: true,
    reports: getTodayTempSlotReports(dateStr || undefined),
  });
});

app.post('/api/temp-slot-reports', (req, res) => {
  try {
    const rawSlotKey = String(req.body?.slotKey || '').trim();
    const rawReport = req.body?.report;
    const updatedBy = String(req.body?.updatedBy || '').trim().slice(0, 120);

    if (
      !rawSlotKey ||
      rawSlotKey.length > 240 ||
      isUnsafeObjectKey(rawSlotKey) ||
      /[\r\n\0]/.test(rawSlotKey) ||
      !rawReport ||
      typeof rawReport !== 'object' ||
      Array.isArray(rawReport)
    ) {
      return res.status(400).json({ ok: false, error: 'Dữ liệu báo cáo tạm khung giờ không hợp lệ.' });
    }

    const todayStr = getVietnamClock().dateStr;
    const reportDateStr = String(rawReport.dateStr || todayStr).trim().slice(0, 10) || todayStr;
    const nowIso = new Date().toISOString();

    const storedReport = {
      ...rawReport,
      id: `TEMP-SLOT::${rawSlotKey}`,
      dateStr: reportDateStr,
      syncStatus: 'local_only',
      timestamp: rawReport.timestamp || nowIso,
    };

    const entryRecord: StoredTempSlotReportEntry = {
      slotKey: rawSlotKey,
      dateStr: reportDateStr,
      updatedAt: nowIso,
      updatedBy,
      report: storedReport,
    };
    tempSlotReportsStore[rawSlotKey] = entryRecord;
    pruneTempSlotReportsStore();
    saveTempSlotReportsStore(tempSlotReportsStore);

    scheduleJournalBackup(
      `temp_slot::${rawSlotKey}`,
      () =>
        persistJournalItemToMasterSheet({
          journalId: `__TEMP_SLOT__::${rawSlotKey}`,
          dateStr: reportDateStr,
          action: 'TEMP_SLOT',
          payloadObj: entryRecord,
        }),
      5000
    );

    broadcastSseEvent('temp_slot_report_updated', {
      slotKey: rawSlotKey,
      report: storedReport,
    });
    broadcastAccountStatusChanged('temp_slot_updated', normalizeServerUserKey(updatedBy));

    return res.json({ ok: true, slotKey: rawSlotKey, report: storedReport });
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: err?.message || 'Không lưu được báo cáo tạm khung giờ.',
    });
  }
});

app.delete('/api/temp-slot-reports', (req, res) => {
  const slotKey = String(req.query.slotKey || req.body?.slotKey || '').trim();
  if (slotKey && slotKey.length <= 240 && !isUnsafeObjectKey(slotKey) && tempSlotReportsStore[slotKey]) {
    delete tempSlotReportsStore[slotKey];
    saveTempSlotReportsStore(tempSlotReportsStore);
    scheduleJournalBackup(
      `temp_slot::${slotKey}`,
      () =>
        persistJournalItemToMasterSheet({
          journalId: `__TEMP_SLOT__::${slotKey}`,
          dateStr: getVietnamClock().dateStr,
          action: 'TEMP_SLOT_DELETE',
        }),
      1500
    );
    broadcastSseEvent('temp_slot_report_deleted', { slotKey });
    broadcastAccountStatusChanged('temp_slot_deleted');
  }
  return res.json({ ok: true });
});

// =============================================================================
// REAL-TIME ACCOUNT PRESENCE & LIVE QC MONITOR (RBAC: ADMIN ALL / TP-T.TEAM-PGD QC)
// =============================================================================
const STAGE_LABELS_SERVER: Record<string, string> = {
  VatTu: 'Vật Tư',
  BanTrai: 'Bàn Trải',
  VTGiaCong: 'VT Gia Công',
  VTTheuGiaCong: 'VT Thêu Gia Công',
  Tem: 'Tem',
  DeGiaCong: 'Đế Gia Công',
  DeBTP: 'Đế BTP',
  PhunHat: 'Phun Hạt',
  SonDanLogo: 'Sơn & Dán Logo',
  DeHoanThien: 'Đế Hoàn Thiện',
  May: 'May',
  Go: 'Gò',
};

function buildLiveWorkSnapshotFromDraft(draft: any): any | null {
  if (!draft || typeof draft !== 'object') return null;
  const stageId = String(draft.stageId || '').trim();
  const areaId = String(draft.areaId || '').trim();
  const isMaterial = ['VatTu', 'VTGiaCong', 'VTTheuGiaCong', 'BanTrai', 'Tem', 'DeGiaCong'].includes(stageId);
  const isDefectRateMode = isMaterial || stageId === 'DeBTP' || areaId === 'dau_vao';

  const matEntries: any[] = Array.isArray(draft.materialEntries)
    ? draft.materialEntries.filter(
        (m: any) =>
          m &&
          (String(m.tenVatTu || '').trim() ||
            String(m.maSoVatTu || '').trim() ||
            Number(m.tongSoLuongNhap || 0) > 0 ||
            Number(m.soLuongKiem || 0) > 0 ||
            Number(m.soLuongLoi || 0) > 0)
      )
    : [];

  const activeDefects = Array.isArray(draft.defects)
    ? draft.defects
        .filter((d: any) => Number(d?.quantity || 0) > 0)
        .map((d: any) => ({
          name: String(d.name || '').trim(),
          capDo: String(d.capDo || 'Nặng').trim(),
          kiemSoat: String(d.kiemSoat || '').trim(),
          quantity: Number(d.quantity || 0),
        }))
        .sort((a: any, b: any) => b.quantity - a.quantity)
    : [];

  const matKiem = matEntries.reduce((s, m) => s + Number(m.soLuongKiem || 0), 0);
  const matLoi = matEntries.reduce((s, m) => s + Number(m.soLuongLoi || 0), 0);
  const defectSum = activeDefects.reduce((s: number, d: any) => s + d.quantity, 0);

  const inspectedQty = isMaterial && matKiem > 0 ? matKiem : Number(draft.tongSoLuongDoikiem || 0);
  const failQty = isMaterial
    ? Number(draft.materialFailedCount || 0) || matLoi || defectSum
    : Number(draft.tongSoLuongDoiKiemKhongDat || 0);
  const passQty = Math.max(0, inspectedQty - failQty);
  const defectQtyTotal = isMaterial ? Math.max(matLoi, defectSum) : defectSum;
  const rftPercent =
    !isDefectRateMode && inspectedQty > 0
      ? Number(((passQty / inspectedQty) * 100).toFixed(1))
      : null;
  const defectRatePercent =
    isDefectRateMode && inspectedQty > 0
      ? Number((((stageId === 'DeBTP' ? failQty : defectQtyTotal || failQty) / inspectedQty) * 100).toFixed(2))
      : null;

  return {
    dateStr: String(draft.dateStr || ''),
    updatedAt: String(draft.updatedAt || ''),
    factory: String(draft.factory || ''),
    areaId,
    line: String(draft.line || ''),
    chuyen: String(draft.chuyen || ''),
    stageId,
    stageName: STAGE_LABELS_SERVER[stageId] || stageId || 'Chưa chọn',
    gioKiem: String(draft.gioKiem || ''),
    maGiay: String(draft.maGiay || ''),
    maDe: String(draft.maDe || ''),
    maSap: String(draft.maSap || ''),
    nhaCungCap: String(draft.nhaCungCap || ''),
    inspectedQty,
    passQty,
    failQty,
    defectQtyTotal,
    rftPercent,
    defectRatePercent,
    isDefectRateMode,
    activeDefects,
    materialEntriesCount: matEntries.length,
    materialEntries: matEntries,
    hasQcSign: Boolean(draft.qcSignatureData || draft.qcSignedAt),
    hasQaSign: Boolean(draft.qaSignatureData || draft.qaSignedAt),
    hasCbqlSign: Boolean(draft.cbqlSignatureData || draft.cbqlSignedAt),
    eodStatus: String(draft.eodStatus || 'Chờ QC hoàn tất'),
    isEODConfirmed: Boolean(draft.isEODConfirmed),
    hasRealData: draftHasRealServerMeasurements(draft),
  };
}

function buildLiveWorkSnapshotFromTempReport(rep: any): any | null {
  if (!rep || typeof rep !== 'object') return null;
  const stageId = String(rep.congDoan || '').trim();
  const areaId = String(rep.khuVuc || '').trim();
  const isMaterial = ['VatTu', 'VTGiaCong', 'VTTheuGiaCong', 'BanTrai', 'Tem', 'DeGiaCong'].includes(stageId);
  const isDefectRateMode = isMaterial || stageId === 'DeBTP' || areaId === 'dau_vao';
  const matEntries = Array.isArray(rep.materialEntries) ? rep.materialEntries : [];
  const activeDefects = Array.isArray(rep.defects)
    ? rep.defects
        .filter((d: any) => Number(d?.quantity || 0) > 0)
        .map((d: any) => ({
          name: String(d.name || '').trim(),
          capDo: String(d.capDo || 'Nặng').trim(),
          kiemSoat: String(d.kiemSoat || '').trim(),
          quantity: Number(d.quantity || 0),
        }))
        .sort((a: any, b: any) => b.quantity - a.quantity)
    : [];
  const inspectedQty = Number(rep.materialSummary?.tongSoLuongKiem ?? rep.tongSoLuongDoikiem ?? 0);
  const failQty = Number(rep.materialSummary?.tongSoLuongKiemKhongDat ?? rep.tongSoLuongDoiKiemKhongDat ?? 0);
  const passQty = Math.max(0, Number(rep.tongSoLuongDoiKiemDat ?? inspectedQty - failQty));
  const defectQtyTotal = Number(rep.materialSummary?.tongSoLuongLoiKhongDat ?? rep.tongSoLuongLoiKhongDat ?? failQty);
  const rftPercent =
    !isDefectRateMode && inspectedQty > 0
      ? Number(((passQty / inspectedQty) * 100).toFixed(1))
      : null;
  const defectRatePercent =
    isDefectRateMode && inspectedQty > 0
      ? Number((((stageId === 'DeBTP' ? failQty : defectQtyTotal || failQty) / inspectedQty) * 100).toFixed(2))
      : null;

  return {
    dateStr: String(rep.dateStr || ''),
    updatedAt: String(rep.timestamp || ''),
    factory: String(rep.nhaMay || ''),
    areaId,
    line: String(rep.lineSanXuat || ''),
    chuyen: String(rep.chuyen || ''),
    stageId,
    stageName: String(rep.congDoanTen || STAGE_LABELS_SERVER[stageId] || stageId),
    gioKiem: String(rep.gioKiem || ''),
    maGiay: String(rep.maGiay || ''),
    maDe: String(rep.maDe || ''),
    maSap: String(rep.maSap || ''),
    nhaCungCap: String(rep.nhaCungCap || ''),
    inspectedQty,
    passQty,
    failQty,
    defectQtyTotal,
    rftPercent,
    defectRatePercent,
    isDefectRateMode,
    activeDefects,
    materialEntriesCount: matEntries.length,
    materialEntries: matEntries,
    hasQcSign: Boolean(rep.qcKiemTraSign || rep.qcSignedAt),
    hasQaSign: Boolean(rep.qaPhuTrachSign || rep.qaSignedAt),
    hasCbqlSign: Boolean(rep.cbqlXacNhanSign || rep.cbqlSignedAt),
    eodStatus: String(rep.eodStatus || 'Chờ QC hoàn tất'),
    isEODConfirmed: false,
    hasRealData: inspectedQty > 0 || defectQtyTotal > 0 || matEntries.length > 0,
  };
}

app.post('/api/accounts/heartbeat', (req, res) => {
  try {
    const fullName = normAuthName(String(req.body?.fullName || ''));
    const userKey = normalizeServerUserKey(fullName || String(req.body?.userKey || ''));
    if (!userKey || isUnsafeObjectKey(userKey)) {
      return res.status(400).json({ ok: false, error: 'Thiếu thông tin tài khoản.' });
    }
    const nowMs = Date.now();
    const nowIso = new Date(nowMs).toISOString();
    const prev = accountPresenceStore[userKey];
    const wasOffline = !prev || prev.explicitlyLoggedOut || nowMs - (prev.lastSeenAtMs || 0) > 70_000;
    const nextTab = String(req.body?.activeTab || prev?.activeTab || 'report').slice(0, 40);
    const tabChanged = prev && prev.activeTab !== nextTab;

    const tkMatchHb = safeTkMkAccountsCache.find((a) => a.userKey === userKey);
    const authRole = tkMatchHb ? tkMatchHb.role : prev?.role || String(req.body?.role || '').slice(0, 80);
    const authChucVu = tkMatchHb ? tkMatchHb.chucVu : prev?.chucVu || String(req.body?.chucVu || '').slice(0, 120);

    accountPresenceStore[userKey] = {
      userKey,
      fullName: tkMatchHb?.fullName || fullName || prev?.fullName || userKey,
      role: authRole,
      chucVu: authChucVu,
      activeTab: nextTab,
      deviceSummary: String(req.body?.deviceSummary || prev?.deviceSummary || '').slice(0, 120),
      lastSeenAt: nowIso,
      lastSeenAtMs: nowMs,
      lastLoginAt: prev?.lastLoginAt || nowIso,
      lastLogoutAt: prev?.lastLogoutAt || '',
      explicitlyLoggedOut: false,
    };
    saveAccountPresenceStore(accountPresenceStore);
    if (wasOffline || tabChanged) {
      broadcastAccountStatusChanged('heartbeat', userKey);
    }
    return res.json({ ok: true });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Lỗi cập nhật trạng thái.' });
  }
});

app.post('/api/accounts/logout', (req, res) => {
  try {
    const fullName = normAuthName(String(req.body?.fullName || ''));
    const userKey = normalizeServerUserKey(fullName || String(req.body?.userKey || ''));
    if (userKey && !isUnsafeObjectKey(userKey)) {
      const nowIso = new Date().toISOString();
      const prev = accountPresenceStore[userKey];
      accountPresenceStore[userKey] = {
        userKey,
        fullName: fullName || prev?.fullName || userKey,
        role: String(req.body?.role || prev?.role || '').slice(0, 80),
        chucVu: String(req.body?.chucVu || prev?.chucVu || '').slice(0, 120),
        activeTab: prev?.activeTab || 'report',
        deviceSummary: String(req.body?.deviceSummary || prev?.deviceSummary || '').slice(0, 120),
        lastSeenAt: nowIso,
        lastSeenAtMs: Date.now(),
        lastLoginAt: prev?.lastLoginAt || '',
        lastLogoutAt: nowIso,
        explicitlyLoggedOut: true,
      };
      saveAccountPresenceStore(accountPresenceStore);
      broadcastAccountStatusChanged('logout', userKey);
    }
    return res.json({ ok: true });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Lỗi ghi nhận đăng xuất.' });
  }
});

app.get('/api/accounts/live-status', async (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  try {
    const queryGasUrl = String(req.query.webAppUrl || '').trim();
    const querySheetUrl = String(req.query.sheetUrl || '').trim();
    const forceRefresh = req.query.refresh === '1';

    const tkMkAccounts = await ensureSafeTkMkAccountsCached(
      forceRefresh,
      queryGasUrl || undefined,
      querySheetUrl || undefined
    );

    // Avoid blocking HTTP response for 10-30s on Sheet hydration (prevents Cloud Run / proxy timeouts)
    if (hasHydratedDataSheetsFromSheets || sharedReportsStore.length > 0) {
      void ensureReportsHydratedFromSheets(forceRefresh);
    } else {
      await Promise.race([
        ensureReportsHydratedFromSheets(false),
        new Promise<void>((resolve) => setTimeout(resolve, 3000)),
      ]);
    }

    // Server-side RBAC check: prefer signed Bearer token, fallback to authoritative TK_MK lookup on X-User-Name
    const authHeader = String(req.headers.authorization || '').trim();
    const bearerToken = authHeader.toLowerCase().startsWith('bearer ') ? authHeader.slice(7).trim() : '';
    const tokenPayload = bearerToken ? verifyServerSessionToken(bearerToken) : null;

    let rawHeaderUserName = String(req.query.requesterName || '').trim();
    if (!rawHeaderUserName && req.headers['x-user-name']) {
      try {
        rawHeaderUserName = decodeURIComponent(String(req.headers['x-user-name']));
      } catch {
        rawHeaderUserName = String(req.headers['x-user-name']);
      }
    }
    const headerUserName = normAuthName(rawHeaderUserName);
    const headerUserKey = normalizeServerUserKey(headerUserName);

    let verifiedRole = '';
    let verifiedChucVu = '';
    if (tokenPayload) {
      verifiedRole = tokenPayload.role;
      verifiedChucVu = tokenPayload.chucVu;
    } else if (headerUserKey) {
      const tkMatch = tkMkAccounts.find((a) => a.userKey === headerUserKey);
      if (tkMatch && tkMatch.accountStatus === 'ACTIVE') {
        verifiedRole = tkMatch.role;
        verifiedChucVu = tkMatch.chucVu;
      } else if (tkMkAccounts.length === 0 && accountPresenceStore[headerUserKey]) {
        // Cold-start fallback when TK_MK sheet is temporarily unreachable
        verifiedRole = accountPresenceStore[headerUserKey].role || '';
        verifiedChucVu = accountPresenceStore[headerUserKey].chucVu || '';
      }
    }

    const callerIsAdmin = isServerAdminRole(verifiedRole);
    const callerIsManagement = isServerManagementMonitorRole(verifiedRole, verifiedChucVu);

    if (!callerIsAdmin && !callerIsManagement) {
      return res.status(403).json({
        ok: false,
        error: 'Chỉ tài khoản Admin hoặc TP / T.Team / PGĐ / QPS / IPL mới có quyền xem trạng thái trực tiếp các tài khoản.',
      });
    }

    const scope: 'all' | 'qc_only' = callerIsAdmin ? 'all' : 'qc_only';
    const todayStr = getVietnamClock().dateStr;
    const nowMs = Date.now();
    const todaySubmitted = sharedReportsStore.filter((r) => r && r.dateStr === todayStr);
    const todayTempEntries = Object.values(tempSlotReportsStore).filter(
      (e) => e && e.dateStr === todayStr && e.report
    );

    // Build unified account map from TK_MK + active presence + drafts + today's reports
    const accountMap = new Map<
      string,
      {
        userKey: string;
        fullName: string;
        chucVu: string;
        role: string;
        roleCategory: 'ADMIN' | 'MANAGEMENT' | 'QA' | 'QC' | 'CBQL' | 'THU_KHO';
        accountStatus: string;
        avatarUrl: string;
      }
    >();

    for (const acc of tkMkAccounts) {
      accountMap.set(acc.userKey, { ...acc });
    }
    for (const [k, pres] of Object.entries(accountPresenceStore)) {
      if (!k || isUnsafeObjectKey(k)) continue;
      if (!accountMap.has(k)) {
        accountMap.set(k, {
          userKey: k,
          fullName: pres.fullName || k,
          chucVu: pres.chucVu || pres.role || 'QC',
          role: pres.role || 'QC_User',
          roleCategory: classifyMonitorRoleCategory(pres.role, pres.chucVu),
          accountStatus: 'ACTIVE',
          avatarUrl: '',
        });
      }
    }
    for (const [k, dr] of Object.entries(userDraftsStore)) {
      if (!k || isUnsafeObjectKey(k) || !dr) continue;
      if (!accountMap.has(k)) {
        accountMap.set(k, {
          userKey: k,
          fullName: String(dr.qcKiemTra || k).trim(),
          chucVu: 'QC',
          role: 'QC_User',
          roleCategory: 'QC',
          accountStatus: 'ACTIVE',
          avatarUrl: '',
        });
      }
    }

    const items: any[] = [];
    for (const acc of accountMap.values()) {
      if (scope === 'qc_only' && acc.roleCategory !== 'QC') {
        continue;
      }

      const pres = accountPresenceStore[acc.userKey];
      const wsActive = hasActiveWsForUser(acc.userKey);
      const recentHeartbeat =
        Boolean(pres) && !pres.explicitlyLoggedOut && nowMs - (pres.lastSeenAtMs || 0) <= 75_000;
      const isOnline = wsActive || recentHeartbeat;

      const userDraft = userDraftsStore[acc.userKey];
      const isTodayDraft = Boolean(userDraft && (!userDraft.dateStr || userDraft.dateStr === todayStr));

      const userSubmittedToday = todaySubmitted.filter((r) => {
        const subKey = normalizeServerUserKey(String(r.submittedBy || ''));
        const qcKey = normalizeServerUserKey(String(r.qcKiemTra || ''));
        return subKey === acc.userKey || qcKey === acc.userKey;
      });

      const userTempEntriesToday = todayTempEntries.filter((e) => {
        const upKey = normalizeServerUserKey(String(e.updatedBy || ''));
        const qcKey = normalizeServerUserKey(String(e.report?.qcKiemTra || ''));
        const subKey = normalizeServerUserKey(String(e.report?.submittedBy || ''));
        return upKey === acc.userKey || qcKey === acc.userKey || subKey === acc.userKey;
      });

      let currentWork = isTodayDraft ? buildLiveWorkSnapshotFromDraft(userDraft) : null;
      if ((!currentWork || !currentWork.hasRealData) && userTempEntriesToday.length > 0) {
        const latestTemp = [...userTempEntriesToday].sort((a, b) =>
          String(b.updatedAt || '').localeCompare(String(a.updatedAt || ''))
        )[0];
        if (latestTemp?.report) {
          currentWork = buildLiveWorkSnapshotFromTempReport(latestTemp.report);
        }
      }

      const hasActiveWorkData = Boolean(currentWork?.hasRealData);
      const presenceState = isOnline
        ? hasActiveWorkData
          ? 'entering'
          : 'online'
        : hasActiveWorkData
        ? 'paused_with_draft'
        : 'offline';

      const combinedTodayReports = [
        ...userSubmittedToday,
        ...userTempEntriesToday
          .map((e) => e.report)
          .filter(
            (tr) =>
              !userSubmittedToday.some(
                (sr) =>
                  sr.congDoan === tr.congDoan &&
                  sr.chuyen === tr.chuyen &&
                  sr.gioKiem === tr.gioKiem
              )
          ),
      ];

      let totalInspected = 0;
      let totalPass = 0;
      let totalFail = 0;
      let rftInspected = 0;
      let rftPass = 0;
      const stationSet = new Set<string>();
      const slotSet = new Set<string>();
      for (const r of combinedTodayReports) {
        const kiem = roundServerQty(r.materialSummary?.tongSoLuongKiem ?? r.tongSoLuongDoikiem ?? 0);
        const fail = roundServerQty(r.materialSummary?.tongSoLuongKiemKhongDat ?? r.tongSoLuongDoiKiemKhongDat ?? 0);
        const pass = roundServerQty(Math.max(0, Number(r.tongSoLuongDoiKiemDat ?? kiem - fail)));
        totalInspected = roundServerQty(totalInspected + kiem);
        totalPass = roundServerQty(totalPass + pass);
        totalFail = roundServerQty(totalFail + fail);
        const isDefRate = isInputOrEodOnlyStageServer(r.congDoan, r.nhaMay) || r.congDoan === 'DeBTP';
        if (!isDefRate && kiem > 0) {
          rftInspected += kiem;
          rftPass += pass;
        }
        if (r.chuyen) stationSet.add(String(r.chuyen));
        if (r.gioKiem) slotSet.add(String(r.gioKiem));
      }
      if (currentWork?.chuyen) stationSet.add(currentWork.chuyen);

      const latestSubmittedAt = userSubmittedToday
        .map((r) => String(r.submittedAt || r.timestamp || ''))
        .sort()
        .reverse()[0] || '';

      items.push({
        userKey: acc.userKey,
        fullName: acc.fullName,
        chucVu: acc.chucVu,
        role: acc.role,
        roleCategory: acc.roleCategory,
        accountStatus: acc.accountStatus,
        avatarUrl: acc.avatarUrl,
        isOnline,
        presenceState,
        activeTab: pres?.activeTab || 'report',
        lastSeenAt: pres?.lastSeenAt || currentWork?.updatedAt || '',
        lastLoginAt: pres?.lastLoginAt || '',
        lastLogoutAt: pres?.lastLogoutAt || '',
        deviceSummary: pres?.deviceSummary || '',
        currentWork,
        todayStats: {
          submittedCount: userSubmittedToday.length,
          tempSlotCount: userTempEntriesToday.length,
          totalInspected,
          totalPass,
          totalFail,
          todayRftPercent:
            rftInspected > 0 ? Number(((rftPass / rftInspected) * 100).toFixed(1)) : null,
          latestSubmittedAt,
          stationsWorked: Array.from(stationSet),
          slotsFilled: Array.from(slotSet),
        },
        todayReports: combinedTodayReports.slice(0, 24),
      });
    }

    const stateOrder: Record<string, number> = {
      entering: 0,
      online: 1,
      paused_with_draft: 2,
      offline: 3,
    };
    items.sort((a, b) => {
      const diff = (stateOrder[a.presenceState] ?? 9) - (stateOrder[b.presenceState] ?? 9);
      if (diff !== 0) return diff;
      const subDiff = (b.todayStats.submittedCount || 0) - (a.todayStats.submittedCount || 0);
      if (subDiff !== 0) return subDiff;
      return String(b.lastSeenAt || '').localeCompare(String(a.lastSeenAt || ''));
    });

    const onlineCount = items.filter((i) => i.isOnline).length;
    const enteringCount = items.filter((i) => i.presenceState === 'entering').length;
    const pausedWithDraftCount = items.filter((i) => i.presenceState === 'paused_with_draft').length;
    const offlineCount = items.filter((i) => i.presenceState === 'offline').length;
    const totalSubmittedToday = items.reduce((s, i) => s + (i.todayStats.submittedCount || 0), 0);
    const totalInspectedToday = roundServerQty(
      items.reduce((s, i) => s + (i.todayStats.totalInspected || 0), 0)
    );
    const rftItems = items.filter((i) => typeof i.todayStats.todayRftPercent === 'number');
    const avgRftToday =
      rftItems.length > 0
        ? Number(
            (
              rftItems.reduce((s, i) => s + Number(i.todayStats.todayRftPercent || 0), 0) /
              rftItems.length
            ).toFixed(1)
          )
        : null;

    return res.json({
      ok: true,
      scope,
      dateStr: todayStr,
      generatedAt: new Date().toISOString(),
      summary: {
        totalAccounts: items.length,
        onlineCount,
        enteringCount,
        pausedWithDraftCount,
        offlineCount,
        totalSubmittedToday,
        totalInspectedToday,
        avgRftToday,
      },
      accounts: items,
    });
  } catch (err: any) {
    console.error('Account live-status error:', err);
    return res.status(500).json({ ok: false, error: err?.message || 'Không tải được trạng thái tài khoản.' });
  }
});

// =============================================================================
// QUALITY DEFECT SOP ALERTS (REAL-TIME WS + SSE + WEB PUSH TO QA / THỦ KHO / CBQL)
// =============================================================================
function loadQualityAlertsStore(): any[] {
  try {
    if (fs.existsSync(QUALITY_ALERTS_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(QUALITY_ALERTS_FILE, 'utf8'));
      if (Array.isArray(parsed)) return parsed;
    }
  } catch {
    // ignore
  }
  return [];
}

function saveQualityAlertsStore(items: any[]) {
  try {
    fs.writeFileSync(QUALITY_ALERTS_FILE, JSON.stringify(items.slice(0, 200)), 'utf8');
  } catch {
    // ignore
  }
}

let qualityAlertsStore: any[] = loadQualityAlertsStore();

function getRecentQualityAlerts(): any[] {
  const todayStr = getVietnamClock().dateStr;
  // Keep today's alerts + any unresolved recent alerts (up to 100)
  return qualityAlertsStore
    .filter((a) => a && (a.dateStr === todayStr || a.status === 'open' || a.status === 'rqc_nok' || a.status === 'qa_nok'))
    .slice(0, 100);
}

app.get('/api/quality-alerts', (_req, res) => {
  return res.json({ ok: true, alerts: getRecentQualityAlerts() });
});

app.post('/api/quality-alerts/dispatch', async (req, res) => {
  try {
    const incoming = Array.isArray(req.body?.alerts) ? req.body.alerts : [];
    const triggeredBy = String(req.body?.triggeredBy || '').trim();
    if (incoming.length === 0) {
      return res.json({ ok: true, alerts: getRecentQualityAlerts(), pushSentCount: 0 });
    }

    const nowIso = new Date().toISOString();
    const upsertedAlerts: any[] = [];

    for (const rawAlert of incoming) {
      if (!rawAlert || !rawAlert.id) continue;
      const alertKey = String(rawAlert.alertKey || rawAlert.id);
      const existingIdx = qualityAlertsStore.findIndex(
        (a) => a.id === rawAlert.id || (a.alertKey && a.alertKey === alertKey)
      );
      const existing = existingIdx >= 0 ? qualityAlertsStore[existingIdx] : null;
      const merged = {
        ...rawAlert,
        id: existing?.id || String(rawAlert.id),
        alertKey,
        createdAt: existing?.createdAt || rawAlert.createdAt || nowIso,
        updatedAt: nowIso,
        status: existing?.status && existing.status !== 'resolved' ? existing.status : rawAlert.status || 'open',
        updatedBy: existing?.updatedBy || triggeredBy || rawAlert.qcName || '',
      };

      if (existingIdx >= 0) {
        qualityAlertsStore[existingIdx] = merged;
      } else {
        qualityAlertsStore.unshift(merged);
      }
      upsertedAlerts.push(merged);
      broadcastSseEvent('quality_defect_alert', { alert: merged });
    }

    qualityAlertsStore = qualityAlertsStore.slice(0, 200);
    saveQualityAlertsStore(qualityAlertsStore);

    // Send Web Push notifications to target roles (QA / THU_KHO / CBQL / ADMIN)
    let totalPushSent = 0;
    for (const alert of upsertedAlerts) {
      const targetRoles = new Set<string>(
        Array.isArray(alert.targetRoles) && alert.targetRoles.length > 0
          ? alert.targetRoles
          : ['QA', 'CBQL', 'ADMIN']
      );
      const targetSubs = pushSubscriptions.filter(
        (sub) => targetRoles.has(sub.normalizedRole) || sub.receiveAllEodAlerts
      );
      if (targetSubs.length > 0) {
        const pushRes = await sendWebPushToSubscriptions(targetSubs, {
          title: `⚠️ ${alert.actionTitle || 'Cảnh báo lỗi chất lượng QC'}`,
          body: `${[alert.factory, alert.chuyen, alert.timeSlot].filter(Boolean).join(' • ')} — ${alert.verdictBadge || alert.actionDetail}`.slice(0, 220),
          tag: `qc-defect-alert-${alert.id}`,
          url: '/',
          requireInteraction: true,
        });
        totalPushSent += pushRes.sentCount;
      }
    }

    return res.json({
      ok: true,
      alerts: getRecentQualityAlerts(),
      dispatchedCount: upsertedAlerts.length,
      pushSentCount: totalPushSent,
    });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Lỗi gửi cảnh báo chất lượng.' });
  }
});

app.post('/api/quality-alerts/:id/status', (req, res) => {
  try {
    const id = String(req.params.id || '').trim();
    const status = String(req.body?.status || 'resolved').trim();
    const updatedBy = String(req.body?.updatedBy || '').trim();
    const resolutionNote = String(req.body?.resolutionNote || '').trim();

    const idx = qualityAlertsStore.findIndex((a) => a.id === id);
    if (idx < 0) {
      return res.status(404).json({ ok: false, error: 'Không tìm thấy cảnh báo.' });
    }

    const updated = {
      ...qualityAlertsStore[idx],
      status,
      updatedBy: updatedBy || qualityAlertsStore[idx].updatedBy,
      resolutionNote: resolutionNote || qualityAlertsStore[idx].resolutionNote,
      updatedAt: new Date().toISOString(),
    };
    qualityAlertsStore[idx] = updated;
    saveQualityAlertsStore(qualityAlertsStore);
    broadcastSseEvent('quality_defect_alert', { alert: updated });

    return res.json({ ok: true, alert: updated, alerts: getRecentQualityAlerts() });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: err?.message || 'Lỗi cập nhật trạng thái cảnh báo.' });
  }
});

app.delete('/api/quality-alerts/:id', (req, res) => {
  const id = String(req.params.id || '').trim();
  qualityAlertsStore = qualityAlertsStore.filter((a) => a.id !== id);
  saveQualityAlertsStore(qualityAlertsStore);
  return res.json({ ok: true, alerts: getRecentQualityAlerts() });
});

// Dev vs Prod Vite Integration
async function startServer() {
  if (process.env.NODE_ENV === 'production') {
    // Ensure unknown /api/* endpoints return JSON 404 instead of SPA index.html
    app.all('/api/*', (_req, res) => {
      res.status(404).json({ ok: false, error: 'API endpoint không tồn tại.' });
    });
    // Serve static files from dist
    app.use(express.static(path.resolve(__dirname, 'dist')));
    app.get('*', (_req, res) => {
      res.sendFile(path.resolve(__dirname, 'dist', 'index.html'));
    });
  } else {
    app.get('/sw.js', (_req, res) => {
      res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      res.setHeader('Service-Worker-Allowed', '/');
      res.send(CLASSIC_PUSH_SW_SCRIPT);
    });
    // Mount Vite middleware in dev
    const { createServer: createViteServer } = await import('vite');
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  }

  const httpServer = http.createServer(app);
  const wss = new WebSocketServer({ noServer: true });

  httpServer.on('upgrade', (request, socket, head) => {
    try {
      const reqUrl = new URL(request.url || '/', 'http://localhost');
      if (reqUrl.pathname !== '/ws') {
        return;
      }
      wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
      });
    } catch {
      socket.destroy();
    }
  });

  wss.on('connection', (ws) => {
    wsClients.add(ws);
    try {
      ws.send(
        JSON.stringify({
          event: 'snapshot',
          data: {
            reports: sharedReportsStore,
            deletedIds: deletedReportIdsStore,
            pendingEodItems: pendingEodList,
            hourlyTasks: getTodayHourlyTasks(),
            qualityAlerts: getRecentQualityAlerts(),
            tempSlotReports: getTodayTempSlotReports(),
          },
        })
      );
    } catch {
      // ignore initial write error
    }

    // Trigger non-blocking sheet hydration so newly connected client gets latest cross-instance updates immediately
    void ensureReportsHydratedFromSheets(false, { broadcastDiffs: true });

    ws.on('message', (rawMsg) => {
      try {
        const parsed = JSON.parse(String(rawMsg || ''));
        if (parsed?.type === 'identify') {
          const fullName = normAuthName(String(parsed.fullName || ''));
          const userKey = normalizeServerUserKey(fullName || String(parsed.userKey || ''));
          if (userKey && !isUnsafeObjectKey(userKey)) {
            wsClientUserMap.set(ws, { userKey, fullName: fullName || userKey });
            const nowMs = Date.now();
            const nowIso = new Date(nowMs).toISOString();
            const prev = accountPresenceStore[userKey];
            accountPresenceStore[userKey] = {
              userKey,
              fullName: fullName || prev?.fullName || userKey,
              role: String(parsed.role || prev?.role || '').slice(0, 80),
              chucVu: String(parsed.chucVu || prev?.chucVu || '').slice(0, 120),
              activeTab: String(parsed.activeTab || prev?.activeTab || 'report').slice(0, 40),
              deviceSummary: String(parsed.deviceSummary || prev?.deviceSummary || '').slice(0, 120),
              lastSeenAt: nowIso,
              lastSeenAtMs: nowMs,
              lastLoginAt: prev?.lastLoginAt || nowIso,
              lastLogoutAt: prev?.lastLogoutAt || '',
              explicitlyLoggedOut: false,
            };
            saveAccountPresenceStore(accountPresenceStore);
            broadcastAccountStatusChanged('ws_identify', userKey);
          }
          return;
        }
        if (parsed?.type === 'sync_request') {
          void ensureReportsHydratedFromSheets(Boolean(parsed?.force), { broadcastDiffs: true }).then(() => {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(
                JSON.stringify({
                  event: 'snapshot',
                  data: {
                    reports: sharedReportsStore,
                    deletedIds: deletedReportIdsStore,
                    pendingEodItems: pendingEodList,
                    hourlyTasks: getTodayHourlyTasks(),
                    qualityAlerts: getRecentQualityAlerts(),
                    tempSlotReports: getTodayTempSlotReports(),
                  },
                })
              );
            }
          });
        }
      } catch {
        // ignore malformed ws message
      }
    });

    const cleanupWsPresence = () => {
      wsClients.delete(ws);
      const meta = wsClientUserMap.get(ws);
      if (meta) {
        wsClientUserMap.delete(ws);
        if (!hasActiveWsForUser(meta.userKey)) {
          broadcastAccountStatusChanged('ws_disconnect', meta.userKey);
        }
      }
    };
    ws.on('close', cleanupWsPresence);
    ws.on('error', cleanupWsPresence);
  });

  const wsHeartbeatTimer = setInterval(() => {
    for (const ws of wsClients) {
      if (ws.readyState !== WebSocket.OPEN) {
        wsClients.delete(ws);
        continue;
      }
      try {
        ws.ping();
      } catch {
        wsClients.delete(ws);
      }
    }
  }, 25_000);
  wsHeartbeatTimer.unref?.();

  httpServer.listen(PORT, '0.0.0.0', () => {
    console.log(`Server listening on port ${PORT} (HTTP + WebSocket /ws)`);
    setTimeout(() => void ensureReportsHydratedFromSheets(false, { broadcastDiffs: true }), 1000);
    setInterval(() => {
      if (wsClients.size > 0 || sseClients.size > 0) {
        void ensureReportsHydratedFromSheets(false, { broadcastDiffs: true });
      }
    }, 8_000);
    setTimeout(() => void runScheduledQcReminderCheck(), 10_000);
    setInterval(() => void runScheduledQcReminderCheck(), 60_000);
    setTimeout(() => void runHourlyCbqlNotificationScheduler(), 8_000);
    setInterval(() => void runHourlyCbqlNotificationScheduler(), 30_000);
    setTimeout(() => void runAfter20hIncompleteAuditScheduler(), 12_000);
    setInterval(() => void runAfter20hIncompleteAuditScheduler(), 60_000);
  });
}

startServer();
