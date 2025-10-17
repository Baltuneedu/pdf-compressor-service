
// server.js
// PDF compressor service: downloads from Supabase Storage, compresses with Ghostscript,
// uploads back (overwrite optional), and updates pdf_storage.status via secure RPC.
// Requires environment variables below to be set on the server (e.g., Vercel).

import http from 'node:http';
import { createClient } from '@supabase/supabase-js';
import { spawn } from 'node:child_process';
import { writeFile, readFile, unlink, stat } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import url from 'node:url';

const PORT = process.env.PORT || 8080;

// 🔐 Bearer token required by callers (e.g., your DB trigger or app)
const SECRET = process.env.PDF_COMPRESSOR_SECRET || '';

// Supabase project + service role (server-side only)
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || ''; // Service Role key

// Ghostscript quality preset: 'screen' | 'ebook' | 'printer' | 'prepress'
const GS_QUALITY = process.env.GS_QUALITY || 'ebook';

// Temp working directory
const TMPDIR = '/tmp';

// Init Supabase client with Service Role (bypasses RLS on server)
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false },
});

// Small JSON response helper
function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
}

// Run Ghostscript to compress PDF
async function runGhostscript(inPath, outPath, quality) {
  const args = [
    '-sDEVICE=pdfwrite',
    '-dCompatibilityLevel=1.4',
    `-dPDFSETTINGS=/${quality}`,
    '-dNOPAUSE', '-dQUIET', '-dBATCH',
    `-sOutputFile=${outPath}`,
    inPath
  ];
  await new Promise((resolve, reject) => {
    const ps = spawn('gs', args);
    let stderr = '';
    ps.stderr?.on('data', (d) => (stderr += d.toString()));
    ps.on('error', reject);
    ps.on('close', (code) => (code === 0 ? resolve() : reject(new Error(`gs exit ${code}: ${stderr}`))));
  });
}

// 🔔 Secure RPC to flip pdf_storage.status by file_path (matches your Step 7 function)
async function markPdfStatusByPath(filePath, status /* 'done'|'processing'|'error' */) {
  const { error } = await supabase.rpc('update_pdf_status_by_path', {
    p_file_path: filePath,
    p_status: status
  });
  if (error) {
    console.error(`RPC update_pdf_status_by_path failed for ${filePath} -> ${status}:`, error.message);
  } else {
    console.log(`pdf_storage.status set to '${status}' for ${filePath}`);
  }
}

// Main compression handler
async function handleCompress(req, res, body) {
  let filePathForStatus = null;

  try {
    // Authorization
    const auth = req.headers['authorization'] || '';
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
    if (!SECRET || token !== SECRET) return json(res, 401, { ok: false, error: 'unauthorized' });

    // Input: bucket + name (name must equal pdf_storage.file_path)
    const { bucket, name, overwrite = true } = body || {};
    if (!bucket || !name) return json(res, 400, { ok: false, error: 'missing bucket or name' });

    filePathForStatus = name; // e.g., "bf26781e-.../Lianke.pdf" (matches your table)

    // Mark processing immediately (nice for observability)
    await markPdfStatusByPath(filePathForStatus, 'processing');

    // Download original from Storage
    const dl = await supabase.storage.from(bucket).download(name);
    if (dl.error) {
      await markPdfStatusByPath(filePathForStatus, 'error');
      return json(res, 500, { ok: false, error: 'download_failed', details: dl.error.message });
    }

    // Paths in temp dir
    const inBuf = Buffer.from(await dl.data.arrayBuffer());
    const id = randomUUID();
    const inPath = path.join(TMPDIR, `${id}-in.pdf`);
    const outPath = path.join(TMPDIR, `${id}-out.pdf`);
    await writeFile(inPath, inBuf);

    // Compress
    await runGhostscript(inPath, outPath, GS_QUALITY);

    // Upload compressed file (overwrite by default)
    const outBuf = await readFile(outPath);
    const up = await supabase.storage.from(bucket).upload(name, outBuf, {
      upsert: overwrite, contentType: 'application/pdf', cacheControl: '3600'
    });
    if (up.error) {
      await markPdfStatusByPath(filePathForStatus, 'error');
      return json(res, 500, { ok: false, error: 'upload_failed', details: up.error.message });
    }

    // Gather stats + cleanup
    const origBytes = (await stat(inPath)).size;
    const compBytes = (await stat(outPath)).size;
    await unlink(inPath).catch(() => {});
    await unlink(outPath).catch(() => {});

    // ✅ Flip to 'done' so OCR trigger runs
    await markPdfStatusByPath(filePathForStatus, 'done');

    // Respond
    return json(res, 200, {
      ok: true,
      overwrote: true,
      original_bytes: origBytes,
      compressed_bytes: compBytes,
      ratio: Number((compBytes / origBytes).toFixed(3)),
      quality: GS_QUALITY
    });

  } catch (e) {
    // Best effort error status
    if (filePathForStatus) {
      await markPdfStatusByPath(filePathForStatus, 'error');
    }
    return json(res, 500, { ok: false, error: 'internal', details: String(e?.message || e) });
  }
}

// HTTP server
const server = http.createServer(async (req, res) => {
  const { pathname } = new url.URL(req.url, `http://${req.headers.host}`);
  if (req.method === 'GET' && pathname === '/healthz') return json(res, 200, { ok: true });

  if (req.method === 'POST' && pathname === '/compress') {
    let data = '';
    req.on('data', (chunk) => { data += chunk.toString(); if (data.length > 1_000_000) req.destroy(); });
    req.on('end', async () => {
      let body = null;
      try { body = data ? JSON.parse(data) : null; } catch {}
      await handleCompress(req, res, body);
    });
    return;
  }

  json(res, 404, { ok: false, error: 'not_found' });
});

server.listen(PORT, () => console.log(`pdf-compressor-service listening on :${PORT}`));
