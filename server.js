
// server.js
// PDF compressor service: downloads from Supabase Storage, compresses with Ghostscript,
// uploads back (overwrite optional), and updates pdf_storage directly using Service Role.
// Environment variables required:
//   PDF_COMPRESSOR_SECRET   — shared Bearer token expected by the DB trigger (Authorization header)
//   SUPABASE_URL            — your Supabase project URL (e.g., https://xxxx.supabase.co)
//   SUPABASE_SERVICE_ROLE   — your Supabase Service Role key
//   GS_QUALITY              — optional Ghostscript preset: 'screen' | 'ebook' | 'printer' | 'prepress'

import http from 'node:http';
import { createClient } from '@supabase/supabase-js';
import { spawn } from 'node:child_process';
import { writeFile, readFile, unlink, stat } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import url from 'node:url';

const PORT = process.env.PORT || 8080;
const SECRET = process.env.PDF_COMPRESSOR_SECRET || '';
const SUPABASE_URL = process.env.SUPABASE_URL || '';
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || '';
const GS_QUALITY = process.env.GS_QUALITY || 'ebook';
const TMPDIR = '/tmp';

// Init Supabase client with Service Role (server-side only; bypasses RLS)
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
    inPath,
  ];
  await new Promise((resolve, reject) => {
    const ps = spawn('gs', args);
    let stderr = '';
    ps.stderr?.on('data', (d) => (stderr += d.toString()));
    ps.on('error', reject);
    ps.on('close', (code) => (code === 0 ? resolve() : reject(new Error(`gs exit ${code}: ${stderr}`))));
  });
}

// 🔔 Direct table update using Service Role (no RPC)
async function markStatusByPath(filePath, status) {
  const updates = { status, updated_at: new Date().toISOString() };
  if (status === 'processing') {
    updates.processing_started_at = new Date().toISOString();
  } else if (status === 'done') {
    updates.processing_finished_at = new Date().toISOString();
  } else if (status === 'error') {
    // leave room for explicit error messages set by caller
  }

  const { error } = await supabase
    .from('pdf_storage')
    .update(updates)
    .eq('file_path', filePath);

  if (error) {
    console.error(`DB update pdf_storage.status failed for ${filePath} -> ${status}:`, error.message);
  } else {
    console.log(`pdf_storage.status set to '${status}' for ${filePath}`);
  }
}

// Optional: record compression stats in the same row
async function writeCompressionStats(filePath, originalBytes, compressedBytes) {
  const ratio = Number((compressedBytes / originalBytes).toFixed(3));
  const { error } = await supabase
    .from('pdf_storage')
    .update({
      compressed_size_bytes: compressedBytes,
      compression_ratio: ratio,
      overwrote: true,
      updated_at: new Date().toISOString(),
    })
    .eq('file_path', filePath);

  if (error) {
    console.error(`DB update compression stats failed for ${filePath}:`, error.message);
  } else {
    console.log(`Compression stats written for ${filePath}: ratio=${ratio}`);
  }
}

// Main compression handler
async function handleCompress(req, res, body) {
  let filePathForStatus = null;

  try {
    // Authorization (Bearer <PDF_COMPRESSOR_SECRET>)
    const auth = req.headers['authorization'] || '';
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
    if (!SECRET || token !== SECRET) return json(res, 401, { ok: false, error: 'unauthorized' });

    // Input: bucket + name (name must equal pdf_storage.file_path)
    const { bucket, name, overwrite = true } = body || {};
    if (!bucket || !name) return json(res, 400, { ok: false, error: 'missing bucket or name' });

    filePathForStatus = name; // e.g., "bf26781e-.../Erin T.pdf" (matches your table)

    // Mark processing immediately (nice for observability)
    await markStatusByPath(filePathForStatus, 'processing');

    // Download original from Storage
    const dl = await supabase.storage.from(bucket).download(name);
    if (dl.error) {
      await supabase
        .from('pdf_storage')
        .update({
          status: 'error',
          processing_error: `download_failed: ${dl.error.message}`,
          updated_at: new Date().toISOString(),
        })
        .eq('file_path', filePathForStatus);
      return json(res, 500, { ok: false, error: 'download_failed', details: dl.error.message });
    }

    // Temp paths
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
      upsert: overwrite,
      contentType: 'application/pdf',
      cacheControl: '3600',
    });
    if (up.error) {
      await supabase
        .from('pdf_storage')
        .update({
          status: 'error',
          processing_error: `upload_failed: ${up.error.message}`,
          updated_at: new Date().toISOString(),
        })
        .eq('file_path', filePathForStatus);
      return json(res, 500, { ok: false, error: 'upload_failed', details: up.error.message });
    }

    // Gather stats + cleanup
    const origBytes = (await stat(inPath)).size;
    const compBytes = (await stat(outPath)).size;
    await unlink(inPath).catch(() => {});
    await unlink(outPath).catch(() => {});

    // Write stats
    await writeCompressionStats(filePathForStatus, origBytes, compBytes);

    // ✅ Flip to 'done' so OCR trigger can run
    await markStatusByPath(filePathForStatus, 'done');

    // Respond
    return json(res, 200, {
      ok: true,
      overwrote: true,
      original_bytes: origBytes,
      compressed_bytes: compBytes,
      ratio: Number((compBytes / origBytes).toFixed(3)),
      quality: GS_QUALITY,
    });
  } catch (e) {
    // Best effort: mark error
    const msg = String(e?.message || e);
    if (filePathForStatus) {
      await supabase
        .from('pdf_storage')
        .update({
          status: 'error',
          processing_error: `internal_error: ${msg}`,
          updated_at: new Date().toISOString(),
        })
        .eq('file_path', filePathForStatus);
    }
    return json(res, 500, { ok: false, error: 'internal', details: msg });
  }
}

// HTTP server
const server = http.createServer(async (req, res) => {
  const { pathname } = new url.URL(req.url, `http://${req.headers.host}`);
  if (req.method === 'GET' && pathname === '/healthz') return json(res, 200, { ok: true });

  if (req.method === 'POST' && pathname === '/compress') {
    let data = '';
    req.on('data', (chunk) => {
      data += chunk.toString();
      if (data.length > 1_000_000) req.destroy(); // basic protection
    });
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
