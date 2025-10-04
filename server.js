
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

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false },
});

function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
}

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

async function handleCompress(req, res, body) {
  try {
    const auth = req.headers['authorization'] || '';
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
    if (!SECRET || token !== SECRET) return json(res, 401, { ok: false, error: 'unauthorized' });

    const { bucket, name, overwrite = true } = body || {};
    if (!bucket || !name) return json(res, 400, { ok: false, error: 'missing bucket or name' });

    const dl = await supabase.storage.from(bucket).download(name);
    if (dl.error) return json(res, 500, { ok: false, error: 'download_failed', details: dl.error.message });

    const inBuf = Buffer.from(await dl.data.arrayBuffer());
    const id = randomUUID();
    const inPath = path.join(TMPDIR, `${id}-in.pdf`);
    const outPath = path.join(TMPDIR, `${id}-out.pdf`);
    await writeFile(inPath, inBuf);

    await runGhostscript(inPath, outPath, GS_QUALITY);

    const outBuf = await readFile(outPath);
    const up = await supabase.storage.from(bucket).upload(name, outBuf, {
      upsert: overwrite, contentType: 'application/pdf', cacheControl: '3600'
    });
    if (up.error) return json(res, 500, { ok: false, error: 'upload_failed', details: up.error.message });

    const origBytes = (await stat(inPath)).size;
    const compBytes = (await stat(outPath)).size;
    await unlink(inPath).catch(()=>{});
    await unlink(outPath).catch(()=>{});

    return json(res, 200, {
      ok: true,
      overwrote: true,
      original_bytes: origBytes,
      compressed_bytes: compBytes,
      ratio: Number((compBytes / origBytes).toFixed(3)),
      quality: GS_QUALITY
    });
  } catch (e) {
    return json(res, 500, { ok: false, error: 'internal', details: String(e?.message || e) });
  }
}

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
