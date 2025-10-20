
// server.js
import http from 'node:http'
import { createClient } from '@supabase/supabase-js'
import { spawn } from 'node:child_process'
import { writeFile, readFile, unlink, stat } from 'node:fs/promises'
import { randomUUID } from 'node:crypto'
import path from 'node:path'
import url from 'node:url'

/**
 * ENV
 *  - PORT (Render provides this)
 *  - PDF_COMPRESSOR_SECRET (matches the Bearer token we send from Postgres via pg_net)
 *  - SUPABASE_URL
 *  - SUPABASE_SERVICE_ROLE (service role key)
 *  - GS_QUALITY (optional, default: ebook)
 */
const PORT = process.env.PORT || 8080
const SECRET = process.env.PDF_COMPRESSOR_SECRET || ''
const SUPABASE_URL = process.env.SUPABASE_URL || ''
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || ''
const GS_QUALITY = process.env.GS_QUALITY || 'ebook'
const TMPDIR = '/tmp'

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  console.warn('[startup] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE env vars')
}
if (!SECRET) {
  console.warn('[startup] Missing PDF_COMPRESSOR_SECRET (requests will be 401)')
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false },
})

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json' })
  res.end(JSON.stringify(obj))
}

async function runGhostscript(inPath, outPath, quality) {
  const args = [
    '-sDEVICE=pdfwrite',
    '-dCompatibilityLevel=1.4',
    `-dPDFSETTINGS=/${quality}`,
    '-dNOPAUSE', '-dQUIET', '-dBATCH',
    `-sOutputFile=${outPath}`,
    inPath,
  ]
  await new Promise((resolve, reject) => {
    const ps = spawn('gs', args)
    let stderr = ''
    ps.stderr?.on('data', d => (stderr += d.toString()))
    ps.on('error', reject)
    ps.on('close', code => (code === 0 ? resolve() : reject(new Error(`gs exit ${code}: ${stderr}`))))
  })
}

async function updatePdfStorageRow(filePath, metrics = {}) {
  // Updates the row that matches this storage object path
  // Fields available in your table: status, compressed_size_bytes, compression_ratio,
  // processing_finished_at, overwrote (boolean)
  const { compressedBytes, ratio, overwrote } = metrics
  const { error } = await supabase
    .from('pdf_storage')
    .update({
      status: 'done',
      compressed_size_bytes: typeof compressedBytes === 'number' ? compressedBytes : null,
      compression_ratio: typeof ratio === 'number' ? ratio : null,
      processing_finished_at: new Date().toISOString(),
      overwrote: typeof overwrote === 'boolean' ? overwrote : true,
    })
    .eq('file_path', filePath)

  if (error) {
    console.error('[updatePdfStorageRow] DB update failed:', error.message)
  }
}

async function readJsonBody(req, res) {
  return new Promise((resolve) => {
    let data = ''
    req.on('data', chunk => {
      data += chunk.toString()
      if (data.length > 2_000_000) {
        // ~2MB safety limit
        console.warn('[body] too large, destroying socket')
        req.destroy()
      }
    })
    req.on('end', () => {
      try {
        resolve(data ? JSON.parse(data) : null)
      } catch {
        resolve(null)
      }
    })
  })
}

// ---------------------------------------------------------------------------
// main handler
// ---------------------------------------------------------------------------
async function handleCompress(req, res, body) {
  try {
    // -- Auth: Bearer
    const auth = req.headers['authorization'] || ''
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : ''
    if (!SECRET || token !== SECRET) {
      return json(res, 401, { ok: false, error: 'unauthorized' })
    }

    // -- Accept both {bucket, name} and {bucket, path}
    const bucket = body?.bucket
    const objectName = body?.name ?? body?.path
    const overwrite = body?.overwrite !== false // default true

    if (!bucket || !objectName) {
      return json(res, 400, { ok: false, error: 'missing bucket or name' })
    }

    console.log('INCOMING /compress', { bucket, objectName, overwrite })

    // Download from Storage
    const dl = await supabase.storage.from(bucket).download(objectName)
    if (dl.error) {
      console.error('[download] error:', dl.error.message)
      return json(res, 500, { ok: false, error: 'download_failed', details: dl.error.message })
    }
    const inBuf = Buffer.from(await dl.data.arrayBuffer())

    // Temp files
    const id = randomUUID()
    const inPath = path.join(TMPDIR, `${id}-in.pdf`)
    const outPath = path.join(TMPDIR, `${id}-out.pdf`)
    await writeFile(inPath, inBuf)

    // Compress
    await runGhostscript(inPath, outPath, GS_QUALITY)

    // Upload back (overwrite original)
    const outBuf = await readFile(outPath)
    const up = await supabase.storage
      .from(bucket)
      .upload(objectName, outBuf, { upsert: overwrite, contentType: 'application/pdf', cacheControl: '3600' })

    if (up.error) {
      console.error('[upload] error:', up.error.message)
      return json(res, 500, { ok: false, error: 'upload_failed', details: up.error.message })
    }

    // Metrics
    const origBytes = (await stat(inPath)).size
    const compBytes = (await stat(outPath)).size
    const ratio = Number((compBytes / origBytes).toFixed(3))

    // Cleanup
    await unlink(inPath).catch(() => {})
    await unlink(outPath).catch(() => {})

    // Update your metadata row (by file_path = objectName)
    await updatePdfStorageRow(objectName, {
      compressedBytes: compBytes,
      ratio,
      overwrote: true,
    })

    return json(res, 200, {
      ok: true,
      overwrote: true,
      original_bytes: origBytes,
      compressed_bytes: compBytes,
      ratio,
      quality: GS_QUALITY,
    })
  } catch (e) {
    console.error('[compress] internal error:', e)
    return json(res, 500, { ok: false, error: 'internal', details: String(e?.message || e) })
  }
}

// ---------------------------------------------------------------------------
// http server
// ---------------------------------------------------------------------------
const server = http.createServer(async (req, res) => {
  const { pathname } = new url.URL(req.url, `http://${req.headers.host}`)

  if (req.method === 'GET' && pathname === '/healthz') {
    return json(res, 200, { ok: true })
  }

  if (req.method === 'POST' && pathname === '/compress') {
    const body = await readJsonBody(req, res)
    return handleCompress(req, res, body)
  }

  return json(res, 404, { ok: false, error: 'not_found' })
})

server.listen(PORT, () => console.log(`pdf-compressor-service listening on :${PORT}`))
