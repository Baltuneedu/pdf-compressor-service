
// server.js
import http from 'node:http'
import { createClient } from '@supabase/supabase-js'
import { spawn } from 'child_process'
import { writeFile, readFile, unlink, stat, rename } from 'node:fs/promises'
import path from 'node:path'
import crypto from 'node:crypto'
import url from 'node:url'
import fetch from 'node-fetch'

// ---- ENVIRONMENT VARIABLES ----
const PORT = process.env.PORT || 8080
const SECRET = process.env.PDF_COMPRESSOR_SECRET || ''
const SUPABASE_URL = process.env.SUPABASE_URL || ''
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || ''
const GS_QUALITY = (process.env.GS_QUALITY || 'screen').trim().toLowerCase()
const TARGET_MAX_BYTES = Number(process.env.TARGET_MAX_BYTES || 900000)
const MAX_INPUT_BYTES = Number(process.env.MAX_INPUT_BYTES || 4000000)
const TMPDIR = '/tmp'
const OCR_FUNCTION_URL = process.env.OCR_FUNCTION_URL || ''

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false },
})

// ---- Helpers ----
function randomUUID() {
  return crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString('hex')
}

function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json' })
  res.end(JSON.stringify(obj))
}

function buildGsArgs(level, inPath, outPath) {
  const base = [
    '-sDEVICE=pdfwrite',
    '-dCompatibilityLevel=1.4',
    `-dPDFSETTINGS=/${GS_QUALITY}`,
    '-dNOPAUSE',
    '-dQUIET',
    '-dBATCH',
    '-dDetectDuplicateImages=true',
    '-dCompressFonts=true',
    '-dSubsetFonts=true',
    `-sOutputFile=${outPath}`,
    inPath,
  ]

  const downsample = (colorDpi, grayDpi, monoDpi, jpegQuality = 60) => [
    '-dDownsampleColorImages=true',
    '-dColorImageDownsampleType=/Bicubic',
    `-dColorImageResolution=${colorDpi}`,
    '-dAutoFilterColorImages=false',
    '-dEncodeColorImages=true',
    '-dColorImageFilter=/DCTEncode',
    `-dJPEGQ=${jpegQuality}`,
    '-dDownsampleGrayImages=true',
    '-dGrayImageDownsampleType=/Bicubic',
    `-dGrayImageResolution=${grayDpi}`,
    '-dAutoFilterGrayImages=false',
    '-dEncodeGrayImages=true',
    '-dGrayImageFilter=/DCTEncode',
    '-dDownsampleMonoImages=true',
    '-dMonoImageDownsampleType=/Bicubic',
    `-dMonoImageResolution=${monoDpi}`,
  ]

  switch (level) {
    case 'baseline': return base
    case 'aggr72': return [...base, ...downsample(72, 72, 200, 55)]
    case 'aggr50': return [...base, ...downsample(50, 50, 120, 45)]
    case 'ultra36': return [...base, ...downsample(36, 36, 100, 35)]
    default: return base
  }
}

async function runGhostscriptWithLevel(level, inPath, outPath) {
  const args = buildGsArgs(level, inPath, outPath)
  console.log(`[compress] Running Ghostscript level: ${level}`)
  return new Promise((resolve, reject) => {
    const ps = spawn('gs', args)
    let stderr = ''
    ps.stderr?.on('data', (d) => (stderr += d.toString()))
    ps.on('error', reject)
    ps.on('close', (code) =>
      code === 0 ? resolve() : reject(new Error(`gs(${level}) exit ${code}: ${stderr}`))
    )
  })
}

async function fileBytes(filePath) {
  return (await stat(filePath)).size
}

async function updatePdfStorageRow(filePath, metrics = {}) {
  const { compressedBytes, ratio, overwrote, hitTarget, passUsed } = metrics
  console.log(`[db] Updating pdf_storage for ${filePath}...`)

  const { data, error } = await supabase
    .from('pdf_storage')
    .update({
      status: 'done',
      compressed_size_bytes: compressedBytes,
      compression_ratio: ratio,
      processing_finished_at: new Date().toISOString(),
      overwrote,
      hit_target: hitTarget,
      pass_used: passUsed,
    })
    .eq('file_path', filePath)
    .select('id, file_path, status')
    .maybeSingle()

  if (error) {
    console.error('[db] Update failed:', error.message)
    return
  }

  console.log(`[db] Updated status = done for ${filePath}`)

  // Notify OCR function
  if (OCR_FUNCTION_URL && data?.status === 'done') {
    try {
      console.log(`[notify_ocr] Sending POST to OCR for ${data.file_path}`)
      await fetch(OCR_FUNCTION_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${SUPABASE_SERVICE_ROLE}`,
        },
        body: JSON.stringify({ id: data.id, file_path: data.file_path }),
      })
      console.log(`[notify_ocr] ✅ OCR notified for ${data.file_path}`)
    } catch (err) {
      console.error('[notify_ocr] ❌ Error notifying OCR:', err.message)
    }
  }
}

async function compressAdaptive(inPath, workDir) {
  const levels = ['baseline', 'aggr72', 'aggr50', 'ultra36']
  let bestPath = inPath
  let bestBytes = await fileBytes(inPath)
  let bestLevel = 'original'
  let passUsed = 0

  for (let i = 0; i < levels.length; i++) {
    passUsed = i + 1
    const level = levels[i]
    const outPath = path.join(workDir, `${randomUUID()}-${level}.pdf`)
    try {
      await runGhostscriptWithLevel(level, bestPath, outPath)
      const outBytes = await fileBytes(outPath)
      console.log(`[compress] ${level}: ${outBytes} bytes`)

      if (outBytes < bestBytes) {
        bestBytes = outBytes
        bestLevel = level
        const nextIn = path.join(workDir, `${randomUUID()}-best.pdf`)
        await rename(outPath, nextIn)
        bestPath = nextIn
      } else {
        await unlink(outPath).catch(() => {})
      }

      if (bestBytes <= TARGET_MAX_BYTES) break
    } catch (e) {
      console.warn(`[compress] ${level} failed:`, e.message)
    }
  }

  console.log(`[compress] Best level: ${bestLevel}, size: ${bestBytes}`)
  return { bestPath, bestBytes, bestLevel, passUsed }
}

// ---- Main Handler ----
async function handleCompress(req, res, body) {
  try {
    const auth = req.headers['authorization'] || ''
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : ''
    if (!SECRET || token !== SECRET) {
      console.warn('[auth] Unauthorized request')
      return json(res, 401, { ok: false, error: 'unauthorized' })
    }

    const bucket = body?.bucket
    const objectName = body?.name ?? body?.path
    const overwrite = body?.overwrite !== false
    console.log(`[input] Bucket: ${bucket}, File: ${objectName}`)

    if (!bucket || !objectName) {
      return json(res, 400, { ok: false, error: 'missing bucket or name' })
    }

    const { data: storageRows } = await supabase
      .from('pdf_storage')
      .select('status')
      .eq('file_path', objectName)
      .limit(1)
      .maybeSingle()

    if (!storageRows || storageRows.status === 'done') {
      console.log(`[compress] Skipping ${objectName} (already done)`)
      return json(res, 200, { ok: true, skipped: true })
    }

    console.log(`[download] Fetching ${objectName} from bucket ${bucket}`)
    const dl = await supabase.storage.from(bucket).download(objectName)
    if (dl.error) {
      console.error('[download] Error:', dl.error.message)
      return json(res, 500, { ok: false, error: 'download_failed' })
    }

    const inBuf = Buffer.from(await dl.data.arrayBuffer())
    console.log(`[download] File size: ${inBuf.length} bytes`)

    if (inBuf.length < 524288) {
      console.log(`[compress] Skipping ${objectName}: under 0.5MB`)
      await updatePdfStorageRow(objectName, {
        compressedBytes: inBuf.length,
        ratio: 1.0,
        overwrote: false,
        hitTarget: null,
        passUsed: 0,
      })
      return json(res, 200, { ok: true, skipped: true })
    }

    const inPath = path.join(TMPDIR, `${randomUUID()}-in.pdf`)
    await writeFile(inPath, inBuf)
    console.log(`[compress] Starting compression for ${objectName}`)

    const originalBytes = inBuf.length
    const { bestPath, bestBytes, bestLevel, passUsed } = await compressAdaptive(inPath, TMPDIR)

    if (bestBytes >= originalBytes) {
      console.log(`[compress] No improvement for ${objectName}`)
      return json(res, 200, { ok: true, overwrote: false })
    }

    const outBuf = await readFile(bestPath)
    console.log(`[upload] Uploading compressed file ${objectName}`)
    const up = await supabase.storage.from(bucket).upload(objectName, outBuf, {
      upsert: overwrite,
      contentType: 'application/pdf',
      cacheControl: '3600',
    })

    await unlink(inPath).catch(() => {})
    await unlink(bestPath).catch(() => {})

    if (up.error) {
      console.error('[upload] Error:', up.error.message)
      return json(res, 500, { ok: false, error: 'upload_failed' })
    }

    const ratio = Number((bestBytes / originalBytes).toFixed(3))
    const hitTarget = bestBytes <= TARGET_MAX_BYTES

    console.log(`[compress] Done: ${objectName} → ${bestBytes} bytes (${ratio * 100}% of original)`)

    await updatePdfStorageRow(objectName, {
      compressedBytes: bestBytes,
      ratio,
      overwrote: true,
      hitTarget,
      passUsed,
    })

    return json(res, 200, {
      ok: true,
      overwrote: true,
      original_bytes: originalBytes,
      compressed_bytes: bestBytes,
      ratio,
      quality: `${GS_QUALITY} + ${bestLevel}`,
      pass_used: passUsed,
      hit_target: hitTarget,
      target_bytes: TARGET_MAX_BYTES,
    })
  } catch (e) {
    console.error('[compress] internal error:', e)
    return json(res, 500, { ok: false, error: 'internal', details: String(e?.message || e) })
  }
}

// ---- HTTP Server ----
const server = http.createServer(async (req, res) => {
  const { pathname } = new url.URL(req.url, `http://${req.headers.host}`)

  if (req.method === 'GET' && pathname === '/healthz') {
    return json(res, 200, { ok: true })
  }

  if (req.method === 'POST' && pathname === '/compress') {
    const body = await new Promise((resolve) => {
      let data = ''
      req.on('data', (chunk) => (data += chunk.toString()))
      req.on('end', () => {
        try {
          resolve(JSON.parse(data) || {})
        } catch {
          resolve({})
        }
      })
    })
    return handleCompress(req, res, body)
  }

  return json(res, 404, { ok: false, error: 'not_found' })
})

server.listen(PORT, () => console.log(`pdf-compressor-service listening on :${PORT}`))
