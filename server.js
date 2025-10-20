
// server.js
import http from 'node:http'
import { createClient } from '@supabase/supabase-js'
import { spawn } from 'node:child_process'
import { writeFile, readFile, unlink, stat, rename } from 'node:fs/promises'
import { randomUUID } from 'node:crypto'
import path from 'node:path'
import url from 'node:url'

/**
 * ENV
 *  - PORT (Render provides this)
 *  - PDF_COMPRESSOR_SECRET   (Bearer token expected from DB trigger)
 *  - SUPABASE_URL
 *  - SUPABASE_SERVICE_ROLE   (service role key)
 *  - GS_QUALITY              (baseline: screen|ebook|printer|prepress ; default: 'screen' for stronger compression)
 *  - TARGET_MAX_BYTES        (target output size; default: 900000 i.e., 0.9 MB)
 *  - MAX_INPUT_BYTES         (advisory; default: 4000000 i.e., 4 MB)
 */
const PORT = process.env.PORT || 8080
const SECRET = process.env.PDF_COMPRESSOR_SECRET || ''
const SUPABASE_URL = process.env.SUPABASE_URL || ''
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || ''
const GS_QUALITY = (process.env.GS_QUALITY || 'screen').trim().toLowerCase()
const TARGET_MAX_BYTES = Number(process.env.TARGET_MAX_BYTES || 900000) // 0.9 MB
const MAX_INPUT_BYTES = Number(process.env.MAX_INPUT_BYTES || 4000000)  // 4 MB
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

/**
 * Build a gs args array for a given 'level'
 * Levels:
 *  - 'baseline'  : /<GS_QUALITY> only (defaults to /screen)
 *  - 'aggr72'    : /screen + explicit 72 dpi downsampling (color/gray/mono)
 *  - 'aggr50'    : more aggressive 50 dpi (color/gray), mono 120 dpi
 *  - 'ultra36'   : very aggressive 36 dpi (color/gray), mono 100 dpi
 */
function buildGsArgs(level, inPath, outPath) {
  const base = [
    '-sDEVICE=pdfwrite',
    '-dCompatibilityLevel=1.4',
    `-dPDFSETTINGS=/${GS_QUALITY || 'screen'}`,
    '-dNOPAUSE', '-dQUIET', '-dBATCH',
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
    // JPEG quality for color images (Ghostscript respects QFactor via image dicts,
    // but this flag influences default compression; 60 is a good aggressive default)
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
    // CCITT Group4 is generally best for monochrome – pdfwrite chooses efficiently
  ]

  switch (level) {
    case 'baseline':
      return base
    case 'aggr72':
      return [
        ...base,
        ...downsample(72, 72, 200, 55),
      ]
    case 'aggr50':
      return [
        ...base,
        ...downsample(50, 50, 120, 45),
      ]
    case 'ultra36':
      return [
        ...base,
        ...downsample(36, 36, 100, 35),
      ]
    default:
      return base
  }
}

async function runGhostscriptWithLevel(level, inPath, outPath) {
  const args = buildGsArgs(level, inPath, outPath)
  await new Promise((resolve, reject) => {
    const ps = spawn('gs', args)
    let stderr = ''
    ps.stderr?.on('data', d => (stderr += d.toString()))
    ps.on('error', reject)
    ps.on('close', code => (code === 0 ? resolve() : reject(new Error(`gs(${level}) exit ${code}: ${stderr}`))))
  })
}

async function fileBytes(filePath) {
  return (await stat(filePath)).size
}

async function updatePdfStorageRow(filePath, metrics = {}) {
  const {
    compressedBytes,
    ratio,
    overwrote,
    hitTarget,
    passUsed
  } = metrics

  const { error } = await supabase
    .from('pdf_storage')
    .update({
      status: 'done',
      compressed_size_bytes: typeof compressedBytes === 'number' ? compressedBytes : null,
      compression_ratio: typeof ratio === 'number' ? ratio : null,
      processing_finished_at: new Date().toISOString(),
      overwrote: typeof overwrote === 'boolean' ? overwrote : true,
      hit_target: typeof hitTarget === 'boolean' ? hitTarget : null,
      pass_used: Number.isInteger(passUsed) ? passUsed : null,
    })
    .eq('file_path', filePath)

  if (error) {
    console.error('[updatePdfStorageRow] DB update failed:', error.message)
  }
}

async function readJsonBody(req) {
  return new Promise((resolve) => {
    let data = ''
    req.on('data', chunk => {
      data += chunk.toString()
      if (data.length > 3_000_000) {
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

/**
 * Adaptive compression:
 * 1) baseline (GS_QUALITY, defaults to /screen)
 * 2) aggr72 (72 dpi)
 * 3) aggr50 (50 dpi)
 * 4) ultra36 (36 dpi)
 * Stops early as soon as TARGET_MAX_BYTES is reached and keeps the best result.
 */
async function compressAdaptive(inPath, workDir) {
  const levels = ['baseline', 'aggr72', 'aggr50', 'ultra36']
  const results = []

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
      results.push({ level, outPath, outBytes })

      // Keep the smallest so far
      if (outBytes < bestBytes) {
        // Replace best
        bestBytes = outBytes
        bestLevel = level

        // To feed the next pass, use the latest output as the input
        // Rename so we always have a stable "bestPath"
        const nextIn = path.join(workDir, `${randomUUID()}-best.pdf`)
        await rename(outPath, nextIn)
        bestPath = nextIn
      } else {
        // Not better; remove output
        await unlink(outPath).catch(() => {})
      }

      // Stop early if we hit target
      if (bestBytes <= TARGET_MAX_BYTES) {
        break
      }
    } catch (e) {
      console.warn(`[compress] ${level} failed:`, e.message)
      // continue to next level
    }
  }

  return { bestPath, bestBytes, bestLevel, passUsed }
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

    // Optional advisory: log if input exceeds MAX_INPUT_BYTES
    if (inBuf.length > MAX_INPUT_BYTES) {
      console.log(`[advisory] input ${inBuf.length} > MAX_INPUT_BYTES ${MAX_INPUT_BYTES}`)
    }

    // Temp files (working dir per request)
    const workDir = path.join(TMPDIR, `work-${randomUUID()}`)
    // lightweight: we can just write into TMPDIR with unique names (no mkdir needed)
    const inPath = path.join(TMPDIR, `${randomUUID()}-in.pdf`)
    await writeFile(inPath, inBuf)

    // Adaptive compression
    const originalBytes = inBuf.length
    const { bestPath, bestBytes, bestLevel, passUsed } = await compressAdaptive(inPath, TMPDIR)

    // If we didn’t improve, don’t overwrite
    const shouldUpload = bestBytes < originalBytes
    if (!shouldUpload) {
      await unlink(inPath).catch(() => {})
      return json(res, 200, {
        ok: true,
        overwrote: false,
        original_bytes: originalBytes,
        compressed_bytes: originalBytes,
        ratio: 1.0,
        quality: `${GS_QUALITY} (no improvement)`,
        pass_used: 0,
        hit_target: originalBytes <= TARGET_MAX_BYTES,
        target_bytes: TARGET_MAX_BYTES
      })
    }

    // Upload back (overwrite original)
    const outBuf = await readFile(bestPath)
    const up = await supabase.storage
      .from(bucket)
      .upload(objectName, outBuf, { upsert: overwrite, contentType: 'application/pdf', cacheControl: '3600' })

    // Cleanup temp files
    await unlink(inPath).catch(() => {})
    await unlink(bestPath).catch(() => {})

    if (up.error) {
      console.error('[upload] error:', up.error.message)
      return json(res, 500, { ok: false, error: 'upload_failed', details: up.error.message })
    }

    const ratio = Number((bestBytes / originalBytes).toFixed(3))
    const hitTarget = bestBytes <= TARGET_MAX_BYTES

    // Update your metadata row (by file_path = objectName)
    await updatePdfStorageRow(objectName, {
      compressedBytes: bestBytes,
      ratio,
      overwrote: true,
      hitTarget,
      passUsed
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
      target_bytes: TARGET_MAX_BYTES
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
    const body = await readJsonBody(req)
    return handleCompress(req, res, body)
  }

  return json(res, 404, { ok: false, error: 'not_found' })
})

server.listen(PORT, () => console.log(`pdf-compressor-service listening on :${PORT}`))
