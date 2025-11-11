
// server.js
// Compresses PDFs, updates Supabase metadata, and notifies OCR function when compression completes.

import { createClient } from '@supabase/supabase-js'
import { spawn } from 'child_process'
import { promises as fs } from 'fs'
import path from 'path'
import crypto from 'crypto'
import fetch from 'node-fetch'

// ---- ENVIRONMENT VARIABLES ----
const SUPABASE_URL = process.env.SUPABASE_URL || ''
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || ''
const GS_QUALITY = (process.env.GS_QUALITY || 'screen').trim().toLowerCase()
const TARGET_MAX_BYTES = Number(process.env.TARGET_MAX_BYTES || 900000)
const MAX_INPUT_BYTES = Number(process.env.MAX_INPUT_BYTES || 4000000)
const TMPDIR = '/tmp'
const SECRET = process.env.PDF_COMPRESSOR_SECRET || ''
const OCR_FUNCTION_URL = process.env.OCR_FUNCTION_URL || ''

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false },
})

// ------- HELPERS -------
function randomUUID() {
  return crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString('hex')
}

function buildGsArgs(level, inPath, outPath) {
  const base = [
    '-sDEVICE=pdfwrite',
    '-dCompatibilityLevel=1.4',
    `-dPDFSETTINGS=/${GS_QUALITY || 'screen'}`,
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
  return (await fs.stat(filePath)).size
}

// ---- Update Database + Notify OCR ----
async function updatePdfStorageRow(filePath, metrics = {}) {
  const { compressedBytes, ratio, overwrote, hitTarget, passUsed } = metrics
  console.log(`[db] Updating row for ${filePath}...`)

  const { data, error } = await supabase
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
    .select('id, file_path, status')
    .maybeSingle()

  if (error) {
    console.error('[db] Update failed:', error.message)
    return
  }

  console.log(`[db] Updated pdf_storage for ${filePath}, status = done`)

  // ---- Notify OCR ----
  if (data?.status === 'done' && OCR_FUNCTION_URL) {
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
      console.log(`[notify_ocr] ✅ OCR function notified for ${data.file_path}`)
    } catch (err) {
      console.error('[notify_ocr] ❌ Error notifying OCR:', err.message)
    }
  }

  return { data }
}

// ---- Compression Routine ----
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
        await fs.rename(outPath, nextIn)
        bestPath = nextIn
      } else {
        await fs.unlink(outPath).catch(() => {})
      }

      if (bestBytes <= TARGET_MAX_BYTES) break
    } catch (e) {
      console.warn(`[compress] ${level} failed:`, e.message)
    }
  }

  console.log(`[compress] Best level: ${bestLevel}, size: ${bestBytes}`)
  return { bestPath, bestBytes, bestLevel, passUsed }
}

// ---- MAIN HANDLER ----
export default async function handler(req, res) {
  console.log(`[api/server] ${req.method} /api/server called`)

  if (req.method !== 'POST') {
    res.status(405).json({ ok: false, error: 'Method not allowed' })
    return
  }

  try {
    const auth = req.headers['authorization'] || ''
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : ''
    if (!SECRET || token !== SECRET) {
      console.warn('[auth] Unauthorized request')
      res.status(401).json({ ok: false, error: 'unauthorized' })
      return
    }

    const body =
      req.body ??
      (await new Promise((resolve) => {
        let data = ''
        req.on('data', (chunk) => (data += chunk.toString()))
        req.on('end', () => {
          try {
            resolve(JSON.parse(data) || {})
          } catch {
            resolve({})
          }
        })
      }))

    const bucket = body?.bucket
    const objectName = body?.name ?? body?.path
    const overwrite = body?.overwrite !== false
    console.log(`[input] Bucket: ${bucket}, File: ${objectName}`)

    if (!bucket || !objectName) {
      res.status(400).json({ ok: false, error: 'missing bucket or name' })
      return
    }

    const { data: storageRows, error: storageError } = await supabase
      .from('pdf_storage')
      .select('status')
      .eq('file_path', objectName)
      .limit(1)
      .maybeSingle()

    if (storageError) {
      console.error('[db] Lookup error:', storageError.message)
      res.status(500).json({ ok: false, error: 'db_lookup_failed', details: storageError.message })
      return
    }

    if (!storageRows || storageRows.status === 'done') {
      console.log(`[compress] Skipping ${objectName} (already done)`)
      res.status(200).json({ ok: true, skipped: true, reason: 'status already done' })
      return
    }

    console.log(`[download] Fetching ${objectName} from bucket ${bucket}`)
    const dl = await supabase.storage.from(bucket).download(objectName)
    if (dl.error) {
      console.error('[download] Error:', dl.error.message)
      res.status(500).json({ ok: false, error: 'download_failed', details: dl.error.message })
      return
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

      res.status(200).json({
        ok: true,
        skipped: true,
        reason: 'file under 0.5MB',
        original_bytes: inBuf.length,
        compressed_bytes: inBuf.length,
      })
      return
    }

    if (inBuf.length > MAX_INPUT_BYTES) {
      console.log(`[advisory] File size ${inBuf.length} > MAX_INPUT_BYTES ${MAX_INPUT_BYTES}`)
    }

    const workDir = TMPDIR
    const inPath = path.join(workDir, `${randomUUID()}-in.pdf`)
    await fs.writeFile(inPath, inBuf)
    console.log(`[compress] Starting compression for ${objectName}`)

    const originalBytes = inBuf.length
    const { bestPath, bestBytes, bestLevel, passUsed } = await compressAdaptive(inPath, workDir)

    const shouldUpload = bestBytes < originalBytes
    if (!shouldUpload) {
      console.log(`[compress] No improvement for ${objectName}, skipping upload`)
      res.status(200).json({
        ok: true,
        overwrote: false,
        original_bytes: originalBytes,
        compressed_bytes: originalBytes,
        ratio: 1.0,
        quality: `${GS_QUALITY} (no improvement)`,
        pass_used: 0,
        hit_target: originalBytes <= TARGET_MAX_BYTES,
        target_bytes: TARGET_MAX_BYTES,
      })
      return
    }

    const outBuf = await fs.readFile(bestPath)
    console.log(`[upload] Uploading compressed file ${objectName}`)
    const up = await supabase.storage
      .from(bucket)
      .upload(objectName, outBuf, {
        upsert: overwrite,
        contentType: 'application/pdf',
        cacheControl: '3600',
      })

    await fs.unlink(inPath).catch(() => {})
    await fs.unlink(bestPath).catch(() => {})

    if (up.error) {
      console.error('[upload] Error:', up.error.message)
      res.status(500).json({ ok: false, error: 'upload_failed', details: up.error.message })
      return
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

    res.status(200).json({
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
    res.status(500).json({ ok: false, error: 'internal', details: String(e?.message || e) })
  }
}
