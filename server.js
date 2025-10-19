// server.js
// ---------------------------------------------------------------------------
// PDF Compressor Microservice
// ---------------------------------------------------------------------------
// 1. Receives JSON POST from Supabase trigger (fn_on_pdf_insert_call_compression)
// 2. Downloads PDF from Supabase Storage
// 3. Compresses with Ghostscript
// 4. Uploads the compressed file back (overwrite)
// 5. Updates pdf_storage row status + compression stats
//
// Environment Variables required in Render Dashboard:
//   PDF_COMPRESSOR_SECRET   — shared token (Authorization header OR JSON body)
//   SUPABASE_URL            — your Supabase project URL (e.g. https://xxxx.supabase.co)
//   SUPABASE_SERVICE_ROLE   — your Supabase Service Role key
//   GS_QUALITY              — Ghostscript preset: screen | ebook | printer | prepress (default: ebook)
// ---------------------------------------------------------------------------

import http from "node:http";
import { createClient } from "@supabase/supabase-js";
import { spawn } from "node:child_process";
import { writeFile, readFile, unlink, stat } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import path from "node:path";

const PORT = process.env.PORT || 8080;
const SECRET = process.env.PDF_COMPRESSOR_SECRET || "";
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE || "";
const GS_QUALITY = process.env.GS_QUALITY || "ebook";
const TMPDIR = "/tmp";

if (!SECRET || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  console.error("❌ Missing required environment variables.");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false, autoRefreshToken: false },
});

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------
function json(res, code, obj) {
  res.writeHead(code, { "Content-Type": "application/json" });
  res.end(JSON.stringify(obj, null, 2));
}

async function runGhostscript(inPath, outPath, quality) {
  const args = [
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.4",
    `-dPDFSETTINGS=/${quality}`,
    "-dNOPAUSE",
    "-dQUIET",
    "-dBATCH",
    `-sOutputFile=${outPath}`,
    inPath,
  ];

  await new Promise((resolve, reject) => {
    const ps = spawn("gs", args);
    let stderr = "";
    ps.stderr?.on("data", (d) => (stderr += d.toString()));
    ps.on("error", reject);
    ps.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`Ghostscript exited ${code}: ${stderr}`));
    });
  });
}

async function markStatusByPath(filePath, status, extra = {}) {
  const updates = { status, updated_at: new Date().toISOString(), ...extra };
  if (status === "processing") updates.processing_started_at = new Date().toISOString();
  if (status === "done") updates.processing_finished_at = new Date().toISOString();

  const { error } = await supabase.from("pdf_storage").update(updates).eq("file_path", filePath);
  if (error) console.error("❌ DB status update failed:", error.message);
  else console.log(`✅ pdf_storage.status → '${status}' (${filePath})`);
}

async function writeCompressionStats(filePath, originalBytes, compressedBytes) {
  const ratio = Number((compressedBytes / originalBytes).toFixed(3));
  const { error } = await supabase
    .from("pdf_storage")
    .update({
      compressed_size_bytes: compressedBytes,
      compression_ratio: ratio,
      overwrote: true,
      updated_at: new Date().toISOString(),
    })
    .eq("file_path", filePath);
  if (error) console.error("❌ Compression stats update failed:", error.message);
  else console.log(`📊 Compression ratio = ${ratio} (${filePath})`);
}

// ---------------------------------------------------------------------------
// Core Handler
// ---------------------------------------------------------------------------
async function handleCompress(body) {
  const token = body.token || "";
  if (token !== SECRET) throw new Error("Invalid secret token");

  const { bucket, file_name, file_path, file_url } = body;
  if (!bucket || !file_name || !file_path) throw new Error("Missing bucket/file_path parameters");

  const localIn = path.join(TMPDIR, `${randomUUID()}_${file_name}`);
  const localOut = path.join(TMPDIR, `${randomUUID()}_compressed.pdf`);

  console.log(`\n⚙️  Starting compression for: ${file_path}`);
  await markStatusByPath(file_path, "processing");

  try {
    // Step 1: Download file from Supabase Storage
    const { data, error: downloadErr } = await supabase.storage.from(bucket).download(file_path);
    if (downloadErr) throw new Error(`Download failed: ${downloadErr.message}`);
    const arrBuf = await data.arrayBuffer();
    await writeFile(localIn, Buffer.from(arrBuf));
    const { size: originalBytes } = await stat(localIn);

    console.log(`⬇️  Downloaded (${(originalBytes / 1e6).toFixed(2)} MB)`);

    // Step 2: Run Ghostscript compression
    await runGhostscript(localIn, localOut, GS_QUALITY);
    const { size: compressedBytes } = await stat(localOut);
    console.log(`🗜️  Compressed (${(compressedBytes / 1e6).toFixed(2)} MB)`);

    // Step 3: Upload compressed file (overwrite)
    const buf = await readFile(localOut);
    const { error: uploadErr } = await supabase.storage.from(bucket).upload(file_path, buf, {
      upsert: true,
      contentType: "application/pdf",
    });
    if (uploadErr) throw new Error(`Upload failed: ${uploadErr.message}`);
    console.log("⬆️  Uploaded compressed PDF back to Supabase.");

    // Step 4: Update DB row
    await writeCompressionStats(file_path, originalBytes, compressedBytes);
    await markStatusByPath(file_path, "done");
  } catch (err) {
    console.error("❌ Compression pipeline error:", err);
    await markStatusByPath(file_path, "error", { processing_error: err.message });
    throw err;
  } finally {
    await Promise.allSettled([unlink(localIn).catch(() => {}), unlink(localOut).catch(() => {})]);
  }
}

// ---------------------------------------------------------------------------
// HTTP Server
// ---------------------------------------------------------------------------
const server = http.createServer(async (req, res) => {
  if (req.method === "GET" && req.url === "/") {
    return json(res, 200, { status: "ok", message: "PDF compressor service active" });
  }

  if (req.method === "POST" && req.url === "/compress") {
    try {
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const body = JSON.parse(Buffer.concat(chunks).toString());
      await handleCompress(body);
      return json(res, 200, { ok: true, message: "Compression complete" });
    } catch (err) {
      console.error("❌ POST /compress failed:", err);
      return json(res, 500, { ok: false, error: err.message });
    }
  }

  json(res, 404, { error: "Not found" });
});

server.listen(PORT, () => {
  console.log(`🚀 PDF Compressor listening on port ${PORT}`);
});
