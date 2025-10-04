\# pdf-compressor-service (AGPLv3)



A minimal Ghostscript-powered microservice for compressing PDFs stored in Supabase Storage.



\## How it works

\- POST `/compress` with JSON `{ bucket, name, overwrite }`

\- Service downloads the file using Supabase Service Role

\- Runs Ghostscript (`-dPDFSETTINGS=/ebook` by default)

\- Uploads back to the same path (overwrites original)

\- Deletes temp files and returns a JSON summary



\## Endpoints

\- `GET /healthz` → `{ ok: true }`

\- `POST /compress` → `{ ok, original\_bytes, compressed\_bytes, ratio, overwrote }`



\## Environment variables

\- `SUPABASE\_URL`

\- `SUPABASE\_SERVICE\_ROLE`

\- `PDF\_COMPRESSOR\_SECRET`

\- `GS\_QUALITY` (`ebook` or `screen`)



\## License

See `LICENSE` (GNU AGPLv3). This microservice’s source is open; your main app (dashboard/Vercel) can remain separate.



