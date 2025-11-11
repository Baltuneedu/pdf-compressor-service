
#!/usr/bin/env bash
set -e

echo "[Render Build] Installing Ghostscript..."
apt-get update && apt-get install -y ghostscript
echo "[Render Build] Ghostscript installed successfully."

# Confirm Ghostscript version
gs --version
