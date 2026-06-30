#!/usr/bin/env bash
# One-time setup: initialize git and push to your own GitHub repo.
# Usage:
#   1. Create an EMPTY repo on github.com named "upishield" (no README).
#   2. Run:  bash setup_github.sh <your-github-username>
set -e

USER="${1:?Usage: bash setup_github.sh <github-username>}"

git init
git add .
git commit -m "UPIShield: event-driven fraud detection pipeline (initial scaffold)"
git branch -M main
git remote add origin "https://github.com/${USER}/upishield.git"
git push -u origin main

echo "Done. Repo pushed to https://github.com/${USER}/upishield"
