#!/bin/bash
# Hourly data refresh for Breast Friend
# Reads latest iCloud .btbk, updates DB, regenerates CSV + HTML, pushes to GitHub Pages

cd "/Users/chenzhu/Claude Code Projects/breast-friend"
source .venv/bin/activate
python3 update.py --skip-sheets 2>&1 | tee -a /tmp/breast_friend_update.log

# Push updated HTML to GitHub Pages if changed
if git diff --quiet docs/index.html 2>/dev/null; then
    echo "$(date): No dashboard changes" >> /tmp/breast_friend_update.log
else
    git add docs/index.html daily_summary.csv
    git commit -m "Auto-update dashboard $(date '+%Y-%m-%d %H:%M')"
    git push origin main
    echo "$(date): Dashboard pushed to GitHub Pages" >> /tmp/breast_friend_update.log
fi
