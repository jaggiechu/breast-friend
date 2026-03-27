#!/bin/bash
# Hourly data refresh for Breast Friend
# Reads latest iCloud .btbk, updates DB, regenerates CSV + HTML, pushes to GitHub Pages

cd "/Users/chenzhu/Claude Code Projects/breast-friend"
source .venv/bin/activate
python3 update.py --skip-sheets 2>&1 | tee -a /tmp/breast_friend_update.log

# Push to GitHub if any tracked data changed
if git diff --quiet docs/index.html daily_summary.csv session_detail.csv 2>/dev/null; then
    echo "$(date): No data changes" >> /tmp/breast_friend_update.log
else
    git add docs/index.html daily_summary.csv session_detail.csv
    git commit -m "Auto-update: new data $(date '+%Y-%m-%d %H:%M')"
    git push origin main
    echo "$(date): Pushed to GitHub" >> /tmp/breast_friend_update.log
fi
