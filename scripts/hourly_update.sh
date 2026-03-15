#!/bin/bash
# Hourly data refresh for Breast Friend
# Reads latest iCloud .btbk, updates DB, regenerates CSV + HTML

cd "/Users/chenzhu/Claude Code Projects/breast-friend"
source .venv/bin/activate
python3 update.py --skip-sheets 2>&1 | tee -a /tmp/breast_friend_update.log
