#!/bin/bash
# Watch iCloud Baby Tracker backups for changes → trigger full pipeline
# fswatch debounces 30s after last change before triggering
# Note: bash ls can't read iCloud dirs from launchd, but Python can —
# so we skip bash-based file checks and let update.py handle it directly.

WATCH_DIR="/Users/chenzhu/Library/Mobile Documents/iCloud~com~nighp~babytracker/Documents/backups"
PROJECT_DIR="/Users/chenzhu/Claude Code Projects/breast-friend"
LOG="/tmp/breast_friend_update.log"
MIN_INTERVAL=60  # seconds between runs to prevent duplicate triggers

export PATH="/opt/homebrew/bin:$PATH"

LAST_RUN=0

echo "$(date): Watcher started, monitoring $WATCH_DIR" >> "$LOG"

/opt/homebrew/bin/fswatch -o --latency 30 "$WATCH_DIR" | while read -r count; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - LAST_RUN))
    if [ $ELAPSED -lt $MIN_INTERVAL ]; then
        echo "$(date): Skipping duplicate trigger (${ELAPSED}s since last run)" >> "$LOG"
        continue
    fi

    echo "$(date): Detected change, waiting 60s for iCloud download..." >> "$LOG"
    sleep 60

    echo "$(date): Running update..." >> "$LOG"
    cd "$PROJECT_DIR"
    source .venv/bin/activate
    python3 update.py --skip-sheets >> "$LOG" 2>&1
    UPDATE_EXIT=$?
    echo "$(date): update.py exit=$UPDATE_EXIT" >> "$LOG"
    LAST_RUN=$(date +%s)

    # Push to GitHub if any tracked data changed
    if ! git diff --quiet docs/index.html daily_summary.csv session_detail.csv 2>/dev/null; then
        git add docs/index.html daily_summary.csv session_detail.csv
        git commit -m "Auto-update: new data $(date '+%Y-%m-%d %H:%M')" >> "$LOG" 2>&1
        git push origin main >> "$LOG" 2>&1
        echo "$(date): Pushed to GitHub" >> "$LOG"
    else
        echo "$(date): No data changes to push" >> "$LOG"
    fi
done
