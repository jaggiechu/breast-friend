#!/bin/bash
# Watch iCloud Baby Tracker backups for changes → trigger full pipeline
# fswatch debounces 30s after last change before triggering

WATCH_DIR="/Users/chenzhu/Library/Mobile Documents/iCloud~com~nighp~babytracker/Documents/backups"
PROJECT_DIR="/Users/chenzhu/Claude Code Projects/breast-friend"
LOG="/tmp/breast_friend_update.log"
MAX_RETRIES=6
RETRY_DELAY=10
MIN_INTERVAL=60  # seconds between runs to prevent duplicate triggers

export PATH="/opt/homebrew/bin:$PATH"

LAST_RUN=0

echo "$(date): Watcher started, monitoring $WATCH_DIR" >> "$LOG"

# Wait for the latest .btbk file to be fully downloaded from iCloud
wait_for_download() {
    local attempt=1
    while [ $attempt -le $MAX_RETRIES ]; do
        local latest
        # Use ls + grep — iCloud placeholder files may not match shell globs
        latest=$(ls -t "$WATCH_DIR/" 2>/dev/null | grep '\.btbk$' | head -1)
        [ -n "$latest" ] && latest="$WATCH_DIR/$latest"
        if [ -z "$latest" ]; then
            echo "$(date): No .btbk files found, retry $attempt/$MAX_RETRIES..." >> "$LOG"
        elif /opt/homebrew/bin/python3 -c "import zipfile; zipfile.ZipFile('$latest', 'r').close()" 2>/dev/null; then
            echo "$(date): File ready: $(basename "$latest")" >> "$LOG"
            return 0
        else
            echo "$(date): File still downloading, retry $attempt/$MAX_RETRIES..." >> "$LOG"
        fi
        sleep $RETRY_DELAY
        attempt=$((attempt + 1))
    done
    echo "$(date): File not ready after $MAX_RETRIES retries, skipping" >> "$LOG"
    return 1
}

/opt/homebrew/bin/fswatch -o --latency 30 "$WATCH_DIR" | while read -r count; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - LAST_RUN))
    if [ $ELAPSED -lt $MIN_INTERVAL ]; then
        echo "$(date): Skipping duplicate trigger (${ELAPSED}s since last run)" >> "$LOG"
        continue
    fi

    echo "$(date): Detected change, waiting for iCloud download..." >> "$LOG"

    if ! wait_for_download; then
        continue
    fi

    echo "$(date): Running update..." >> "$LOG"
    cd "$PROJECT_DIR"
    source .venv/bin/activate
    python3 update.py >> "$LOG" 2>&1
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
