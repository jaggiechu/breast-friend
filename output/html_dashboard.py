"""
html_dashboard.py — Generate a self-contained interactive HTML dashboard.

Uses Plotly for charts. The output is a single index.html with all data
and JS embedded inline — no external dependencies needed to view.
Mobile-responsive.
"""

import json
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config import DB_PATH, HTML_OUTPUT_DIR, BABY_NAME
from data.db import get_connection
from data.aggregator import get_daily_summary

log = logging.getLogger(__name__)


_ML_TO_OZ = 0.033814


def _make_intake_chart(df: pd.DataFrame) -> str:
    """Daily intake stacked bar chart (nursing + expressed + formula) with oz on right axis."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(
        x=df["date"], y=df["nursing_vol_ml"],
        name="Nursing Transfer", marker_color="#e377c2",
    ), secondary_y=False)
    fig.add_trace(go.Bar(
        x=df["date"], y=df["expressed_ml"],
        name="Expressed BM", marker_color="#ff7f0e",
    ), secondary_y=False)
    fig.add_trace(go.Bar(
        x=df["date"], y=df["formula_ml"],
        name="Formula", marker_color="#1f77b4",
    ), secondary_y=False)
    # Invisible trace on secondary axis to sync oz scale
    fig.add_trace(go.Bar(
        x=df["date"], y=df["total_intake_ml"] * _ML_TO_OZ,
        name="Total (oz)", marker_color="rgba(0,0,0,0)",
        showlegend=False, hoverinfo="skip",
    ), secondary_y=True)
    fig.update_layout(
        barmode="stack",
        title="Daily Intake",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=40, r=50, t=60, b=40),
    )
    fig.update_yaxes(title_text="ml", secondary_y=False)
    fig.update_yaxes(title_text="oz", secondary_y=True)
    # Keep oz axis range synced with ml
    ml_max = df["total_intake_ml"].max() if not df.empty else 800
    fig.update_yaxes(range=[0, ml_max * 1.1], secondary_y=False)
    fig.update_yaxes(range=[0, ml_max * 1.1 * _ML_TO_OZ], secondary_y=True)
    return fig.to_html(full_html=False, include_plotlyjs=False)


def _make_bm_pct_chart(df: pd.DataFrame) -> str:
    """BM% trend line."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["bm_pct"],
        mode="lines+markers", name="BM %",
        line=dict(color="#2ca02c", width=2),
        marker=dict(size=6),
    ))
    fig.update_layout(
        title="Breast Milk % of Total Intake",
        xaxis_title="Date", yaxis_title="%",
        yaxis=dict(range=[0, 100]),
        margin=dict(l=40, r=20, t=60, b=40),
    )
    return fig.to_html(full_html=False, include_plotlyjs=False)


def _make_supply_chart(df: pd.DataFrame) -> str:
    """BM supply breakdown: nursing vs pump."""
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["date"], y=df["nursing_vol_ml"],
        name="Nursing Transfer", marker_color="#e377c2",
    ))
    fig.add_trace(go.Bar(
        x=df["date"], y=df["pump_total_ml"],
        name="Pump Output", marker_color="#9467bd",
    ))
    fig.update_layout(
        barmode="stack",
        title="BM Supply (Nursing + Pump)",
        xaxis_title="Date", yaxis_title="ml",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=40, r=20, t=60, b=40),
    )
    return fig.to_html(full_html=False, include_plotlyjs=False)


def _make_weight_chart(df: pd.DataFrame) -> str:
    """Weight growth curve."""
    w = df[df["weight_lbs"].notna()].copy()
    if w.empty:
        return "<p>No weight data available</p>"
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=w["date"], y=w["weight_lbs"],
        mode="lines+markers", name="Weight (lbs)",
        line=dict(color="#d62728", width=2),
        marker=dict(size=6),
    ))
    fig.update_layout(
        title=f"{BABY_NAME}'s Weight (lbs)",
        xaxis_title="Date", yaxis_title="lbs",
        margin=dict(l=40, r=20, t=60, b=40),
    )
    return fig.to_html(full_html=False, include_plotlyjs=False)


def _make_nursing_transfer_chart(df: pd.DataFrame) -> str:
    """Nursing transfer volume per session (scatter from raw sessions)."""
    conn = get_connection(DB_PATH)
    try:
        ns = pd.read_sql(
            "SELECT datetime, nursing_ml FROM nursing_sessions "
            "WHERE nursing_ml > 0 ORDER BY datetime",
            conn,
        )
    finally:
        conn.close()

    if ns.empty:
        return "<p>No nursing transfer data</p>"

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ns["datetime"], y=ns["nursing_ml"],
        mode="markers", name="Per Session",
        marker=dict(size=5, color="#e377c2", opacity=0.6),
    ))
    # Rolling average by date
    ns["date"] = pd.to_datetime(ns["datetime"]).dt.date
    daily_avg = ns.groupby("date")["nursing_ml"].mean().reset_index()
    fig.add_trace(go.Scatter(
        x=daily_avg["date"], y=daily_avg["nursing_ml"],
        mode="lines", name="Daily Avg",
        line=dict(color="#d62728", width=2),
    ))
    fig.update_layout(
        title="Nursing Transfer per Session (ml)",
        xaxis_title="Date", yaxis_title="ml",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=40, r=20, t=60, b=40),
    )
    return fig.to_html(full_html=False, include_plotlyjs=False)


def _make_diaper_sleep_chart(df: pd.DataFrame) -> str:
    """Diaper counts + sleep hours dual-axis chart."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(
        x=df["date"],
        y=df["diaper_wet_count"] + df["diaper_dirty_count"] + df["diaper_mixed_count"],
        name="Diapers", marker_color="#8c564b", opacity=0.7,
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["sleep_total_min"] / 60,
        mode="lines+markers", name="Sleep (hrs)",
        line=dict(color="#17becf", width=2),
        marker=dict(size=5),
    ), secondary_y=True)
    fig.update_layout(
        title="Diapers & Sleep",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=40, r=40, t=60, b=40),
    )
    fig.update_yaxes(title_text="Diaper Count", secondary_y=False)
    fig.update_yaxes(title_text="Sleep (hours)", secondary_y=True)
    return fig.to_html(full_html=False, include_plotlyjs=False)


def _summary_cards(df: pd.DataFrame) -> str:
    """Generate HTML summary cards for latest day and 7-day average."""
    if df.empty:
        return ""

    latest = df.iloc[0]
    last7 = df.head(7)

    def _val(v, fmt=".0f"):
        if v is None or (isinstance(v, float) and str(v) == "nan"):
            return "—"
        return f"{v:{fmt}}"

    cards = f"""
    <div class="cards">
      <div class="card">
        <h3>Latest: {latest['date']}</h3>
        <div class="stat">Total Intake: <b>{_val(latest['total_intake_ml'])} ml</b></div>
        <div class="stat">BM%: <b>{_val(latest['bm_pct'], '.1f')}%</b></div>
        <div class="stat">BM Supply: <b>{_val(latest['bm_supply_ml'])} ml</b></div>
        <div class="stat">Nursing: <b>{_val(latest['nursing_vol_ml'])} ml</b> ({int(latest['nursing_sessions_count'])} sessions)</div>
        <div class="stat">Pump: <b>{_val(latest['pump_total_ml'])} ml</b> ({int(latest['pump_sessions_count'])} sessions)</div>
        <div class="stat">Weight: <b>{_val(latest['weight_lbs'], '.2f')} lbs</b></div>
      </div>
      <div class="card">
        <h3>7-Day Average</h3>
        <div class="stat">Total Intake: <b>{_val(last7['total_intake_ml'].mean())} ml</b></div>
        <div class="stat">BM%: <b>{_val(last7['bm_pct'].mean(), '.1f')}%</b></div>
        <div class="stat">BM Supply: <b>{_val(last7['bm_supply_ml'].mean())} ml</b></div>
        <div class="stat">Nursing: <b>{_val(last7['nursing_vol_ml'].mean())} ml/day</b></div>
        <div class="stat">Pump: <b>{_val(last7['pump_total_ml'].mean())} ml/day</b></div>
        <div class="stat">Diapers: <b>{_val((last7['diaper_wet_count']+last7['diaper_dirty_count']+last7['diaper_mixed_count']).mean(), '.1f')}/day</b></div>
      </div>
    </div>
    """
    return cards


def _data_table(df: pd.DataFrame) -> str:
    """Generate a sortable HTML data table."""
    cols = [
        ("Date", "date"),
        ("Intake", "total_intake_ml"),
        ("BM%", "bm_pct"),
        ("Nursing", "nursing_vol_ml"),
        ("Expressed", "expressed_ml"),
        ("Formula", "formula_ml"),
        ("BM Supply", "bm_supply_ml"),
        ("Pump", "pump_total_ml"),
        ("N%Supply", "nursing_pct_of_supply"),
        ("N Sess", "nursing_sessions_count"),
        ("P Sess", "pump_sessions_count"),
        ("Diapers", None),
        ("Sleep(h)", None),
        ("Weight", "weight_lbs"),
    ]

    header = "".join(f"<th>{c[0]}</th>" for c in cols)
    rows_html = []
    for _, r in df.iterrows():
        diapers = int(r["diaper_wet_count"] + r["diaper_dirty_count"] + r["diaper_mixed_count"])
        sleep_h = f"{r['sleep_total_min']/60:.1f}" if r["sleep_total_min"] else "—"

        is_incomplete = r.get("is_complete_day", 1) == 0
        no_nursing_ml = r.get("has_nursing_ml", 1) == 0
        row_class = ' class="incomplete"' if is_incomplete else ""

        cells = []
        for label, col in cols:
            if col:
                v = r[col]
                if v is None or (isinstance(v, float) and str(v) == "nan"):
                    cells.append("<td>—</td>")
                elif col == "bm_pct" or col == "nursing_pct_of_supply":
                    cells.append(f"<td>{v:.1f}%</td>")
                elif col == "weight_lbs":
                    cells.append(f"<td>{v:.2f}</td>")
                elif col in ("nursing_sessions_count", "pump_sessions_count"):
                    cells.append(f"<td>{int(v)}</td>")
                elif col == "nursing_vol_ml" and no_nursing_ml:
                    cells.append("<td>—*</td>")
                elif isinstance(v, str):
                    cells.append(f"<td>{v}</td>")
                else:
                    cells.append(f"<td>{v:.0f}</td>")
            elif label == "Diapers":
                cells.append(f"<td>{diapers}</td>")
            elif label == "Sleep(h)":
                cells.append(f"<td>{sleep_h}</td>")

        # Add incomplete marker to date cell
        if is_incomplete:
            cells[0] = cells[0].replace(r["date"], f'{r["date"]} *')

        rows_html.append(f"<tr{row_class}>{''.join(cells)}</tr>")

    return f"""
    <div class="table-container">
      <table>
        <thead><tr>{header}</tr></thead>
        <tbody>{''.join(rows_html)}</tbody>
      </table>
    </div>
    """


def generate_html() -> Path:
    """Generate the full dashboard HTML and write to docs/index.html."""
    conn = get_connection(DB_PATH)
    try:
        df = get_daily_summary(conn)
    finally:
        conn.close()

    if df.empty:
        log.warning("No data for HTML dashboard")
        return None

    # Sort chronologically for charts
    df_chrono = df.sort_values("date")

    # Filter: only complete days for trend charts (exclude today if incomplete)
    df_complete = df_chrono[df_chrono["is_complete_day"] == 1]

    # Filter: only days with nursing ml data for nursing-specific charts
    df_nursing_ml = df_complete[df_complete["has_nursing_ml"] == 1]

    # Build chart HTML — use complete days for fair comparison
    charts = [
        _make_intake_chart(df_nursing_ml),       # needs nursing_vol for accurate total
        _make_bm_pct_chart(df_nursing_ml),       # needs nursing_vol for BM%
        _make_supply_chart(df_nursing_ml),        # needs nursing_vol
        _make_weight_chart(df_chrono),            # weight uses all days
        _make_nursing_transfer_chart(df_nursing_ml),  # nursing ml data only
        _make_diaper_sleep_chart(df_complete),    # all complete days
    ]

    summary = _summary_cards(df)  # df is newest-first (includes today even if incomplete)
    table = _data_table(df)

    # Prepare data context JSON for chat (last 14 days, compact)
    chat_context_df = df.head(14).copy()
    chat_cols = ["date", "total_intake_ml", "bm_pct", "nursing_vol_ml", "expressed_ml",
                 "formula_ml", "bm_supply_ml", "pump_total_ml", "nursing_sessions_count",
                 "pump_sessions_count", "diaper_wet_count", "diaper_dirty_count",
                 "sleep_total_min", "weight_lbs", "is_complete_day"]
    chat_data_json = chat_context_df[chat_cols].fillna("").to_json(orient="records")

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    plotly_js_cdn = "https://cdn.plot.ly/plotly-2.35.2.min.js"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{BABY_NAME}'s Feeding Dashboard</title>
<script src="{plotly_js_cdn}"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #f5f5f5; color: #333; padding: 16px;
    max-width: 1200px; margin: 0 auto;
  }}
  h1 {{ text-align: center; margin-bottom: 4px; font-size: 1.5em; }}
  .updated {{ text-align: center; color: #888; font-size: 0.85em; margin-bottom: 16px; }}
  .cards {{
    display: flex; gap: 16px; margin-bottom: 20px; flex-wrap: wrap;
  }}
  .card {{
    flex: 1; min-width: 280px; background: white; border-radius: 12px;
    padding: 16px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);
  }}
  .card h3 {{ margin-bottom: 8px; color: #e377c2; font-size: 1.1em; }}
  .stat {{ padding: 4px 0; font-size: 0.95em; }}
  .chart {{ background: white; border-radius: 12px; padding: 8px; margin-bottom: 16px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
  .table-container {{
    overflow-x: auto; background: white; border-radius: 12px;
    padding: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);
  }}
  table {{ border-collapse: collapse; width: 100%; font-size: 0.85em; }}
  th {{ background: #e377c2; color: white; padding: 8px 6px; text-align: right; white-space: nowrap; position: sticky; top: 0; }}
  th:first-child {{ text-align: left; }}
  td {{ padding: 6px; text-align: right; border-bottom: 1px solid #eee; white-space: nowrap; }}
  td:first-child {{ text-align: left; font-weight: 600; }}
  tr:hover {{ background: #fdf2f8; }}
  tr.incomplete {{ background: #fff3cd; opacity: 0.85; }}
  .legend {{ font-size: 0.8em; color: #888; margin: 8px 0; }}
  /* Chat widget */
  .chat-container {{
    background: white; border-radius: 12px; padding: 16px; margin-top: 20px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
  }}
  .chat-container h2 {{ margin-bottom: 12px; }}
  .chat-messages {{
    max-height: 400px; overflow-y: auto; border: 1px solid #eee; border-radius: 8px;
    padding: 12px; margin-bottom: 12px; background: #fafafa; min-height: 60px;
  }}
  .chat-msg {{ margin-bottom: 10px; line-height: 1.5; }}
  .chat-msg.user {{ color: #1f77b4; }}
  .chat-msg.user::before {{ content: "You: "; font-weight: 600; }}
  .chat-msg.assistant {{ color: #333; }}
  .chat-input-row {{ display: flex; gap: 8px; }}
  .chat-input {{
    flex: 1; padding: 10px 14px; border: 2px solid #e377c2; border-radius: 8px;
    font-size: 1em; outline: none;
  }}
  .chat-input:focus {{ border-color: #d63384; }}
  .chat-send {{
    padding: 10px 20px; background: #e377c2; color: white; border: none;
    border-radius: 8px; font-size: 1em; cursor: pointer; white-space: nowrap;
  }}
  .chat-send:hover {{ background: #d63384; }}
  .chat-toast {{
    position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%);
    background: #333; color: white; padding: 10px 20px; border-radius: 8px;
    font-size: 0.9em; opacity: 0; transition: opacity 0.3s; pointer-events: none; z-index: 999;
  }}
  .chat-toast.show {{ opacity: 1; }}
  @media (max-width: 600px) {{
    body {{ padding: 8px; }}
    .cards {{ flex-direction: column; }}
    table {{ font-size: 0.75em; }}
    th, td {{ padding: 4px 3px; }}
  }}
</style>
</head>
<body>
<h1>🍼 {BABY_NAME}'s Feeding Dashboard</h1>
<div class="updated">Last updated: {now}</div>

{summary}

{''.join(f'<div class="chart">{c}</div>' for c in charts)}

<h2 style="margin: 20px 0 10px;">Daily Summary</h2>
<div class="legend">* = incomplete day (last record before 11pm) | —* = no nursing ml data for this date</div>
{table}

<div class="chat-container">
  <h2>Ask a Question</h2>
  <p style="color:#888;font-size:0.85em;margin-bottom:12px;">Type your question, click Send to copy it with data context, then paste in claude.ai</p>
  <div class="chat-input-row">
    <input type="text" class="chat-input" id="chatInput" placeholder="e.g. How is BM% trending this week?" onkeydown="if(event.key==='Enter')askQuestion()" />
    <button class="chat-send" onclick="askQuestion()">Send</button>
  </div>
</div>
<div class="chat-toast" id="toast">Copied! Opening claude.ai...</div>

<script>
const DATA_CONTEXT = {chat_data_json};

function askQuestion() {{
  const input = document.getElementById('chatInput');
  const q = input.value.trim();
  if (!q) return;

  const context = `Here is {BABY_NAME}'s feeding data (last 14 days, newest first).
Columns: date, total_intake_ml, bm_pct, nursing_vol_ml, expressed_ml, formula_ml, bm_supply_ml (=nursing+pump), pump_total_ml, nursing/pump session counts, diaper counts, sleep_total_min, weight_lbs, is_complete_day (1=full day).
Only compare complete days. Use ml and oz. Nursing ml data reliable from 2/20/26 onwards.

${{JSON.stringify(DATA_CONTEXT, null, 1)}}

My question: ${{q}}`;

  navigator.clipboard.writeText(context).then(() => {{
    const toast = document.getElementById('toast');
    toast.classList.add('show');
    setTimeout(() => toast.classList.remove('show'), 2500);
    input.value = '';
    setTimeout(() => window.open('https://claude.ai/new', '_blank'), 500);
  }});
}}
</script>

</body>
</html>"""

    HTML_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = HTML_OUTPUT_DIR / "index.html"
    out_path.write_text(html, encoding="utf-8")
    log.info("HTML dashboard written to %s", out_path)
    return out_path
