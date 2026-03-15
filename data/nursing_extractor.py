"""
nursing_extractor.py — Extract nursing transfer volume (ml) from free-text notes.

The notes field in liz_nursing.csv contains a wide variety of formats for
recording how many ml were transferred during a weighed nursing session.
This module normalises all observed variants into a float.

Return value semantics:
    None   = no note at all (pre-tracking; row excluded from transfer stats)
    0.0    = note exists but no reliable ml value found
    float  = extracted transfer volume in ml
"""

import re
import math
from typing import Optional


# ──────────────────────────────────────────────────────────────────
# PRIMARY PATTERN
# Matches all observed "XX ml" variants including:
#   "30.25ml", "30.25 mL", "exact: 30.25ml", "exact：22ml" (fullwidth colon),
#   "Exact = 19.25ml", "exact=32.53ml", "Weighed: 65 ml transferred",
#   "weighed：18 mL transferred", "38 mL transferred", "est 10ml",
#   "Did NOT weigh (est. 45 ml)", "23.7ml soft boobs weak suck",
#   "Exact: 35.8 mL / 1.3 oz"
# ──────────────────────────────────────────────────────────────────
_ML_PATTERN = re.compile(
    r"""
    (?:
        (?:exact|weighed|est)   # optional keyword prefix
        \s*[=:：]?\s*           # optional separator (ASCII :=, fullwidth ：)
    )?
    (\d+(?:\.\d+)?)             # CAPTURE: the numeric value
    \s*                         # optional space between number and unit
    (?:ml|mL)                   # unit — only ml / mL observed in data
    """,
    re.VERBOSE | re.IGNORECASE,
)

# ──────────────────────────────────────────────────────────────────
# SECONDARY PATTERN — bare decimal with no unit
# Handles the one confirmed case: "63.25" (2/26 7:54 AM)
# Only triggers when the entire trimmed note is just a decimal number.
# ──────────────────────────────────────────────────────────────────
_BARE_DECIMAL_PATTERN = re.compile(
    r"""
    ^\s*
    (\d+\.\d+)   # decimal number — require decimal point to avoid false positives
    \s*$
    """,
    re.VERBOSE,
)

# Pattern to detect "Did NOT weigh" strings so we can decide whether
# to look for an ml value inside them or just return 0.
_DID_NOT_WEIGH = re.compile(r"did\s+not\s+weigh", re.IGNORECASE)


def extract_nursing_ml(note) -> Optional[float]:
    """
    Extract nursing transfer volume from a nursing session note.

    Parameters
    ----------
    note : str | float | None
        The raw note string from liz_nursing.csv.  NaN floats and
        empty strings are treated as "no note".

    Returns
    -------
    None   — no note present (pre-tracking session)
    0.0    — note present but no reliable ml value
    float  — extracted transfer volume (ml)
    """
    # ── Handle missing / NaN notes ──
    if note is None:
        return None
    if isinstance(note, float) and math.isnan(note):
        return None
    note = str(note).strip()
    if not note:
        return None

    # ── "Did NOT weigh" entries ──
    # "Did NOT weigh (est.30)"   → no ml unit → 0.0
    # "Did NOT weigh (est. 45 ml)" → has ml unit → extract it
    if _DID_NOT_WEIGH.search(note):
        m = _ML_PATTERN.search(note)
        return float(m.group(1)) if m else 0.0

    # ── Primary: look for XX ml / XX mL anywhere in note ──
    m = _ML_PATTERN.search(note)
    if m:
        return float(m.group(1))

    # ── Secondary: bare decimal (entire note is just "63.25") ──
    m = _BARE_DECIMAL_PATTERN.match(note)
    if m:
        return float(m.group(1))

    # ── Note exists but nothing useful found ──
    return 0.0


# ──────────────────────────────────────────────────────────────────
# Quick self-test (run this file directly to verify)
# ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    cases = [
        # (input_note, expected_output)
        ("30.25ml",                          30.25),
        ("63.25",                            63.25),   # bare decimal
        ("41.25",                            41.25),   # bare decimal
        ("22ml. 12:11am 2/26",               22.0),
        ("30.25 mL",                         30.25),
        ("49.5ml",                           49.5),
        ("exact：33ml",                      33.0),    # fullwidth colon
        ("Exact = 19.25ml. Close to last pump", 19.25),
        ("exact: 30.25ml",                   30.25),
        ("exact：22ml",                      22.0),
        ("exact：30.25ml. But Short after last pump", 30.25),
        ("exact:24.75 ml",                   24.75),
        ("exact: 19.25ml",                   19.25),
        ("Exact: 16.5ml",                    16.5),
        ("24.75 mL transferred ",            24.75),
        ("exact：16.68ml",                   16.68),
        ("exact:35 mL",                      35.0),
        ("exact:68.75ml. strong sucking the entire time", 68.75),
        ("24.8 mL",                          24.8),
        ("25 ml",                            25.0),
        ("Exact: 35.75ml. after bath",       35.75),
        ("Exact: 24.75ml. <2hr cycle ",      24.75),
        ("Exact: 35.8 mL / 1.3 oz",         35.8),
        ("exact=32.53ml，1.1oz",             32.53),
        ("Exact:53ml 1.8oz",                 53.0),
        ("38.4 mL",                          38.4),
        ("exact=23.7ml",                     23.7),
        ("exact=20ml. sleepy sucking",       20.0),
        ("30 mL sleepy weak after first 5 min", 30.0),
        ("38 mL transferred，sleepy after initial 5min", 38.0),
        ("54 mL transferred ",               54.0),
        ("24 mL transferred ",               24.0),
        ("weighed：18 mL transferred",       18.0),
        ("Refuse the right nipple. Still fuzzy after that.", 0.0),
        ("Weighed: 35.5ml",                  35.5),
        ("est 10ml: laying down position when couldn't sleep", 10.0),
        ("Weighed: 30ml. 2hr  from last nursing, didn't sleep", 30.0),
        ("Did NOT weigh (est.30)",           0.0),
        ("Did NOT weigh (est. 45 ml)",       45.0),
        ("Weighed: 65 ml transferred (checked by scale)", 65.0),
        ("23.7ml soft boobs weak suck",      23.7),
        ("strong suck",                      0.0),
        ("45ml",                             45.0),
        (float("nan"),                       None),
        (None,                               None),
        ("",                                 None),
        ("Exact:41.25ml",                    41.25),
        ("27.5 mL",                          27.5),
        ("\"44ml\"",                         44.0),
    ]

    all_pass = True
    for note, expected in cases:
        result = extract_nursing_ml(note)
        status = "✅" if result == expected else "❌"
        if result != expected:
            all_pass = False
        print(f"{status}  {repr(note)!s:55s}  → {result!r}  (expected {expected!r})")

    print()
    print("All tests passed ✅" if all_pass else "SOME TESTS FAILED ❌")
