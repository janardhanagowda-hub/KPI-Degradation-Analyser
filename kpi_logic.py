# kpi_logic.py
import io
import re
import zipfile
import pandas as pd

# -------------------------
# KPI Threshold Rules
# -------------------------
KPI_RULES = {
    # LTE (names aligned with your sheets)
    "data accessibility%": (90, 95),
    "volte accessibility%": (90, 95),
    "rrc connection success rate%": (90, 95),
    "data retainability%": (95, 97),
    "volte retainability%": (95, 97),
    "s1 success rate%": (95, 97),
    "macro-ap handin%": (90, 95),
    "ap-macro intra-freq handout%": (90, 95),
    "ap-macro inter-freq handout%": (90, 95),
    "intra freq x1- handover success rate": (90, 95),
    "inter freq x1- handover success rate": (90, 95),

    # 5G / ENDC
    "endc data accessibility": (90, 95),
    "endc data retainability": (95, 97),
    "endc ue retainability": (95, 97),
    "sgnb addition success rate": (95, 97),
    "endc intracu intrafreq handover success rate": (90, 95),
    "endc intracu interfreq handover success rate": (90, 95),

    # Present in your LTE sheet
    "updated-macro-ap-handin%": (90, 95),
}

# These are conceptual checks. They may not exist as physical columns.
ZERO_CHECK_KPIS = [
    "zero rrc connection success rate%",
    "if handover attempts are zero -applies to all flavors",
]

# We keep Period Start Time (exact case) and other identifiers safe
BASE_COLS_TEMPLATE = ["unnamed: 0", "Period Start Time", "carrier", "region", "site id", "site name"]

# Columns we force to pandas string dtype for Arrow-safety (identifiers only)
ARROW_ID_COLUMNS = ["site id", "carrier", "region", "site name", "unnamed: 0"]


# -------------------------
# Helpers
# -------------------------
def _coerce_identifier_cols_to_str(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce identifier-like and known mixed-type columns to string (leave Period Start Time as-is)."""
    if df is None or df.empty:
        return df
    out = df.copy()
    for col in ARROW_ID_COLUMNS:
        if col in out.columns:
            out[col] = out[col].astype("string")
    for c in ["Degraded KPI", "Severity", "Tech", "remarks"]:
        if c in out.columns:
            out[c] = out[c].astype("string")
    return out


def _canon(name: str) -> str:
    """
    Canonicalize a column/KPI label for robust matching across exports:
    - lowercase, strip
    - replace underscores with space
    - collapse all whitespace
    - unify fancy dashes to '-'
    - remove spaces before a trailing '%'
    """
    s = str(name).lower().strip()
    s = s.replace("_", " ")
    # unify different dash characters to normal hyphen
    s = re.sub(r"[‐‑‒–—−]", "-", s)
    # collapse internal whitespace
    s = re.sub(r"\s+", " ", s)
    # remove spaces immediately before the final %
    s = re.sub(r"\s*%$", "%", s)
    return s


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize columns for robust KPI matching, but restore 'Period Start Time'
    to its original case for output (exactly as requested).
    """
    df = df.copy()
    # First, simple normalization like you had
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace("_", " ", regex=False)
    )
    # Make percent spacing consistent (no space before %)
    df.columns = df.columns.str.replace(r"\s*%$", "%", regex=True)
    # Unify special dashes
    df.columns = df.columns.str.replace(r"[‐‑‒–—−]", "-", regex=True)
    # Collapse spaces
    df.columns = df.columns.str.replace(r"\s+", " ", regex=True)

    # Restore Period Start Time to exact case for outputs/UI
    if "period start time" in df.columns:
        df = df.rename(columns={"period start time": "Period Start Time"})
    return df


def detect_tech(kpi_label: str) -> str:
    k = str(kpi_label).lower()
    if "endc" in k or "sgnb" in k:
        return "5G"
    return "LTE"


# -------------------------
# Core
# -------------------------
def process_files_to_reports(uploaded_files):
    """
    Args:
        uploaded_files: Iterable of Streamlit UploadedFile (or any file-like with .name and .read())
    Returns:
        summary_df, final_df, excel_bytes, zip_bytes, missing_kpis
    """
    if not uploaded_files:
        return None, None, None, None, []

    # Read & normalize each uploaded file
    df_list = []
    for up in uploaded_files:
        try:
            temp_df = pd.read_excel(up)
        except Exception:
            # Skip unreadable file; UI can show warning if needed
            continue

        temp_df = normalize_columns(temp_df)
        # Do NOT create/format a 'date' column. We keep 'Period Start Time' as-is.
        df_list.append(temp_df)

    if not df_list:
        return None, None, None, None, []

    df = pd.concat(df_list, ignore_index=True)

    # Early Arrow-safe for identifiers
    df = _coerce_identifier_cols_to_str(df)

    # Build a map from canonicalized df column names -> actual column names
    df_col_map = { _canon(col): col for col in df.columns }

    # Canonicalize the KPI rules
    rules_canon = { _canon(k): v for k, v in KPI_RULES.items() }

    # Detect if any 5G/ENDC columns are present. If none, we'll skip NR rules in "missing".
    any_5g_present = any(("endc" in k or "sgnb" in k) for k in df_col_map.keys())

    summary_rows = []
    degraded_frames = []
    missing_kpis = []

    # Evaluate thresholds
    actual_kpi_cols_used = set()

    for kpi_key_canon, (crit, maj) in rules_canon.items():
        # Skip NR rules in "missing" if sheet has no NR columns at all
        if (("endc" in kpi_key_canon or "sgnb" in kpi_key_canon) and not any_5g_present):
            # If not present, simply ignore quietly
            continue

        if kpi_key_canon not in df_col_map:
            # Not found -> report as missing
            missing_kpis.append(kpi_key_canon)
            continue

        actual_col = df_col_map[kpi_key_canon]
        actual_kpi_cols_used.add(actual_col)

        # Build base cols that exist
        base_cols = [c for c in BASE_COLS_TEMPLATE if c in df.columns]

        temp = df[base_cols + [actual_col]].copy()

        # Compute severity splits
        critical_df = temp[temp[actual_col] < crit].copy()
        critical_df["Degraded KPI"] = actual_col
        critical_df["Percentage"] = critical_df[actual_col]
        critical_df["Severity"] = "Critical"

        major_df = temp[(temp[actual_col] >= crit) & (temp[actual_col] < maj)].copy()
        major_df["Degraded KPI"] = actual_col
        major_df["Percentage"] = major_df[actual_col]
        major_df["Severity"] = "Major"

        degraded_frames.append(critical_df)
        degraded_frames.append(major_df)
        summary_rows.append([actual_col, len(critical_df), len(major_df)])

    # Zero KPI checks — only if such columns actually exist; do NOT warn if absent
    for zk in ZERO_CHECK_KPIS:
        zk_canon = _canon(zk)
        if zk_canon in df_col_map:
            actual_col = df_col_map[zk_canon]
            base_cols = [c for c in BASE_COLS_TEMPLATE if c in df.columns]
            zero_df = df[df[actual_col] == 0][base_cols + [actual_col]].copy()
            zero_df["Degraded KPI"] = actual_col
            zero_df["Percentage"] = zero_df[actual_col]
            zero_df["Severity"] = "Critical"
            degraded_frames.append(zero_df)
            summary_rows.append([actual_col, len(zero_df), 0])

    summary_df = pd.DataFrame(summary_rows, columns=["KPI", "Critical Count", "Major Count"])

    # Combine degraded rows
    if degraded_frames:
        final_df = pd.concat(degraded_frames, ignore_index=True)
        # Drop the raw KPI numeric columns (keep derived fields)
        # Only drop the ones we actually used to avoid accidental header mismatches.
        final_df = final_df.drop(columns=list(actual_kpi_cols_used), errors="ignore")
        final_df["Tech"] = final_df["Degraded KPI"].apply(detect_tech)

        # Arrow-safe before groupby/merge
        final_df = _coerce_identifier_cols_to_str(final_df)

        # Trend vs Issue remarks using 'Period Start Time' if available
        if "Period Start Time" in final_df.columns and not final_df.empty:
            total_periods = final_df["Period Start Time"].nunique()
            counts = (
                final_df.groupby(["site id", "Degraded KPI"])["Period Start Time"]
                .nunique()
                .reset_index(name="period_count")
            )
            counts["remarks"] = counts["period_count"].apply(
                lambda x: "trend" if x == total_periods else "issue"
            )
            final_df = final_df.merge(
                counts[["site id", "Degraded KPI", "remarks"]],
                on=["site id", "Degraded KPI"],
                how="left",
            )
    else:
        final_df = pd.DataFrame()

    # Arrow-safe right before returning (idempotent)
    summary_df = _coerce_identifier_cols_to_str(summary_df)
    final_df = _coerce_identifier_cols_to_str(final_df)

    # Build in-memory Excel
    excel_buf = io.BytesIO()
    try:
        with pd.ExcelWriter(excel_buf, engine="xlsxwriter") as writer:
            summary_df.to_excel(writer, sheet_name="Summary", index=False)
            if not final_df.empty:
                final_df.to_excel(writer, sheet_name="Degraded_Sites", index=False)
    except Exception:
        excel_buf = io.BytesIO()
        with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
            summary_df.to_excel(writer, sheet_name="Summary", index=False)
            if not final_df.empty:
                final_df.to_excel(writer, sheet_name="Degraded_Sites", index=False)

    excel_bytes = excel_buf.getvalue()

    # Build in-memory ZIP
    from datetime import datetime
    from zoneinfo import ZoneInfo
    zip_buf = io.BytesIO()
    date_str = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d-%m-%Y")
    excel_filename = f"KPI_Degraded_Report_Combined_{date_str}.xlsx"
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(excel_filename, excel_bytes)
    zip_bytes = zip_buf.getvalue()

    # Sort & pretty the missing list for display
    missing_kpis = sorted(set(missing_kpis))
    return summary_df, final_df, excel_bytes, zip_bytes, missing_kpis