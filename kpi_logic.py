# kpi_logic.py
import io
import re
import zipfile
import pandas as pd

# -------------------------
# KPI Threshold Rules
# -------------------------
KPI_RULES = {
    "data accessibility %": (90, 95),
    "volte accessibility %": (90, 95),
    "rrc connection success rate %": (90, 95),
    "data retainability %": (95, 97),
    "volte retainability %": (95, 97),
    "s1 success rate %": (95, 97),
    "macro-ap handin %": (90, 95),
    "ap-macro intra-freq handout %": (90, 95),
    "ap-macro inter-freq handout %": (90, 95),
    "intra freq x1- handover success rate": (90, 95),
    "inter freq x1- handover success rate": (90, 95),
    "endc data accessibility": (90, 95),
    "endc data retainability": (95, 97),
    "endc ue retainability": (95, 97),
    "sgnb addition success rate": (95, 97),
    "endc intracu intrafreq handover success rate": (90, 95),
    "endc intracu interfreq handover success rate": (90, 95),
}

ZERO_CHECK_KPIS = [
    "zero rrc connection success rate %",
    "if handover attempts are zero -applies to all flavors",
]

BASE_COLS_TEMPLATE = ["unnamed: 0", "date", "carrier", "region", "site id", "site name"]

# ---- NEW: columns we will force to string to be Arrow-safe
ARROW_ID_COLUMNS = ["site id", "carrier", "region", "site name", "unnamed: 0"]

def _coerce_identifier_cols_to_str(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce identifier-like and known mixed-type columns to string."""
    if df is None or df.empty:
        return df
    out = df.copy()
    for col in ARROW_ID_COLUMNS:
        if col in out.columns:
            out[col] = out[col].astype("string")  # pandas string dtype (Arrow-friendly)
    # Ensure date is consistent as text since we format DD-MM-YYYY
    if "date" in out.columns:
        out["date"] = out["date"].astype("string")
    # Also ensure KPI labels are clean strings if present
    for c in ["Degraded KPI", "Severity", "Tech", "remarks"]:
        if c in out.columns:
            out[c] = out[c].astype("string")
    return out


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace("_", " ", regex=False)
    )
    return df


def extract_date_from_filename(filename: str) -> str:
    # YYYYMMDD anywhere in filename -> DD-MM-YYYY
    m = re.search(r"\d{8}", filename)
    if not m:
        return "Unknown"
    date_str = m.group()
    return f"{date_str[6:8]}-{date_str[4:6]}-{date_str[0:4]}"


def detect_tech(kpi: str) -> str:
    k = str(kpi).lower()
    if "endc" in k or "sgnb" in k:
        return "5G"
    return "LTE"


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
        formatted_date = extract_date_from_filename(getattr(up, "name", "Unknown"))
        temp_df.insert(1, "date", formatted_date)
        df_list.append(temp_df)

    if not df_list:
        return None, None, None, None, []

    df = pd.concat(df_list, ignore_index=True)

    # ---- NEW: make raw combined df Arrow-safe early
    df = _coerce_identifier_cols_to_str(df)

    # Threshold evaluations
    summary_data = []
    all_degraded_rows = []
    missing_kpis = []

    for kpi_raw, (crit, maj) in KPI_RULES.items():
        kpi = kpi_raw.lower().strip()
        if kpi not in df.columns:
            missing_kpis.append(kpi)
            continue

        base_cols = [c for c in BASE_COLS_TEMPLATE if c in df.columns]
        temp = df[base_cols + [kpi]].copy()

        critical_df = temp[temp[kpi] < crit].copy()
        critical_df["Degraded KPI"] = kpi
        critical_df["Percentage"] = critical_df[kpi]
        critical_df["Severity"] = "Critical"

        major_df = temp[(temp[kpi] >= crit) & (temp[kpi] < maj)].copy()
        major_df["Degraded KPI"] = kpi
        major_df["Percentage"] = major_df[kpi]
        major_df["Severity"] = "Major"

        all_degraded_rows.append(critical_df)
        all_degraded_rows.append(major_df)
        summary_data.append([kpi, len(critical_df), len(major_df)])

    # Zero KPI checks
    for kpi in ZERO_CHECK_KPIS:
        if kpi in df.columns:
            base_cols = [c for c in BASE_COLS_TEMPLATE if c in df.columns]
            zero_df = df[df[kpi] == 0][base_cols + [kpi]].copy()
            zero_df["Degraded KPI"] = kpi
            zero_df["Percentage"] = zero_df[kpi]
            zero_df["Severity"] = "Critical"
            all_degraded_rows.append(zero_df)
            summary_data.append([kpi, len(zero_df), 0])
        else:
            missing_kpis.append(kpi)

    summary_df = pd.DataFrame(summary_data, columns=["KPI", "Critical Count", "Major Count"])

    # Combine degraded rows
    if all_degraded_rows:
        final_df = pd.concat(all_degraded_rows, ignore_index=True)
        final_df = final_df.drop(columns=KPI_RULES.keys(), errors="ignore")
        final_df["Tech"] = final_df["Degraded KPI"].apply(detect_tech)

        # ---- NEW: Arrow-safe before groupby/merge
        final_df = _coerce_identifier_cols_to_str(final_df)

        if "date" in final_df.columns and not final_df.empty:
            total_dates = final_df["date"].nunique()
            counts = (
                final_df.groupby(["site id", "Degraded KPI"])["date"]
                .nunique()
                .reset_index(name="date_count")
            )
            counts["remarks"] = counts["date_count"].apply(
                lambda x: "trend" if x == total_dates else "issue"
            )
            final_df = final_df.merge(
                counts[["site id", "Degraded KPI", "remarks"]],
                on=["site id", "Degraded KPI"],
                how="left",
            )
    else:
        final_df = pd.DataFrame()

    # ---- NEW: Arrow-safe right before returning (idempotent)
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
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("KPI_Degraded_Report_Combined.xlsx", excel_bytes)
    zip_bytes = zip_buf.getvalue()

    return summary_df, final_df, excel_bytes, zip_bytes, sorted(set(missing_kpis))