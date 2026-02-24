# app.py
import streamlit as st
from kpi_logic import process_files_to_reports

# ---------------------------------------------------------
#  LOGIN PAGE (Hard-coded)
# ---------------------------------------------------------
def login_page():
    st.title("🔐 Login to KPI Analysis Tool")

    # Input fields
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    # Login button
    if st.button("Login"):
        if username == "admin" and password == "admin123":
            st.session_state["logged_in"] = True
            st.success("Login successful! Redirecting...")
            st.rerun()
        else:
            st.error("Invalid username or password")

# If user not logged in → show login page
if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
    login_page()
    st.stop()


# ---------------------------------------------------------
# Helper to make dataframes Arrow-safe for Streamlit
# ---------------------------------------------------------
def _arrow_safe(df):
    if df is None:
        return df
    df = df.copy()
    for col in [
        "site id", "carrier", "region", "site name", "unnamed: 0",
        "Degraded KPI", "Severity", "Tech", "remarks", "date"
    ]:
        if col in df.columns:
            df[col] = df[col].astype("string")
    return df


# ---------------------------------------------------------
#  MAIN APP UI
# ---------------------------------------------------------
st.set_page_config(page_title="KPI Degraded Report", layout="wide")

st.title("📊 KPI Degraded Report")
st.caption("Upload daily/periodic KPI Excel files; combine, evaluate against thresholds, and download the report.")

# Sidebar tips
with st.sidebar:
    st.header("How to use")
    st.markdown(
        """
        1. Upload one or more Excel files (`.xlsx`/`.xls`)  
        2. Wait for processing  
        3. Preview Summary and Degraded Sites  
        4. Download Excel or ZIP  
        """
    )
    if st.button("Logout"):
        st.session_state["logged_in"] = False
        st.rerun()


# File upload section
files = st.file_uploader(
    "Upload one or more Excel files",
    type=["xlsx", "xls"],
    accept_multiple_files=True,
)


if files:
    with st.spinner("Processing files..."):
        summary_df, final_df, excel_bytes, zip_bytes, missing_kpis = process_files_to_reports(files)

    if summary_df is None:
        st.error("No readable Excel files were uploaded.")
        st.stop()

    # Arrow-safe conversion
    summary_df = _arrow_safe(summary_df)
    final_df = _arrow_safe(final_df)

    # Missing KPI alerts
    if missing_kpis:
        with st.expander("⚠️ Missing KPI columns (click to view)"):
            st.write(
                "These KPIs are in the rule set but were not found in the uploaded data.\n"
                "Note: Columns are normalized (case-insensitive, '_' replaced with spaces)."
            )
            st.code("\n".join(missing_kpis))

    st.subheader("📘 Summary")
    st.dataframe(summary_df, width="stretch")

    st.subheader("📕 Degraded Sites")
    if final_df is not None and not final_df.empty:
        st.dataframe(final_df, width="stretch")
    else:
        st.info("No degraded rows found.")

    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label="⬇️ Download Excel (Summary & Degraded_Sites)",
            data=excel_bytes,
            file_name="KPI_Degraded_Report_Combined.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with col2:
        st.download_button(
            label="⬇️ Download ZIP (contains Excel)",
            data=zip_bytes,
            file_name="KPI_Degraded_Report_Combined.zip",
            mime="application/zip",
        )

    st.success("✅ Report generated successfully.")
else:
    st.info("Upload your KPI Excel files to begin.")


# ---------------------------------------------------------
# FOOTER
# ---------------------------------------------------------
footer_html = """
<hr>
<div style='text-align: center; color: gray; padding-top: 10px;'>
    <b>RAN KPI Analysis Tool | LTE + 5G | 2026 | Developed by Janardhana T</b>
</div>
"""
st.markdown(footer_html, unsafe_allow_html=True)