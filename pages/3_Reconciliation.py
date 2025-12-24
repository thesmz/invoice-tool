import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from google.cloud import documentai_v1 as documentai
from google.api_core.client_options import ClientOptions
import unicodedata
import time
import re

st.set_page_config(page_title="Reconciliation", layout="wide", page_icon="⚖️")

# --- AUTHENTICATION ---
if "gcp_service_account" not in st.secrets:
    st.error("Secrets not found. Please setup secrets in app.py first.")
    st.stop()

creds_dict = dict(st.secrets["gcp_service_account"])

# --- HELPER: PARSE YOUR NEW CLEAN CSV ---
def parse_clean_zengin_csv(file):
    """
    Parses the 'rakuten nov' style CSV.
    1. Reads clean UTF-8 text.
    2. Converts 'Half-width' Katakana (ｱ) to 'Full-width' (ア) for better matching.
    """
    transactions = []
    
    # Read CSV (Header=None to safely grab by index)
    try:
        file.seek(0)
        df = pd.read_csv(file, header=None, dtype=str)
    except Exception as e:
        st.error(f"Failed to read CSV: {e}")
        return pd.DataFrame()
    
    # Filter: Look for rows where the first column is "2" (Transaction Data)
    # Zengin Format: Column 0 is the Record Type.
    # We filter safely converting to str to handle potential "Column1" headers
    df[0] = df[0].astype(str)
    data_rows = df[df[0] == '2']
    
    for _, row in data_rows.iterrows():
        try:
            # --- 1. DATE (Column Index 2) ---
            # Format: "71104" -> Reiwa 7, Nov 04 -> 2025/11/04
            raw_date = str(row[2]).strip()
            if len(raw_date) == 5: raw_date = "0" + raw_date # Pad if needed
            
            if len(raw_date) == 6:
                year_val = int(raw_date[:2]) # 07
                month_val = raw_date[2:4]    # 11
                day_val = raw_date[4:]       # 04
                
                # Reiwa Year Conversion (Reiwa 1 = 2019) -> Year + 2018
                full_year = 2018 + year_val
                date_str = f"{full_year}/{month_val}/{day_val}"
            else:
                date_str = raw_date # Fallback

            # --- 2. AMOUNT (Column Index 6) ---
            amount = int(row[6])
            
            # --- 3. VENDOR / DESCRIPTION (Column Index 14) ---
            raw_desc = str(row[14]).strip() if pd.notna(row[14]) else ""
            
            # [SMART FEATURE] Normalize Katakana
            # Converts "ﾔｻｶｼﾞﾄﾞｳｼﾔ" (Half) -> "ヤサカジドウシヤ" (Full)
            # This makes matching with your Google Sheet MUCH easier.
            clean_desc = unicodedata.normalize('NFKC', raw_desc)
            
            # Filter: Only show money leaving the bank (Withdrawals > 0)
            if amount > 0:
                transactions.append({
                    "Date": date_str,
                    "Bank Description": clean_desc,
                    "Amount": amount
                })
                
        except Exception as e:
            continue
            
    return pd.DataFrame(transactions)

# --- HELPER: GOOGLE SHEETS ---
def load_bank_mapping(sheet_url):
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(sheet_url).worksheet("Bank Mapping")
        records = sheet.get_all_values()
        mapping = {}
        for row in records[1:]:
            if len(row) >= 2 and row[0]:
                # Normalize mapping keys too, just in case
                key = unicodedata.normalize('NFKC', row[0].strip())
                mapping[key] = row[1].strip()
        return mapping
    except:
        return {}

def add_unknowns_to_sheet(sheet_url, new_names):
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(sheet_url).worksheet("Bank Mapping")
        rows = [[name, ""] for name in new_names]
        sheet.append_rows(rows)
        return True
    except:
        return False

# --- MAIN APP ---
st.title("⚖️ Monthly Reconciliation")

with st.sidebar:
    st.header("⚙️ Configuration")
    sheet_url = st.text_input("Google Sheet URL", placeholder="https://docs.google.com/spreadsheets/d/...")

if not sheet_url:
    st.info("Please enter your Google Sheet URL.")
    st.stop()

# 1. UPLOAD
uploaded_file = st.file_uploader("1. Upload Bank CSV (Recommended)", type=["csv", "xlsx"])

if uploaded_file:
    # PARSE THE CLEAN CSV
    bank_df = parse_clean_zengin_csv(uploaded_file)
    
    if bank_df.empty:
        st.error("Could not find transaction rows (Type '2'). Check file format.")
        st.stop()
        
    st.success(f"✅ Successfully loaded {len(bank_df)} transactions!")

    # 2. LOAD SYSTEM DATA
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        
        sheet = client.open_by_url(sheet_url).sheet1
        sys_df = pd.DataFrame(sheet.get_all_records())
        
        # Smart Column Finder
        status_col = next((c for c in sys_df.columns if "Status" in c), None)
        fb_col = next((c for c in sys_df.columns if "FB" in c and "Amount" in c), None)
        vendor_col = next((c for c in sys_df.columns if "Vendor" in c), None)
        
        if not all([status_col, fb_col, vendor_col]):
            st.error("Missing columns in Google Sheet. Need: Status, Vendor, FB Amount.")
            st.stop()
            
        paid_invoices = sys_df[sys_df[status_col] == "Paid"].copy()
    except Exception as e:
        st.error(f"Error loading Google Sheet: {e}")
        st.stop()

    # 3. MATCHING LOGIC
    mapping_dict = load_bank_mapping(sheet_url)
    matches = []
    unmatched_bank = []
    unknown_names = set()
    
    for idx, row in bank_df.iterrows():
        bank_desc = row['Bank Description']
        bank_amt = row['Amount']
        
        # Translate
        trans_name = "Unknown"
        if bank_desc in mapping_dict:
            trans_name = mapping_dict[bank_desc]
        else:
            # Partial Match
            for k, v in mapping_dict.items():
                if k in bank_desc:
                    trans_name = v
                    break
        
        if trans_name == "Unknown":
            unknown_names.add(bank_desc)
            
        # Match against Paid Invoices
        # Criteria: Vendor Name AND Amount
        match = paid_invoices[
            (paid_invoices[vendor_col] == trans_name) & 
            (paid_invoices[fb_col] == bank_amt)
        ]
        
        if not match.empty:
            matches.append({
                "Date": row['Date'],
                "Bank Name": bank_desc,
                "System Name": trans_name,
                "Amount": f"¥{bank_amt:,.0f}",
                "Status": "✅ Match"
            })
        else:
            unmatched_bank.append({
                "Date": row['Date'],
                "Bank Name": bank_desc,
                "Translated": trans_name,
                "Amount": f"¥{bank_amt:,.0f}",
                "Status": "❌ Missing"
            })

    # 4. DISPLAY RESULTS
    st.divider()
    
    # Auto-Add Button
    if unknown_names:
        st.warning(f"Found {len(unknown_names)} unknown vendor names.")
        col_act1, col_act2 = st.columns([1, 2])
        with col_act1:
            if st.button("☁️ Auto-Add Unknowns to Mapping Sheet", type="primary"):
                with st.spinner("Saving..."):
                    success = add_unknowns_to_sheet(sheet_url, list(unknown_names))
                    if success:
                        st.success("Added! Open Google Sheets 'Bank Mapping' tab to edit.")
                        time.sleep(2)
                        st.rerun()

    c1, c2 = st.columns(2)
    with c1:
        st.subheader(f"✅ Matched ({len(matches)})")
        if matches:
            st.dataframe(pd.DataFrame(matches), hide_index=True, use_container_width=True)
        else:
            st.info("No matches yet.")

    with c2:
        st.subheader(f"❌ Unmatched ({len(unmatched_bank)})")
        if unmatched_bank:
            st.dataframe(pd.DataFrame(unmatched_bank), hide_index=True, use_container_width=True)
            st.caption("These items left the bank but have no 'Paid' invoice in the system.")
