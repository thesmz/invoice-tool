import streamlit as st
import pandas as pd
import pdfplumber
import gspread
from google.oauth2.service_account import Credentials
import time

st.set_page_config(page_title="Reconciliation", layout="wide", page_icon="⚖️")

# --- AUTHENTICATION ---
if "gcp_service_account" not in st.secrets:
    st.error("Secrets not found. Please setup secrets in app.py first.")
    st.stop()

creds_dict = dict(st.secrets["gcp_service_account"])
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
client = gspread.authorize(creds)

# --- HELPER: PARSE RAKUTEN PDF ---
def parse_rakuten_pdf(file):
    """Extracts Date, Description, and Withdrawal Amount from Rakuten PDF"""
    transactions = []
    with pdfplumber.open(file) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if table:
                for row in table:
                    # Rakuten Format check (Date | Desc | Withdrawal ...)
                    if not row or len(row) < 3: continue
                    
                    date_str = row[0]
                    desc = row[1]
                    withdrawal = row[2]
                    
                    if withdrawal and isinstance(withdrawal, str):
                        clean_w = withdrawal.replace(',', '').replace('¥', '').strip()
                        if clean_w.isdigit() and int(clean_w) > 0:
                            transactions.append({
                                "Date": date_str.replace('\n', ''), 
                                "Bank Description": desc.replace('\n', ' ').strip(),
                                "Amount": int(clean_w)
                            })
    return pd.DataFrame(transactions)

# --- HELPER: GOOGLE SHEETS ---
def load_bank_mapping(sheet_url):
    try:
        sheet = client.open_by_url(sheet_url).worksheet("Bank Mapping")
        records = sheet.get_all_values()
        mapping = {}
        # Skip header row (index 0)
        for row in records[1:]:
            if len(row) >= 2 and row[0]:
                mapping[row[0].strip()] = row[1].strip()
        return mapping
    except:
        return {}

def add_unknowns_to_sheet(sheet_url, new_names):
    """Bulk append new Japanese names to the mapping sheet"""
    try:
        sheet = client.open_by_url(sheet_url).worksheet("Bank Mapping")
        # Prepare rows: [Japanese Name, ""]
        rows = [[name, ""] for name in new_names]
        sheet.append_rows(rows)
        return True
    except Exception as e:
        st.error(f"Error saving to sheet: {e}")
        return False

# --- MAIN APP ---
st.title("⚖️ Monthly Reconciliation")

with st.sidebar:
    st.header("⚙️ Configuration")
    sheet_url = st.text_input("Google Sheet URL", placeholder="https://docs.google.com/spreadsheets/d/...")

if not sheet_url:
    st.info("Please enter your Google Sheet URL in the sidebar.")
    st.stop()

# 1. UPLOAD BANK STATEMENT
uploaded_file = st.file_uploader("1. Upload Rakuten PDF", type="pdf")

if uploaded_file:
    # A. Parse Bank Data
    bank_df = parse_rakuten_pdf(uploaded_file)
    
    if bank_df.empty:
        st.error("Could not find any withdrawal transactions in this PDF.")
        st.stop()

    # B. Load System Data (Paid Invoices Only)
    try:
        sheet = client.open_by_url(sheet_url).sheet1
        sys_data = sheet.get_all_records()
        sys_df = pd.DataFrame(sys_data)
        
        # Check if 'Status' column exists
        if "Status" not in sys_df.columns:
            st.error("Your Invoice Sheet is missing the 'Status' column.")
            st.stop()
            
        paid_invoices = sys_df[sys_df["Status"] == "Paid"].copy()
    except Exception as e:
        st.error(f"Error loading Invoice Sheet: {e}")
        st.stop()
    
    # C. Load Translation Map
    mapping_dict = load_bank_mapping(sheet_url)
    
    # --- MATCHING LOGIC ---
    matches = []
    unmatched_bank = []
    unknown_names_found = set() # Track unique unknown Japanese names
    
    for idx, bank_row in bank_df.iterrows():
        bank_desc = bank_row['Bank Description']
        bank_amt = bank_row['Amount']
        
        # 1. Try to translate
        translated_name = "Unknown"
        
        # Exact match check first
        if bank_desc in mapping_dict:
            translated_name = mapping_dict[bank_desc]
        else:
            # Partial match check
            for kana, eng in mapping_dict.items():
                if kana in bank_desc:
                    translated_name = eng
                    break
        
        # If still unknown, add to our list to save later
        if translated_name == "Unknown":
            unknown_names_found.add(bank_desc)
        
        # 2. Look for matching invoice
        # Criteria: Same Vendor Name AND Same Amount
        match = paid_invoices[
            (paid_invoices['Vendor Name'] == translated_name) & 
            (paid_invoices['FB Amount'] == bank_amt)
        ]
        
        if not match.empty:
            matches.append({
                "Date": bank_row['Date'],
                "Bank Name": bank_desc,
                "System Name": translated_name,
                "Amount": f"¥{bank_amt:,.0f}",
                "Status": "✅ Match"
            })
        else:
            unmatched_bank.append({
                "Date": bank_row['Date'],
                "Bank Name": bank_desc,
                "Translated": translated_name,
                "Amount": f"¥{bank_amt:,.0f}",
                "Status": "❌ Missing"
            })

    # --- DISPLAY RESULTS ---
    st.divider()
    
    # Show "Action Needed" box if there are unknown names
    if unknown_names_found:
        st.warning(f"⚠️ Found {len(unknown_names_found)} unknown Japanese vendor names.")
        
        col_act1, col_act2 = st.columns([1, 2])
        with col_act1:
            if st.button("☁️ Auto-Add Unknowns to Mapping Sheet", type="primary"):
                with st.spinner("Saving to Google Sheets..."):
                    success = add_unknowns_to_sheet(sheet_url, list(unknown_names_found))
                    if success:
                        st.success("✅ Added! Open your Google Sheet 'Bank Mapping' tab and type the English names.")
                        time.sleep(3)
                        st.rerun()

    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader(f"✅ Matched ({len(matches)})")
        if matches:
            st.dataframe(pd.DataFrame(matches), hide_index=True, use_container_width=True)
        else:
            st.info("No matches found yet.")

    with c2:
        st.subheader(f"❌ Unmatched / Unknown ({len(unmatched_bank)})")
        if unmatched_bank:
            st.dataframe(pd.DataFrame(unmatched_bank), hide_index=True, use_container_width=True)
