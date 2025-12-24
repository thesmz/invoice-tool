import streamlit as st
import pandas as pd
import pdfplumber
import gspread
from google.oauth2.service_account import Credentials
import re
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

# --- ROBUST PARSER (REGEX LOGIC) ---
def parse_rakuten_pdf(file):
    """
    Parses Rakuten Bank PDF by splitting text lines.
    Structure: [Date] [Description/Vendor] [Withdrawal] [Deposit] [Balance]
    """
    transactions = []
    
    # Regex to find lines starting with Date (e.g., 2025/11/28)
    date_pattern = re.compile(r'^(\d{4}/\d{1,2}/\d{1,2})')
    
    with pdfplumber.open(file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text: continue
            
            lines = text.split('\n')
            for line in lines:
                # 1. Check if line starts with a Date
                match = date_pattern.match(line)
                if match:
                    date_str = match.group(1)
                    
                    # 2. Split line by whitespace
                    parts = line.split()
                    
                    # Rakuten lines usually look like:
                    # [2025/11/28] [振込 カ）カガヤ] [150,000] [1,200,000]
                    # OR
                    # [2025/11/28] [振込 カ）カガヤ] [150,000] [0] [1,200,000]
                    
                    if len(parts) < 3: continue
                    
                    # 3. Extract Numbers from the END of the line backwards
                    # We collect all valid numbers from the right side
                    numbers_found = []
                    
                    # Iterate backwards from the end of the line
                    last_text_index = len(parts) - 1
                    
                    for i in range(len(parts) - 1, 0, -1):
                        token = parts[i].replace(',', '').replace('¥', '')
                        if token.isdigit():
                            numbers_found.append(int(token))
                            last_text_index = i - 1 # Update where text ends
                        else:
                            # Stop once we hit non-number text (Description)
                            break
                    
                    # Numbers are collected in reverse: [Balance, Deposit?, Withdrawal]
                    # Example: [1200000, 150000] -> Withdrawal is 150000
                    
                    if len(numbers_found) >= 2:
                        # Withdrawal is usually the second number from the end (before balance)
                        # NOTE: Rakuten shows Withdrawal in col 3 and Deposit in col 4.
                        # If Deposit is empty, it might not appear in text extract.
                        # Assumption: We only care about Withdrawals (Money Out)
                        
                        # Let's assume the Withdrawal is the number just before Balance.
                        withdrawal_amt = numbers_found[1] 
                        
                        # 4. Extract Vendor Name (The Yellow Highlight)
                        # It is everything between Date (index 0) and the numbers we found
                        # parts[0] is Date.
                        # parts[1] to parts[last_text_index] is Description.
                        
                        desc_parts = parts[1 : last_text_index + 1]
                        description = " ".join(desc_parts)
                        
                        transactions.append({
                            "Date": date_str,
                            "Bank Description": description,
                            "Amount": withdrawal_amt
                        })
                        
    return pd.DataFrame(transactions)

# --- HELPER: GOOGLE SHEETS ---
def load_bank_mapping(sheet_url):
    try:
        sheet = client.open_by_url(sheet_url).worksheet("Bank Mapping")
        records = sheet.get_all_values()
        mapping = {}
        for row in records[1:]:
            if len(row) >= 2 and row[0]:
                mapping[row[0].strip()] = row[1].strip()
        return mapping
    except:
        return {}

def add_unknowns_to_sheet(sheet_url, new_names):
    try:
        sheet = client.open_by_url(sheet_url).worksheet("Bank Mapping")
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

# 1. UPLOAD
uploaded_file = st.file_uploader("1. Upload Rakuten PDF", type="pdf")

if uploaded_file:
    # A. Parse
    bank_df = parse_rakuten_pdf(uploaded_file)
    
    if bank_df.empty:
        st.error("Could not parse transactions. The PDF format might be different.")
        st.stop()
        
    st.caption(f"Parsed {len(bank_df)} transactions.")
    
    # Debug View (Optional, helps you see if parsing is correct)
    with st.expander("🔍 Debug: Check Parsed Data"):
        st.dataframe(bank_df)

    # B. Load System Data
    try:
        sheet = client.open_by_url(sheet_url).sheet1
        sys_df = pd.DataFrame(sheet.get_all_records())
        
        # Smart Column Search (Handles 'FB Amount' vs 'FB Amount (Tax incld.)')
        status_col = next((c for c in sys_df.columns if "Status" in c), None)
        fb_col = next((c for c in sys_df.columns if "FB" in c and "Amount" in c), None)
        vendor_col = next((c for c in sys_df.columns if "Vendor" in c), None)
        
        if not all([status_col, fb_col, vendor_col]):
            st.error("Missing columns in Google Sheet (Status, Vendor, or FB Amount)")
            st.stop()
            
        paid_invoices = sys_df[sys_df[status_col] == "Paid"].copy()
    except Exception as e:
        st.error(f"Error loading sheet: {e}")
        st.stop()

    # C. Load Map
    mapping_dict = load_bank_mapping(sheet_url)
    
    # D. Match
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
            for k, v in mapping_dict.items():
                if k in bank_desc:
                    trans_name = v
                    break
        
        if trans_name == "Unknown":
            unknown_names.add(bank_desc)
            
        # Match
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

    # E. Display
    st.divider()
    if unknown_names:
        st.warning(f"Found {len(unknown_names)} unknown names.")
        if st.button("☁️ Auto-Add Unknowns"):
            add_unknowns_to_sheet(sheet_url, list(unknown_names))
            st.success("Added! Please refresh.")
    
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("✅ Matched")
        st.dataframe(matches)
    with c2:
        st.subheader("❌ Unmatched")
        st.dataframe(unmatched_bank)
