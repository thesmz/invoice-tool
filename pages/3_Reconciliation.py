import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import unicodedata
import time

st.set_page_config(page_title="Reconciliation", layout="wide", page_icon="⚖️")

# --- CONFIGURATION ---
SKIP_KEYWORDS = [
    "振込手数料",       # Transfer Fees
    "カイガイソウキン",  # Overseas Remittance
    "JCBデビット",      # JCB Debit (as requested)
    "PE",             # PayEasy / Ministry of Justice
    "手数料",          # Generic fees
    "口振"            # Auto-withdrawal (generic)
]

# --- AUTHENTICATION ---
if "gcp_service_account" not in st.secrets:
    st.error("Secrets not found. Please setup secrets in app.py first.")
    st.stop()

creds_dict = dict(st.secrets["gcp_service_account"])

# --- PARSER: RAKUTEN TRANSACTION HISTORY (NEW FORMAT) ---
def parse_rakuten_history_csv(file):
    """
    Parses 'Transaction History' CSV (Date, Amount, Balance, Content)
    Format: 20251101, -1925, 16924179, Description...
    """
    transactions = []
    
    # 1. Read File (Universal Loader)
    df = None
    try:
        # Try UTF-8 first (since your snippet was UTF-8)
        file.seek(0)
        df = pd.read_csv(file)
    except:
        try:
            # Try CP932 (Japanese Windows Standard)
            file.seek(0)
            df = pd.read_csv(file, encoding='cp932')
        except:
            return pd.DataFrame()

    # 2. Identify Columns
    # We look for "取引日" (Date) and "入出金内容" (Content)
    # The snippet showed: 取引日,入出金(円),残高(円),入出金先内容
    
    # Normalize headers to find them easily
    df.columns = [str(c).strip() for c in df.columns]
    
    date_col = next((c for c in df.columns if "取引日" in c), None)
    amt_col = next((c for c in df.columns if "入出金" in c and "内容" not in c), None)
    desc_col = next((c for c in df.columns if "内容" in c), None)
    
    if not all([date_col, amt_col, desc_col]):
        return pd.DataFrame() # Not the right format

    # 3. Process Rows
    for _, row in df.iterrows():
        try:
            # A. Extract Description
            raw_desc = str(row[desc_col]).strip()
            # Normalize: "ヤサカ　（カ" (Full width) -> "ヤサカ (カ" (Standard)
            # This also turns full-width spaces into normal spaces
            norm_desc = unicodedata.normalize('NFKC', raw_desc)
            
            # B. FILTERING (Skip Logic)
            if any(keyword in norm_desc for keyword in SKIP_KEYWORDS):
                continue
            
            # C. Extract Vendor Name (Smart logic)
            # Pattern: "Bank Branch Type Num VENDOR (ClientInfo)"
            # Example: "MITSUI... KYOTO... 12345 YASAKA (Client...)"
            
            vendor_name = norm_desc
            parts = norm_desc.split(' ') # Split by space
            
            # Heuristic: If it looks like a bank transfer, the Vendor is usually the 5th item
            # (Bank, Branch, Type, Number, VENDOR)
            if len(parts) >= 5 and any(b in parts[0] for b in ['銀行', '金庫', '組合']):
                # Grab the 5th element (Index 4)
                candidate = parts[4]
                
                # Cleanup: Sometimes Vendor is "NAME (Client...)"
                # Remove anything starting with "("
                if '(' in candidate:
                    candidate = candidate.split('(')[0]
                
                vendor_name = candidate
            else:
                # If not a standard transfer string, just use the whole text
                # But clean up any trailing "(Client...)"
                if '（依頼人' in vendor_name:
                    vendor_name = vendor_name.split('（依頼人')[0]
                if '(依頼人' in vendor_name:
                    vendor_name = vendor_name.split('(依頼人')[0]

            vendor_name = vendor_name.strip()

            # D. Amount
            amount = int(str(row[amt_col]).replace(',', ''))
            
            # E. Date (20251104 -> 2025/11/04)
            raw_date = str(row[date_col])
            if len(raw_date) == 8:
                date_str = f"{raw_date[:4]}/{raw_date[4:6]}/{raw_date[6:]}"
            else:
                date_str = raw_date

            # Only keep Withdrawals (Negative numbers)
            if amount < 0:
                transactions.append({
                    "Date": date_str,
                    "Bank Description": vendor_name,
                    "Amount": abs(amount) # Store as positive for matching
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
uploaded_file = st.file_uploader("1. Upload Bank CSV", type=["csv", "xlsx"])

if uploaded_file:
    # Try the new Parser first
    bank_df = parse_rakuten_history_csv(uploaded_file)
    
    if bank_df.empty:
        # Fallback to Universal Parser (if user uploads Zengin/PDF later)
        st.warning("Could not read as Transaction History. Trying legacy formats...")
        # (Legacy parser code omitted for brevity, but you can keep parse_bank_file here if needed)
        st.error("Please upload the 'Transaction History' CSV (取引履歴明細).")
        st.stop()
        
    st.success(f"✅ Successfully loaded {len(bank_df)} transactions (Fees & Debits skipped)!")

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

    # 4. DISPLAY RESULTS
    st.divider()
    
    if unknown_names:
        st.warning(f"Found {len(unknown_names)} unknown vendor names.")
        col_act1, col_act2 = st.columns([1, 2])
        with col_act1:
            if st.button("☁️ Auto-Add Unknowns to Mapping Sheet", type="primary"):
                with st.spinner("Saving..."):
                    success = add_unknowns_to_sheet(sheet_url, list(unknown_names))
                    if success:
                        st.success("Added! Open 'Bank Mapping' tab to edit.")
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
