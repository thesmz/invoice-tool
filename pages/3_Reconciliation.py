import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import unicodedata
import time
import re

st.set_page_config(page_title="Reconciliation", layout="wide", page_icon="⚖️")

# --- 1. CONFIGURATION ---
SKIP_KEYWORDS = [
    "振込手数料",       # Transfer Fees
    "カイガイソウキン",  # Overseas Remittance
    "JCBデビット",      # Debit Card
    "PE",             # PayEasy (Gov/Tax)
    "手数料",          # Generic Fees
    "口振"            # Auto-withdrawal
]

# --- 2. AUTHENTICATION ---
if "gcp_service_account" not in st.secrets:
    st.error("Secrets not found.")
    st.stop()

creds_dict = dict(st.secrets["gcp_service_account"])

# --- 3. HELPER: TEXT NORMALIZER ---
def normalize_japanese_text(text):
    """
    Standardizes Japanese text:
    1. Glues separated dots (ヘ ゛ -> ベ)
    2. Converts Half-width to Full-width (NFKC)
    """
    if not isinstance(text, str):
        return str(text)

    # Remove spaces before dots and glue them
    text = re.sub(r'\s+([゛゜ﾞﾟ])', r'\1', text)
    text = text.replace('\u309B', '\u3099').replace('\u309C', '\u309A')
    text = text.replace('ﾞ', '\u3099').replace('ﾟ', '\u309A')
    
    # Normalize
    text = unicodedata.normalize('NFC', text)
    text = unicodedata.normalize('NFKC', text)
    
    # Cleanup symbols
    text = text.replace('-', 'ー').replace('−', 'ー').replace('‐', 'ー')
    text = text.replace('　', ' ').strip()
    
    return text

# --- 4. PARSER: UNIVERSAL READER ---
def parse_rakuten_file(file):
    transactions = []
    df = None
    
    # STRATEGY 1: Excel (.xlsx)
    try:
        df = pd.read_excel(file)
    except:
        pass
    
    # STRATEGY 2: CSV (UTF-8 with BOM)
    if df is None:
        try:
            file.seek(0)
            df = pd.read_csv(file, encoding='utf-8-sig')
        except:
            pass

    # STRATEGY 3: CSV (CP932 / Shift-JIS)
    if df is None:
        try:
            file.seek(0)
            df = pd.read_csv(file, encoding='cp932')
        except:
            pass

    if df is None:
        st.error("Could not read the file. Please ensure it is a valid .xlsx or .csv file.")
        return pd.DataFrame()

    # Normalize Headers
    df.columns = [str(c).strip() for c in df.columns]
    
    # Identify Columns
    date_col = next((c for c in df.columns if "取引日" in c), None)
    amt_col = next((c for c in df.columns if "入出金" in c and "内容" not in c), None)
    desc_col = next((c for c in df.columns if "内容" in c), None)
    
    if not all([date_col, amt_col, desc_col]):
        st.error(f"Error: Columns not found. Found: {list(df.columns)}")
        return pd.DataFrame()

    # Process Rows
    for _, row in df.iterrows():
        try:
            # A. DESCRIPTION & NORMALIZE
            raw_desc = str(row[desc_col]).strip()
            norm_desc = normalize_japanese_text(raw_desc)
            
            # B. SKIP LOGIC
            if any(keyword in norm_desc for keyword in SKIP_KEYWORDS):
                continue
            
            # C. CLEAN VENDOR NAME (THE 7-DIGIT FIX)
            # Find 7 digits followed by a space, then capture the text after it
            match = re.search(r'\d{7}\s+(.+?)(?:$|[（(]依頼人)', norm_desc)
            
            if match:
                vendor_name = match.group(1).strip()
            else:
                # Fallback: remove (依頼人...) and use the whole string
                cleaned = norm_desc.split(' (依頼人')[0]
                cleaned = cleaned.split('(依頼人')[0]
                vendor_name = cleaned.strip()

            # D. AMOUNT
            val = row[amt_col]
            if pd.isna(val): continue
            
            if isinstance(val, str):
                amount = int(float(val.replace(',', '')))
            else:
                amount = int(val)
            
            # E. DATE
            raw_date = row[date_col]
            if isinstance(raw_date, pd.Timestamp):
                date_str = raw_date.strftime("%Y/%m/%d")
            else:
                s_date = str(raw_date).replace('/', '')
                if len(s_date) == 8:
                    date_str = f"{s_date[:4]}/{s_date[4:6]}/{s_date[6:]}"
                else:
                    date_str = str(raw_date)

            # Only Keep Withdrawals
            if amount < 0:
                transactions.append({
                    "Date": date_str,
                    "Bank Description": vendor_name,
                    "Amount": abs(amount)
                })

        except Exception:
            continue
            
    return pd.DataFrame(transactions)

# --- 5. GOOGLE SHEETS HELPERS ---
def load_bank_mapping(sheet_url):
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        # FIX: Point to "Bank Mapping" specifically
        sheet = client.open_by_url(sheet_url).worksheet("Bank Mapping")
        records = sheet.get_all_values()
        mapping = {}
        for row in records[1:]:
            if len(row) >= 2 and row[0]:
                key = normalize_japanese_text(row[0])
                mapping[key] = row[1].strip()
        return mapping
    except:
        return {}

def add_unknowns_to_sheet(sheet_url, new_names):
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        # FIX: Point to "Bank Mapping" specifically
        sheet = client.open_by_url(sheet_url).worksheet("Bank Mapping")
        rows = [[name, ""] for name in new_names]
        sheet.append_rows(rows)
        return True
    except:
        return False

# --- 6. MAIN APP ---
st.title("⚖️ Monthly Reconciliation")

with st.sidebar:
    st.header("⚙️ Configuration")
    sheet_url = st.text_input("Google Sheet URL", placeholder="https://docs.google.com/spreadsheets/d/...")

if not sheet_url:
    st.info("Please enter your Google Sheet URL.")
    st.stop()

# UPLOAD
uploaded_file = st.file_uploader("1. Upload Bank File (Excel or CSV)", type=["xlsx", "csv"])

if uploaded_file:
    # Use Universal Parser
    bank_df = parse_rakuten_file(uploaded_file)
    
    if bank_df.empty:
        st.error("No valid transactions found. Please check the file.")
        st.stop()
        
    st.success(f"✅ Loaded {len(bank_df)} transactions.")
    
    # Optional: Debug view
    with st.expander("🔍 Check Parsed Names (Debug)"):
        st.dataframe(bank_df.head())

    # LOAD SYSTEM DATA
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        
        # FIX: Point to "Invoice Summary" specifically (Not sheet1)
        sheet = client.open_by_url(sheet_url).worksheet("Invoice Summary")
        sys_df = pd.DataFrame(sheet.get_all_records())
        
        status_col = next((c for c in sys_df.columns if "Status" in c), None)
        fb_col = next((c for c in sys_df.columns if "FB" in c and "Amount" in c), None)
        vendor_col = next((c for c in sys_df.columns if "Vendor" in c), None)
        
        if not all([status_col, fb_col, vendor_col]):
            st.error("Google Sheet missing required columns in 'Invoice Summary' tab.")
            st.stop()
            
        paid_invoices = sys_df[sys_df[status_col] == "Paid"].copy()
    except Exception as e:
        st.error(f"Error loading Google Sheet: {e}")
        st.stop()

    # MATCHING
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

    # DISPLAY
    st.divider()
    
    if unknown_names:
        st.warning(f"Found {len(unknown_names)} unknown vendor names.")
        col_act1, col_act2 = st.columns([1, 2])
        with col_act1:
            if st.button("☁️ Auto-Add Unknowns to Mapping Sheet", type="primary"):
                with st.spinner("Saving..."):
                    if add_unknowns_to_sheet(sheet_url, list(unknown_names)):
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
