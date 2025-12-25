import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import unicodedata
import re
import time

st.set_page_config(page_title="Reconciliation", layout="wide", page_icon="⚖️")

# --- 1. CONFIGURATION ---
SKIP_KEYWORDS = [
    "振込手数料", "カイガイソウキン", "JCBデビット", "PE", "手数料", "口振"
]

# --- 2. SMART TEXT NORMALIZER ---
def smart_normalize(text):
    if not isinstance(text, str): return str(text)
    # Aggressive Glue: Remove spaces before dots
    text = re.sub(r'\s+([゛゜ﾞﾟ])', r'\1', text)
    # Convert separate dots to combined dots
    text = text.replace('\u309B', '\u3099').replace('\u309C', '\u309A')
    text = text.replace('ﾞ', '\u3099').replace('ﾟ', '\u309A')
    # Normalize
    text = unicodedata.normalize('NFC', text)
    text = unicodedata.normalize('NFKC', text)
    # Cleanup
    text = text.replace('-', 'ー').replace('−', 'ー').replace('‐', 'ー')
    text = text.replace('　', ' ').strip()
    return text

# --- 3. UNIVERSAL FILE READER ---
def read_rakuten_file(file):
    df = None
    try: df = pd.read_excel(file)
    except: pass
    if df is None:
        try:
            file.seek(0)
            df = pd.read_csv(file, encoding='utf-8-sig')
        except: pass
    if df is None:
        try:
            file.seek(0)
            df = pd.read_csv(file, encoding='cp932')
        except: pass
    if df is None: return pd.DataFrame()
    df.columns = [str(c).strip() for c in df.columns]
    return df

# --- 4. PARSER LOGIC ---
def parse_transactions(df):
    transactions = []
    
    date_col = next((c for c in df.columns if "取引日" in c), None)
    amt_col = next((c for c in df.columns if "入出金" in c and "内容" not in c), None)
    desc_col = next((c for c in df.columns if "内容" in c), None)
    
    if not all([date_col, amt_col, desc_col]):
        st.error(f"❌ Columns not found. We need '取引日', '入出金', '内容'. Found: {list(df.columns)}")
        return pd.DataFrame()

    for _, row in df.iterrows():
        try:
            raw_desc = str(row[desc_col])
            clean_desc = smart_normalize(raw_desc)
            if any(k in clean_desc for k in SKIP_KEYWORDS): continue
            
            val = row[amt_col]
            if pd.isna(val): continue
            amount = int(float(str(val).replace(',', '')))
            
            raw_date = row[date_col]
            if isinstance(raw_date, pd.Timestamp):
                date_str = raw_date.strftime("%Y/%m/%d")
            else:
                s = str(raw_date).replace('/', '')
                date_str = f"{s[:4]}/{s[4:6]}/{s[6:]}" if len(s) == 8 else str(raw_date)

            if amount < 0:
                transactions.append({
                    "Date": date_str,
                    "Bank Description": clean_desc,
                    "Amount": abs(amount)
                })
        except:
            continue
            
    return pd.DataFrame(transactions)

# --- 5. GOOGLE SHEETS ---
def get_gsheet_client():
    if "gcp_service_account" not in st.secrets:
        st.error("Secrets not found.")
        st.stop()
    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return gspread.authorize(creds)

def load_mapping(sheet_url):
    try:
        client = get_gsheet_client()
        sheet = client.open_by_url(sheet_url).worksheet("Bank Mapping")
        records = sheet.get_all_values()
        mapping = {}
        for row in records[1:]:
            if len(row) >= 2 and row[0]:
                key = smart_normalize(row[0])
                mapping[key] = row[1].strip()
        return mapping
    except: return {}

def add_mapping(sheet_url, bank_name, system_name=""):
    try:
        client = get_gsheet_client()
        sheet = client.open_by_url(sheet_url).worksheet("Bank Mapping")
        sheet.append_row([bank_name, system_name])
        return True
    except: return False

# --- 6. MAIN APP ---
st.title("⚖️ Monthly Reconciliation")

sheet_url = st.sidebar.text_input("Google Sheet URL", placeholder="https://docs.google.com...")
if not sheet_url: st.stop()

uploaded_file = st.file_uploader("1. Upload Bank File", type=["xlsx", "csv"])

if uploaded_file:
    # A. Read & Parse
    raw_df = read_rakuten_file(uploaded_file)
    if raw_df.empty:
        st.error("Could not read file.")
        st.stop()
        
    bank_df = parse_transactions(raw_df)
    st.success(f"✅ Loaded {len(bank_df)} withdrawals.")

    # B. Load System Data (FIXED: SMART COLUMN FINDER)
    client = get_gsheet_client()
    try:
        sys_data = client.open_by_url(sheet_url).sheet1.get_all_records()
        sys_df = pd.DataFrame(sys_data)
        
        # --- FIX STARTS HERE ---
        # Look for columns that *contain* keywords instead of exact match
        status_col = next((c for c in sys_df.columns if "Status" in c), None)
        vendor_col = next((c for c in sys_df.columns if "Vendor" in c), None)
        fb_col = next((c for c in sys_df.columns if "FB" in c and "Amount" in c), None)
        
        if not all([status_col, vendor_col, fb_col]):
            st.error(f"❌ Missing columns! Found: {list(sys_df.columns)}. Need: 'Status', 'Vendor', 'FB Amount'")
            st.stop()
            
        # Rename them internally so the rest of the code works
        paid_invoices = sys_df[sys_df[status_col] == "Paid"].copy()
        paid_invoices = paid_invoices.rename(columns={
            vendor_col: "Vendor Name",
            fb_col: "FB Amount"
        })
        # --- FIX ENDS HERE ---
        
    except Exception as e:
        st.error(f"Sheet Error: {e}")
        st.stop()

    # C. Smart Matching Logic
    mapping = load_mapping(sheet_url)
    
    matches = []
    unmatched = []
    
    for _, row in bank_df.iterrows():
        bank_desc = row['Bank Description']
        amount = row['Amount']
        
        # 1. Match Mapping
        matched_name = None
        for key, val in mapping.items():
            if key in bank_desc: 
                matched_name = val
                break
        
        # 2. Match System
        status = "❌ Missing"
        if matched_name:
            sys_match = paid_invoices[
                (paid_invoices["Vendor Name"] == matched_name) & 
                (paid_invoices["FB Amount"] == amount)
            ]
            if not sys_match.empty:
                status = "✅ Match"
        
        item = {
            "Date": row['Date'],
            "Bank Description": bank_desc,
            "Mapped Vendor": matched_name if matched_name else "Unknown",
            "Amount": f"¥{amount:,.0f}",
            "Status": status
        }
        
        if status == "✅ Match":
            matches.append(item)
        else:
            unmatched.append(item)

    # D. Display
    st.divider()
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader(f"✅ Matched ({len(matches)})")
        st.dataframe(matches, use_container_width=True)

    with c2:
        st.subheader(f"❌ Unmatched ({len(unmatched)})")
        st.dataframe(unmatched, use_container_width=True)
        
        if unmatched:
            st.write("---")
            st.write("### 📝 Quick Map")
            options = [u['Bank Description'] for u in unmatched if u['Mapped Vendor'] == "Unknown"]
            if options:
                selected_desc = st.selectbox("Select Bank Description", options)
                new_alias = st.text_input("Enter Key Word (e.g. 'ヤサカ')")
                
                if st.button("Save to Mapping Sheet"):
                    if new_alias:
                        add_mapping(sheet_url, new_alias, "") 
                        st.success(f"Added '{new_alias}'! Add English Name in Sheet.")
                        time.sleep(3)
                        st.rerun()
