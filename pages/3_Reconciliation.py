import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import unicodedata
import re
import time

st.set_page_config(page_title="Reconciliation", layout="wide", page_icon="⚖️")

# --- 1. CONFIGURATION ---
# If a line contains these, we skip it entirely (Fees, Debits, etc.)
SKIP_KEYWORDS = [
    "振込手数料", "カイガイソウキン", "JCBデビット", "PE", "手数料", "口振"
]

# --- 2. SMART TEXT NORMALIZER ---
def smart_normalize(text):
    """
    The 'Smarter' Cleaner.
    1. Glues separated dots (ヘ ゛ -> ベ) even if there are spaces.
    2. Standardizes all characters to Full-Width (NFKC).
    3. Standardizes dashes and spaces.
    """
    if not isinstance(text, str): return str(text)

    # A. Aggressive Glue: Remove spaces before Dakuten/Handakuten
    # Matches [Space(s)] + [Dakuten] and replaces with just [Dakuten]
    text = re.sub(r'\s+([゛゜ﾞﾟ])', r'\1', text)

    # B. Convert Standalone Dakuten to "Combining" Dakuten
    # This tells the computer: "These dots belong to the previous letter"
    text = text.replace('\u309B', '\u3099').replace('\u309C', '\u309A') # Full-width
    text = text.replace('ﾞ', '\u3099').replace('ﾟ', '\u309A')           # Half-width
    
    # C. Apply Unicode Normalization (NFC) -> Actually merges the characters
    text = unicodedata.normalize('NFC', text)
    
    # D. Apply Compatibility Normalization (NFKC) -> Fixes Half-width Kana
    text = unicodedata.normalize('NFKC', text)

    # E. Final Cleanup (Dashes and Spaces)
    text = text.replace('-', 'ー').replace('−', 'ー').replace('‐', 'ー')
    text = text.replace('　', ' ').strip()
    
    return text

# --- 3. UNIVERSAL FILE READER ---
def read_rakuten_file(file):
    """Reads Excel or CSV (UTF-8/Shift-JIS) automatically."""
    df = None
    
    # Try Excel
    try: df = pd.read_excel(file)
    except: pass
    
    # Try CSV (Excel-style UTF-8)
    if df is None:
        try:
            file.seek(0)
            df = pd.read_csv(file, encoding='utf-8-sig')
        except: pass

    # Try CSV (Japanese Shift-JIS)
    if df is None:
        try:
            file.seek(0)
            df = pd.read_csv(file, encoding='cp932')
        except: pass

    if df is None: return pd.DataFrame()

    # Clean Headers
    df.columns = [str(c).strip() for c in df.columns]
    return df

# --- 4. PARSER LOGIC ---
def parse_transactions(df):
    transactions = []
    
    # Find Columns
    date_col = next((c for c in df.columns if "取引日" in c), None)
    amt_col = next((c for c in df.columns if "入出金" in c and "内容" not in c), None)
    desc_col = next((c for c in df.columns if "内容" in c), None)
    
    if not all([date_col, amt_col, desc_col]):
        st.error(f"❌ Columns not found. We need '取引日', '入出金', '内容'. Found: {list(df.columns)}")
        return pd.DataFrame()

    for _, row in df.iterrows():
        try:
            # 1. Clean Description
            raw_desc = str(row[desc_col])
            clean_desc = smart_normalize(raw_desc)
            
            # 2. Skip Logic
            if any(k in clean_desc for k in SKIP_KEYWORDS): continue
            
            # 3. Clean Amount
            val = row[amt_col]
            if pd.isna(val): continue
            amount = int(float(str(val).replace(',', '')))
            
            # 4. Clean Date
            raw_date = row[date_col]
            if isinstance(raw_date, pd.Timestamp):
                date_str = raw_date.strftime("%Y/%m/%d")
            else:
                s = str(raw_date).replace('/', '')
                date_str = f"{s[:4]}/{s[4:6]}/{s[6:]}" if len(s) == 8 else str(raw_date)

            # 5. Only Withdrawals
            if amount < 0:
                transactions.append({
                    "Date": date_str,
                    "Bank Description": clean_desc, # We keep the FULL string (Safe!)
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
        # Read as dict: { "タカナシハンバイ": "Takanashi Sales" }
        records = sheet.get_all_values()
        mapping = {}
        for row in records[1:]:
            if len(row) >= 2 and row[0]:
                # Normalize the key too!
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

    # B. Load System Data
    client = get_gsheet_client()
    try:
        sys_data = client.open_by_url(sheet_url).sheet1.get_all_records()
        sys_df = pd.DataFrame(sys_data)
        
        # Check cols
        if not all(k in sys_df.columns for k in ["Status", "Vendor Name", "FB Amount"]):
            st.error("Sheet needs columns: Status, Vendor Name, FB Amount")
            st.stop()
            
        paid_invoices = sys_df[sys_df["Status"] == "Paid"].copy()
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
        
        # 1. Find mapped name
        # Logic: Does the long Bank Description CONTAIN any key from our mapping?
        # This handles prefixes/suffixes automatically!
        matched_name = None
        
        for key, val in mapping.items():
            if key in bank_desc: # "MITSUBISHI... YASAKA..." contains "YASAKA"
                matched_name = val
                break
        
        # 2. Match with System
        status = "❌ Missing"
        if matched_name:
            # Look for Vendor + Amount in System
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
        
        # E. Quick Add to Mapping
        if unmatched:
            st.write("---")
            st.write("### 📝 Quick Map")
            # Select an unmatched item to map
            options = [u['Bank Description'] for u in unmatched if u['Mapped Vendor'] == "Unknown"]
            if options:
                selected_desc = st.selectbox("Select Bank Description to Map", options)
                new_alias = st.text_input("Enter Key Word (e.g. 'ヤサカ')", help="Copy the unique part of the bank name here.")
                
                if st.button("Save to Mapping Sheet"):
                    if new_alias:
                        # We save the ALIAS (Short name) -> English Name
                        # But wait, usually we want to map:
                        # "ヤサカ" -> "Yasaka Taxi"
                        # User needs to ensure the Mapping Sheet has "Yasaka Taxi" in Col B.
                        
                        add_mapping(sheet_url, new_alias, "") 
                        st.success(f"Added '{new_alias}' to mapping! Go to your sheet and add the English name in Column B.")
                        time.sleep(3)
                        st.rerun()
