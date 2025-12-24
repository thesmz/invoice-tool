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

# --- 3. HELPER: UNIVERSAL TEXT NORMALIZER (Algorithmic) ---
def normalize_japanese_text(text):
    """
    辞書を使わず、Unicodeの仕組みを使って自動的に濁点を結合する関数
    """
    if not isinstance(text, str):
        return str(text)

    # 1. まず標準正規化 (NFKC)
    # これで半角カナは全角になり、半角の濁点は結合されます。
    # しかし「全角の基底文字」+「全角の独立した濁点」の組み合わせは、これだけでは結合されません。
    text = unicodedata.normalize('NFKC', text)
    
    # 2. 独立した濁点・半濁点を「結合用文字」に置換する (ここがミソ)
    # \u309B (全角濁点 ゛) -> \u3099 (結合用濁点)
    # \u309C (全角半濁点 ゜) -> \u309A (結合用半濁点)
    text = text.replace('\u309B', '\u3099').replace('\u309C', '\u309A')
    
    # 3. もう一度正規化 (NFC)
    # 結合用文字は、前の文字と自動的に合体して1文字になります (カ + ゛ -> ガ)
    text = unicodedata.normalize('NFC', text)
    
    # 4. 記号の統一 (ハイフン類を長音「ー」へ)
    # 銀行データはマイナス記号などが混在しやすいため統一します
    text = text.replace('-', 'ー').replace('−', 'ー').replace('‐', 'ー')
    
    # 5. 余計な空白の削除
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
            
            # C. CLEAN VENDOR NAME
            # Remove (依頼人...)
            clean_desc = norm_desc.split(' (依頼人')[0]
            clean_desc = clean_desc.split('(依頼人')[0]
            
            # Remove Bank Name prefixes
            # Logic: If 4+ spaces and starts with Bank, take the tail
            parts = clean_desc.split(' ')
            if len(parts) >= 4 and any(b in parts[0] for b in ['銀行', '金庫', '組合']):
                vendor_name = " ".join(parts[4:]) 
            else:
                vendor_name = clean_desc

            vendor_name = vendor_name.strip()

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
        sheet = client.open_by_url(sheet_url).worksheet("Bank Mapping")
        records = sheet.get_all_values()
        mapping = {}
        for row in records[1:]:
            if len(row) >= 2 and row[0]:
                # Keys in mapping sheet should also be normalized!
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
    
    # Optional: Debug view to see if text is fixed
    with st.expander("🔍 Check Parsed Names (Debug)"):
        st.dataframe(bank_df.head())

    # LOAD SYSTEM DATA
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(sheet_url).sheet1
        sys_df = pd.DataFrame(sheet.get_all_records())
        
        status_col = next((c for c in sys_df.columns if "Status" in c), None)
        fb_col = next((c for c in sys_df.columns if "FB" in c and "Amount" in c), None)
        vendor_col = next((c for c in sys_df.columns if "Vendor" in c), None)
        
        if not all([status_col, fb_col, vendor_col]):
            st.error("Google Sheet missing required columns.")
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
