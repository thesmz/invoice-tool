import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import unicodedata
import time

st.set_page_config(page_title="Reconciliation", layout="wide", page_icon="⚖️")

# --- 1. CONFIGURATION ---
# Items containing these words will be completely ignored (Hidden)
SKIP_KEYWORDS = [
    "振込手数料",       # Transfer Fees
    "カイガイソウキン",  # Overseas Remittance fees
    "JCBデビット",      # Debit Card usage
    "PE",             # PayEasy (Taxes/Gov)
    "手数料",          # Generic fees
    "口振"            # Auto-withdrawals
]

# --- 2. AUTHENTICATION ---
if "gcp_service_account" not in st.secrets:
    st.error("Secrets not found.")
    st.stop()

creds_dict = dict(st.secrets["gcp_service_account"])

# --- 3. PARSER: RAKUTEN TRANSACTION HISTORY ---
def parse_rakuten_csv(file):
    transactions = []
    
    # Read the file (Try UTF-8, then Shift-JIS)
    try:
        file.seek(0)
        df = pd.read_csv(file) # Default UTF-8
    except:
        file.seek(0)
        df = pd.read_csv(file, encoding='cp932') # Japanese Windows

    # Clean headers (remove spaces)
    df.columns = [str(c).strip() for c in df.columns]
    
    # Identify required columns
    # We look for: '取引日', '入出金(円)', '入出金先内容'
    date_col = next((c for c in df.columns if "取引日" in c), None)
    amt_col = next((c for c in df.columns if "入出金" in c and "内容" not in c), None)
    desc_col = next((c for c in df.columns if "内容" in c), None)
    
    if not all([date_col, amt_col, desc_col]):
        st.error("Error: CSV must have '取引日', '入出金(円)', and '入出金先内容' columns.")
        return pd.DataFrame()

    for _, row in df.iterrows():
        try:
            # A. Extract Description & Normalize
            raw_desc = str(row[desc_col]).strip()
            # Converts "ヤサカ　（カ" (Full Width) -> "ヤサカ (カ" (Normal)
            norm_desc = unicodedata.normalize('NFKC', raw_desc)
            
            # B. SKIP LOGIC (Ignore fees/taxes)
            if any(keyword in norm_desc for keyword in SKIP_KEYWORDS):
                continue
            
            # C. Extract Vendor Name (Remove Bank Info & Client Info)
            # 1. Remove "(依頼人..." info at the end
            clean_desc = norm_desc.split(' (依頼人')[0]
            clean_desc = clean_desc.split('(依頼人')[0]
            
            # 2. Try to remove Bank Name at the start (e.g. "三井住友銀行...")
            # Pattern: If it has spaces, the Vendor is usually the last or 2nd to last item.
            parts = clean_desc.split(' ')
            if len(parts) >= 4 and any(b in parts[0] for b in ['銀行', '金庫', '組合']):
                # It's likely [Bank] [Branch] [Type] [Num] [VENDOR]
                # We take the part after the Account Number
                vendor_name = " ".join(parts[4:]) 
            else:
                vendor_name = clean_desc

            vendor_name = vendor_name.strip()

            # D. Amount
            # Rakuten format: "-193,168"
            amount_str = str(row[amt_col]).replace(',', '')
            amount = int(float(amount_str)) # Float conversion handles .0 safety
            
            # E. Date
            # Rakuten format: "20251104" -> "2025/11/04"
            raw_date = str(row[date_col])
            if len(raw_date) == 8:
                date_str = f"{raw_date[:4]}/{raw_date[4:6]}/{raw_date[6:]}"
            else:
                date_str = raw_date

            # Only process Withdrawals (Negative numbers)
            if amount < 0:
                transactions.append({
                    "Date": date_str,
                    "Bank Description": vendor_name,
                    "Amount": abs(amount) # Positive for matching
                })

        except Exception:
            continue
            
    return pd.DataFrame(transactions)

# --- 4. GOOGLE SHEETS HELPERS ---
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

# --- 5. MAIN APP ---
st.title("⚖️ Monthly Reconciliation")

with st.sidebar:
    st.header("⚙️ Configuration")
    sheet_url = st.text_input("Google Sheet URL", placeholder="https://docs.google.com/spreadsheets/d/...")

if not sheet_url:
    st.info("Please enter your Google Sheet URL.")
    st.stop()

# UPLOAD
uploaded_file = st.file_uploader("1. Upload 'Transaction History' CSV", type=["csv", "xlsx"])

if uploaded_file:
    bank_df = parse_rakuten_csv(uploaded_file)
    
    if bank_df.empty:
        st.error("No valid withdrawal transactions found. Check if the file is correct.")
        st.stop()
        
    st.success(f"✅ Loaded {len(bank_df)} transactions (Fees & Debit skipped).")

    # LOAD SYSTEM DATA
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(sheet_url).sheet1
        sys_df = pd.DataFrame(sheet.get_all_records())
        
        # Check Columns
        status_col = next((c for c in sys_df.columns if "Status" in c), None)
        fb_col = next((c for c in sys_df.columns if "FB" in c and "Amount" in c), None)
        vendor_col = next((c for c in sys_df.columns if "Vendor" in c), None)
        
        if not all([status_col, fb_col, vendor_col]):
            st.error("Your Google Sheet is missing 'Status', 'Vendor Name', or 'FB Amount'.")
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
            
        # Check for Match
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
