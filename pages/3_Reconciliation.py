import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from google.cloud import documentai_v1 as documentai
from google.api_core.client_options import ClientOptions
import re
import time
import io

st.set_page_config(page_title="Reconciliation", layout="wide", page_icon="⚖️")

# --- AUTHENTICATION ---
if "gcp_service_account" not in st.secrets:
    st.error("Secrets not found. Please setup secrets in app.py first.")
    st.stop()

creds_dict = dict(st.secrets["gcp_service_account"])

# --- HELPER: FIX MOJIBAKE (THE DECODER RING) ---
def fix_rakuten_mojibake(text):
    """
    Reverses the specific corruption:
    Shift-JIS -> ISO-8859-10 -> UTF-8
    """
    try:
        if not isinstance(text, str): return str(text)
        # 1. Encode back to bytes using the 'wrong' encoding (Nordic)
        # 2. Decode using the 'right' encoding (Japanese)
        return text.encode('iso8859_10').decode('shift_jis').strip()
    except:
        return text.strip()

# --- HELPER: PARSE CSV (ZENGIN FORMAT) ---
def parse_zengin_csv(file):
    """Parses Rakuten Bank Zengin CSV"""
    transactions = []
    
    # Read as standard UTF-8 (since it was saved that way)
    # Header=None because Zengin doesn't have a normal header row
    df = pd.read_csv(file, header=None, dtype=str)
    
    # Filter for Data Rows (First column == "2")
    # Zengin Structure: 1=Header, 2=Data, 8=Trailer, 9=End
    data_rows = df[df[0] == '2']
    
    for _, row in data_rows.iterrows():
        try:
            # Col 2: Date (YYMMDD) -> e.g., "071104" (Reiwa 7)
            raw_date = row[2]
            year_val = int(raw_date[:2])
            month_val = raw_date[2:4]
            day_val = raw_date[4:]
            
            # Year Conversion: 07 -> 2025 (Reiwa)
            # Reiwa 1 = 2019, so Year + 2018 = Gregorian
            full_year = 2018 + year_val
            date_str = f"{full_year}/{month_val}/{day_val}"
            
            # Col 6: Amount (Withdrawal/Deposit)
            amount = int(row[6])
            
            # Col 14: Description (Corrupted)
            raw_desc = row[14]
            # Apply the fix!
            clean_desc = fix_rakuten_mojibake(raw_desc)
            
            # Filter: Only withdrawals (>0)
            if amount > 0:
                transactions.append({
                    "Date": date_str,
                    "Bank Description": clean_desc,
                    "Amount": amount
                })
        except Exception as e:
            continue
            
    return pd.DataFrame(transactions)

# --- HELPER: CALL GOOGLE DOC AI (PDF FALLBACK) ---
def get_text_from_docai(file_content, project_id, loc, proc_id):
    opts = ClientOptions(api_endpoint=f"{loc}-documentai.googleapis.com")
    creds = Credentials.from_service_account_info(creds_dict)
    client = documentai.DocumentProcessorServiceClient(client_options=opts, credentials=creds)
    name = client.processor_path(project_id, loc, proc_id)
    raw_document = documentai.RawDocument(content=file_content, mime_type="application/pdf")
    request = documentai.ProcessRequest(name=name, raw_document=raw_document)
    result = client.process_document(request=request)
    return result.document.text

def parse_docai_text(full_text):
    transactions = []
    lines = full_text.split('\n')
    date_pattern = re.compile(r'(\d{4}/\d{1,2}/\d{1,2})')
    
    for line in lines:
        line = line.strip()
        if not line: continue
        
        date_match = date_pattern.search(line)
        if not date_match: continue
            
        date_str = date_match.group(1)
        start_idx = line.find(date_str) + len(date_str)
        content_after_date = line[start_idx:].strip()
        parts = content_after_date.split()
        
        numeric_values = []
        valid_indices = []
        for i in range(len(parts) - 1, -1, -1):
            token = parts[i].replace(',', '').replace('¥', '').replace('\\', '')
            if token.replace('-', '').isdigit():
                numeric_values.append(int(token))
                valid_indices.append(i)
            else:
                if len(numeric_values) >= 2: break
        
        if len(numeric_values) >= 2:
            withdrawal = numeric_values[1] # 2nd from last
            if withdrawal > 0:
                first_number_index = valid_indices[-1]
                desc_tokens = parts[:first_number_index]
                clean_desc = [t for t in desc_tokens if t.lower() not in ['rakuten', 'bank', '楽天', '銀行']]
                vendor_name = " ".join(clean_desc)
                
                if vendor_name:
                    transactions.append({
                        "Date": date_str,
                        "Bank Description": vendor_name,
                        "Amount": withdrawal
                    })
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
                mapping[row[0].strip()] = row[1].strip()
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
    with st.expander("Doc AI Settings (For PDF)"):
        project_id = st.text_input("Project ID", value="receipt-processor-479605")
        location = st.selectbox("Location", ["us", "eu"], index=0)
        processor_id = st.text_input("Processor ID", value="88cff36a297265dc")

if not sheet_url:
    st.info("Please enter your Google Sheet URL.")
    st.stop()

# 1. UPLOAD (CSV or PDF)
uploaded_file = st.file_uploader("1. Upload Bank Statement (CSV or PDF)", type=["csv", "pdf"])

if uploaded_file:
    bank_df = pd.DataFrame()
    
    # A. HANDLE CSV (PREFERRED)
    if uploaded_file.name.lower().endswith('.csv'):
        try:
            bank_df = parse_zengin_csv(uploaded_file)
            st.success(f"✅ CSV Parsed Successfully! Found {len(bank_df)} transactions.")
        except Exception as e:
            st.error(f"Error parsing CSV: {e}")
            
    # B. HANDLE PDF (FALLBACK)
    elif uploaded_file.name.lower().endswith('.pdf'):
        with st.spinner("🤖 Google AI is reading the Japanese text..."):
            file_content = uploaded_file.read()
            try:
                full_text = get_text_from_docai(file_content, project_id, location, processor_id)
                bank_df = parse_docai_text(full_text)
                st.success(f"✅ AI Parsed Successfully! Found {len(bank_df)} transactions.")
            except Exception as e:
                st.error(f"Google AI Failed: {e}")

    if bank_df.empty:
        st.stop()

    # C. Load System Data
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
            st.error("Missing columns in Google Sheet (Status, Vendor, FB Amount).")
            st.stop()
            
        paid_invoices = sys_df[sys_df[status_col] == "Paid"].copy()
    except Exception as e:
        st.error(f"Error loading Google Sheet: {e}")
        st.stop()

    # D. Match Logic
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

    # E. Display
    st.divider()
    if unknown_names:
        st.warning(f"Found {len(unknown_names)} unknown names.")
        if st.button("☁️ Auto-Add Unknowns"):
            add_unknowns_to_sheet(sheet_url, list(unknown_names))
            st.success("Added! Please refresh.")
            time.sleep(2)
            st.rerun()
    
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("✅ Matched")
        st.dataframe(pd.DataFrame(matches))
    with c2:
        st.subheader("❌ Unmatched")
        st.dataframe(pd.DataFrame(unmatched_bank))
