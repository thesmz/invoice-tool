import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from google.cloud import documentai_v1 as documentai
from google.api_core.client_options import ClientOptions
import re
import time

st.set_page_config(page_title="Reconciliation", layout="wide", page_icon="⚖️")

# --- AUTHENTICATION ---
if "gcp_service_account" not in st.secrets:
    st.error("Secrets not found. Please setup secrets in app.py first.")
    st.stop()

creds_dict = dict(st.secrets["gcp_service_account"])

# --- HELPER: CALL GOOGLE DOC AI ---
def get_text_from_docai(file_content, project_id, loc, proc_id):
    """Sends PDF to Google and returns the full text string"""
    opts = ClientOptions(api_endpoint=f"{loc}-documentai.googleapis.com")
    creds = Credentials.from_service_account_info(creds_dict)
    client = documentai.DocumentProcessorServiceClient(client_options=opts, credentials=creds)
    
    name = client.processor_path(project_id, loc, proc_id)
    raw_document = documentai.RawDocument(content=file_content, mime_type="application/pdf")
    request = documentai.ProcessRequest(name=name, raw_document=raw_document)
    
    result = client.process_document(request=request)
    return result.document.text

# --- PARSER: REGEX LOGIC ---
def parse_docai_text(full_text):
    """Parses the raw text string returned by Google"""
    transactions = []
    
    # 1. Split into lines
    lines = full_text.split('\n')
    
    # Regex for Date (YYYY/MM/DD)
    date_pattern = re.compile(r'^(\d{4}/\d{1,2}/\d{1,2})')
    
    for line in lines:
        line = line.strip()
        if not line: continue
        
        # 2. Check for Date at start
        match = date_pattern.match(line)
        if match:
            date_str = match.group(1)
            
            # 3. Tokenize
            parts = line.split()
            if len(parts) < 3: continue
            
            # 4. Find Numbers from the END (Backwards)
            # Logic: We expect [Desc] [Withdrawal] [Deposit] [Balance]
            # We want the 'Withdrawal' amount.
            
            numeric_values = []
            for part in reversed(parts):
                clean = part.replace(',', '').replace('¥', '')
                if clean.isdigit():
                    numeric_values.append(int(clean))
                else:
                    break # Stop when we hit text
            
            # Usually: [Balance, Deposit(0), Withdrawal] or [Balance, Withdrawal]
            # Rakuten Example: 2025/11/28  振込 カ）カガヤ  150,000  1,200,000
            # Numeric found (reversed): [1200000, 150000]
            
            if len(numeric_values) >= 2:
                # The Withdrawal is the one BEFORE the Balance (which is last)
                withdrawal = numeric_values[1] 
                
                # 5. Extract Vendor (Yellow Part)
                # Everything between Date and the Numbers
                # parts[0] is Date.
                # numeric_values covers the last N tokens.
                
                desc_end_index = len(parts) - len(numeric_values)
                desc_tokens = parts[1 : desc_end_index]
                description = " ".join(desc_tokens)
                
                transactions.append({
                    "Date": date_str,
                    "Bank Description": description,
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
st.title("⚖️ Monthly Reconciliation (Powered by Doc AI)")

with st.sidebar:
    st.header("⚙️ Configuration")
    sheet_url = st.text_input("Google Sheet URL", placeholder="https://docs.google.com/spreadsheets/d/...")
    
    with st.expander("Doc AI Settings (Same as Invoices)"):
        project_id = st.text_input("Project ID", value="receipt-processor-479605")
        location = st.selectbox("Location", ["us", "eu"], index=0)
        processor_id = st.text_input("Processor ID", value="88cff36a297265dc")

if not sheet_url:
    st.stop()

# 1. UPLOAD
uploaded_file = st.file_uploader("1. Upload Rakuten PDF", type="pdf")

if uploaded_file:
    # A. Use Google AI to Read Text
    with st.spinner("🤖 Google AI is reading the Japanese text..."):
        file_content = uploaded_file.read()
        try:
            full_text = get_text_from_docai(file_content, project_id, location, processor_id)
            bank_df = parse_docai_text(full_text)
        except Exception as e:
            st.error(f"Google AI Failed: {e}")
            st.stop()
    
    if bank_df.empty:
        st.error("AI read the file but found no transactions. Check layout.")
        with st.expander("See Raw AI Text"):
            st.text(full_text)
        st.stop()
        
    st.success(f"✅ AI successfully read {len(bank_df)} transactions!")

    # B. Load System Data
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
            st.error("Missing columns in Google Sheet.")
            st.stop()
            
        paid_invoices = sys_df[sys_df[status_col] == "Paid"].copy()
    except Exception as e:
        st.error(f"Error loading sheet: {e}")
        st.stop()

    # C. Load Map
    mapping_dict = load_bank_mapping(sheet_url)
    
    # D. Match Logic
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
