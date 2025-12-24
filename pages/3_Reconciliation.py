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
    st.error("Secrets not found.")
    st.stop()

creds_dict = dict(st.secrets["gcp_service_account"])
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
client = gspread.authorize(creds)

# --- ROBUST PARSER V3 (ANCHOR LOGIC) ---
def parse_rakuten_pdf_debug(file):
    """
    Parses PDF and captures Raw Text for debugging.
    Logic: Anchors on Date at Start and Numbers at End.
    """
    transactions = []
    raw_lines = [] # To show user what we see
    
    # Regex: Start with Date (YYYY/MM/DD)
    date_pattern = re.compile(r'^(\d{4}/\d{1,2}/\d{1,2})')
    
    with pdfplumber.open(file) as pdf:
        for page in pdf.pages:
            # Try to keep physical layout
            text = page.extract_text(x_tolerance=2, y_tolerance=2) 
            if not text: continue
            
            lines = text.split('\n')
            for line in lines:
                raw_lines.append(line) # Save for debug view
                
                # 1. Find Date at start
                match = date_pattern.match(line)
                if match:
                    date_str = match.group(1)
                    
                    # 2. Tokenize line
                    parts = line.split()
                    
                    # We need at least: Date, Desc, Amount, Balance (4 parts) 
                    # OR Date, Desc, Amount (3 parts if balance hidden)
                    if len(parts) < 3: continue
                    
                    # 3. Find numbers from the END backwards
                    # We expect the last items to be Balance, Deposit, Withdrawal
                    # Let's collect all valid numbers at the end of the line
                    numeric_values = []
                    
                    # Walk backwards from the end
                    # Stop when we hit a string that is NOT a number (e.g., "Kagaya")
                    for part in reversed(parts):
                        clean = part.replace(',', '').replace('¥', '')
                        if clean.isdigit():
                            numeric_values.append(int(clean))
                        else:
                            break # Hit text, stop looking for numbers
                    
                    # numeric_values is now reversed: [Balance, Deposit?, Withdrawal?]
                    # Example: [1200000, 150000] -> Withdrawal is 150000
                    
                    if len(numeric_values) >= 2:
                        # Standard Case: Last is Balance, 2nd Last is Transaction
                        amount = numeric_values[1]
                        
                        # Reconstruct Description
                        # It's everything between Date (index 0) and the Start of Numbers
                        # Total tokens = len(parts)
                        # Number tokens = len(numeric_values)
                        # Desc tokens = Total - 1 (Date) - Number tokens
                        
                        desc_end_index = len(parts) - len(numeric_values)
                        desc_tokens = parts[1 : desc_end_index]
                        description = " ".join(desc_tokens)
                        
                        transactions.append({
                            "Date": date_str,
                            "Bank Description": description,
                            "Amount": amount
                        })
    
    return pd.DataFrame(transactions), raw_lines

# --- HELPER: MAPPING ---
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
    except:
        return False

# --- MAIN APP ---
st.title("⚖️ Monthly Reconciliation (Debug Mode)")

with st.sidebar:
    st.header("⚙️ Configuration")
    sheet_url = st.text_input("Google Sheet URL", placeholder="https://docs.google.com/spreadsheets/d/...")

if not sheet_url:
    st.stop()

# 1. UPLOAD
uploaded_file = st.file_uploader("1. Upload Rakuten PDF", type="pdf")

if uploaded_file:
    # A. Parse with Debug Info
    bank_df, raw_text_lines = parse_rakuten_pdf_debug(uploaded_file)
    
    # --- DEBUG SECTION ---
    with st.expander("👀 View Raw PDF Text (Click if parsing fails)", expanded=False):
        st.write("This is exactly what the computer sees. Check if lines look correct:")
        st.text("\n".join(raw_text_lines[:20])) # Show first 20 lines
        st.write("...")
    
    if bank_df.empty:
        st.error("❌ Parsing Failed. No transactions found.")
        st.info("Check the 'View Raw PDF Text' box above. Does the text look garbled?")
        st.stop()
        
    st.success(f"✅ Successfully parsed {len(bank_df)} transactions!")

    # B. Load System Data
    try:
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
