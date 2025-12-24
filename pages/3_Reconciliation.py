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

# --- HELPER: CALL GOOGLE DOC AI (THE "EYES") ---
def get_text_from_docai(file_content, project_id, loc, proc_id):
    """Google OCRを使って全テキストを取得"""
    opts = ClientOptions(api_endpoint=f"{loc}-documentai.googleapis.com")
    creds = Credentials.from_service_account_info(creds_dict)
    client = documentai.DocumentProcessorServiceClient(client_options=opts, credentials=creds)
    
    name = client.processor_path(project_id, loc, proc_id)
    raw_document = documentai.RawDocument(content=file_content, mime_type="application/pdf")
    request = documentai.ProcessRequest(name=name, raw_document=raw_document)
    
    result = client.process_document(request=request)
    return result.document.text

# --- PARSER: NOISE FILTER LOGIC ---
def parse_docai_text(full_text):
    """
    透かし文字(Rakuten Bank)だらけのテキストから、
    正規表現を使って有効な取引行だけを救出する。
    """
    transactions = []
    
    # 1. 改行で分割
    lines = full_text.split('\n')
    
    # 日付パターン (2025/11/01 など)
    # 行のどこかにこの日付が含まれていれば、それは取引行の可能性が高い
    date_pattern = re.compile(r'(\d{4}/\d{1,2}/\d{1,2})')
    
    for line in lines:
        line = line.strip()
        if not line: continue
        
        # 2. 強力なフィルタリング：日付が含まれない行は即座に捨てる
        # これにより "Rakuten Bank 楽天銀行..." だけの行を無視できます
        date_match = date_pattern.search(line)
        if not date_match:
            continue
            
        # 日付を取得
        date_str = date_match.group(1)
        
        # 3. ノイズ除去（透かし文字を消す）
        # 行の中から "Rakuten", "Bank", "楽天", "銀行" などのノイズを除去
        # ただし、ベンダー名にこれらが含まれる可能性もゼロではないので慎重に、
        # まずは単純にスペースで分解して解析する
        
        # 行内の日付より「後ろ」にあるテキストを取得
        # 例: "Rakuten 2025/11/04 カ）カガヤ 150,000 Bank" -> "カ）カガヤ 150,000 Bank"
        start_idx = line.find(date_str) + len(date_str)
        content_after_date = line[start_idx:].strip()
        
        # トークン化（空白で分割）
        parts = content_after_date.split()
        
        # 4. 数字（金額）を探す（後ろから）
        numeric_values = []
        valid_indices = []
        
        for i in range(len(parts) - 1, -1, -1):
            token = parts[i]
            # カンマと円記号を除去
            clean = token.replace(',', '').replace('¥', '').replace('\\', '')
            
            # 数字かどうかチェック（マイナスも考慮）
            if clean.replace('-', '').isdigit():
                numeric_values.append(int(clean))
                valid_indices.append(i)
            else:
                # 数字以外の文字が出たら、そこが金額エリアの境界線とみなす
                # ただし、"Bank" とか "Rakuten" みたいな単語が末尾についている可能性があるので
                # もう少し賢く判定する
                
                # もし既に2つ以上の数字（残高と出金額）が見つかっていれば終了
                if len(numeric_values) >= 2:
                    break
        
        # numeric_values は後ろから順に入っている [残高, 入金額?, 出金額?]
        
        if len(numeric_values) >= 1:
            # 金額候補が見つかった
            
            # 5. 出金額（Withdrawal）を特定する
            # 通常、一番右が「残高」、その左が「入金」、その左が「出金」
            # 出金がある行は、数字が2つ（出金、残高）または3つ（出金、0、残高）並ぶことが多い
            
            target_amount = 0
            is_withdrawal = False
            
            # 数字が2つ以上ある場合、2番目（後ろから2番目）を出金とみなす
            if len(numeric_values) >= 2:
                target_amount = numeric_values[1] # 0が残高、1が出金or入金
                
                # ここで判定：もしこの行が「入金」行なら、このロジックだと入金額を拾ってしまう。
                # しかし今は「支払い消込」ツールなので、あえてそのまま拾い、
                # DBとの照合時にマッチしなければ無視される、という手もある。
                # 簡易的に、この数値が0より大きければ採用
                if target_amount > 0:
                    is_withdrawal = True
            
            elif len(numeric_values) == 1:
                # 数字が1つしかない（残高しかない？）場合は無視、またはそれが金額かも？
                # 通常は残高だけ行にはならないので、解析ミスの可能性あり
                continue

            if is_withdrawal:
                # 6. ベンダー名（Description）の抽出
                # 日付の後ろから、最初に見つけた数字の前まで
                
                # 数字が始まった位置（partsのインデックス）
                first_number_index = valid_indices[-1] # valid_indicesは後ろから順に入ってるので最後が一番左の数字
                
                # ベンダー名部分のトークンを取得
                desc_tokens = parts[:first_number_index]
                
                # ノイズ除去: "Rakuten", "Bank", "楽天", "銀行" が単独で混ざっていたら消す
                clean_desc_tokens = []
                for t in desc_tokens:
                    # 完全に一致するノイズ単語を除外（部分一致だと社名が消える恐れあり）
                    if t.lower() not in ['rakuten', 'bank', '楽天', '銀行', '天銀行', '行']:
                        clean_desc_tokens.append(t)
                
                vendor_name = " ".join(clean_desc_tokens)
                
                # 空でなければ追加
                if vendor_name:
                    transactions.append({
                        "Date": date_str,
                        "Bank Description": vendor_name,
                        "Amount": target_amount
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
st.title("⚖️ Monthly Reconciliation (Powered by Google AI)")

with st.sidebar:
    st.header("⚙️ Configuration")
    sheet_url = st.text_input("Google Sheet URL", placeholder="https://docs.google.com/spreadsheets/d/...")
    
    with st.expander("Doc AI Settings"):
        project_id = st.text_input("Project ID", value="receipt-processor-479605")
        location = st.selectbox("Location", ["us", "eu"], index=0)
        processor_id = st.text_input("Processor ID", value="88cff36a297265dc")

if not sheet_url:
    st.info("Please enter your Google Sheet URL.")
    st.stop()

# 1. UPLOAD
uploaded_file = st.file_uploader("1. Upload Rakuten PDF", type="pdf")

if uploaded_file:
    # A. Use Google AI to Read Text (The "Eyes")
    with st.spinner("🤖 Google AI is reading the Japanese text..."):
        file_content = uploaded_file.read()
        try:
            full_text = get_text_from_docai(file_content, project_id, location, processor_id)
            bank_df = parse_docai_text(full_text)
        except Exception as e:
            st.error(f"Google AI Failed: {e}")
            st.stop()
    
    if bank_df.empty:
        st.error("AI read the file but could not extract valid transactions.")
        with st.expander("See Raw AI Text (Debug)"):
            st.text(full_text)
        st.stop()
        
    st.success(f"✅ AI successfully extracted {len(bank_df)} transactions!")

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
            st.error("Missing columns in Google Sheet. Check 'Status', 'Vendor Name', 'FB Amount'.")
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
