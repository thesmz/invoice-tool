import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import unicodedata
import re
import time

st.set_page_config(page_title="Reconciliation", layout="wide", page_icon="⚖️")

# --- 1. 設定 ---
SKIP_KEYWORDS = [
    "振込手数料", "カイガイソウキン", "JCBデビット", "PE", "手数料", "口振"
]

# --- 2. 文字化け・表記ゆれ修正 ---
def smart_normalize(text):
    if not isinstance(text, str): return str(text)
    
    # 1. 離れ離れの濁点をくっつける (ヘ ゛ -> ベ)
    text = re.sub(r'\s+([゛゜ﾞﾟ])', r'\1', text) # 空白除去
    text = text.replace('\u309B', '\u3099').replace('\u309C', '\u309A') # 結合文字へ
    text = text.replace('ﾞ', '\u3099').replace('ﾟ', '\u309A')
    
    # 2. 正規化実行
    text = unicodedata.normalize('NFC', text)  # 合体
    text = unicodedata.normalize('NFKC', text) # 全角化
    
    # 3. 記号統一
    text = text.replace('-', 'ー').replace('−', 'ー').replace('‐', 'ー')
    text = text.replace('　', ' ').strip()
    return text

# --- 3. 賢い社名抽出ロジック (NEW!) ---
def extract_vendor_name(raw_text):
    """
    「銀行名...数字7桁 社名 (依頼人...」 という構造を利用して社名だけを抜き出す。
    """
    # まず全体をきれいに正規化（濁点結合など）
    text = smart_normalize(raw_text)
    
    # パターン: [数字7桁] + [空白] + [社名] + [(依頼人 or 文末]
    # 例: 0556309　カ）ヘ゛リ－．フ゜ロシ゛エクト（依頼人名...
    match = re.search(r'\d{7}\s+(.+?)(?:$|[（(]依頼人)', text)
    
    if match:
        # 数字7桁の後ろの部分をそのまま採用！
        return match.group(1).strip()
    else:
        # 数字7桁がない場合（手数料など）は、(依頼人...)だけ消してそのまま使う
        cleaned = re.sub(r'[（(]依頼人.*', '', text)
        return cleaned.strip()

# --- 4. ファイル読込 ---
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

# --- 5. 取引データ解析 ---
def parse_transactions(df):
    transactions = []
    
    date_col = next((c for c in df.columns if "取引日" in c), None)
    amt_col = next((c for c in df.columns if "入出金" in c and "内容" not in c), None)
    desc_col = next((c for c in df.columns if "内容" in c), None)
    
    if not all([date_col, amt_col, desc_col]):
        st.error(f"❌ ファイル形式エラー: '取引日', '入出金', '内容' の列が見つかりません。")
        return pd.DataFrame()

    for _, row in df.iterrows():
        try:
            raw_desc = str(row[desc_col])
            
            # --- ここが新しい抽出ロジック ---
            vendor_name = extract_vendor_name(raw_desc)
            # ---------------------------
            
            if any(k in vendor_name for k in SKIP_KEYWORDS): continue
            
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
                    "Bank Description": vendor_name, # きれいに抽出された社名
                    "Amount": abs(amount)
                })
        except:
            continue
            
    return pd.DataFrame(transactions)

# --- 6. Google Sheets 連携 ---
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
        records = sheet.get_all_values() # 生データを取得
        mapping = {}
        for row in records[1:]: # ヘッダー飛ばし
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

# --- メインアプリ ---
st.title("⚖️ Monthly Reconciliation")

with st.sidebar:
    st.header("⚙️ 設定")
    sheet_url = st.text_input("Google Sheet URL", placeholder="https://docs.google.com...")
    
    selected_sheet = None
    if sheet_url:
        try:
            client = get_gsheet_client()
            sh = client.open_by_url(sheet_url)
            worksheets = [s.title for s in sh.worksheets()]
            selected_sheet = st.selectbox("請求書データのタブを選択", worksheets, index=0)
        except:
            st.error("URLが無効です")

if not sheet_url or not selected_sheet:
    st.info("Google SheetのURLを入力してください。")
    st.stop()

# 1. アップロード
uploaded_file = st.file_uploader("1. 銀行の明細ファイルをアップロード (Excel/CSV)", type=["xlsx", "csv"])

if uploaded_file:
    # A. 読込 & 解析
    raw_df = read_rakuten_file(uploaded_file)
    if raw_df.empty:
        st.error("ファイルを読み込めませんでした。")
        st.stop()
        
    bank_df = parse_transactions(raw_df)
    st.success(f"✅ {len(bank_df)} 件の出金データを読み込みました。")

    # B. システムデータの読込 (強化版)
    try:
        # get_all_records() は結合セル等でエラーになりやすいので get_all_values() を使う
        raw_data = sh.worksheet(selected_sheet).get_all_values()
        
        if len(raw_data) < 2:
            st.error(f"❌ タブ '{selected_sheet}' にデータが見当たりません。")
            st.stop()
            
        # 1行目をヘッダーとしてDataFrame化
        headers = raw_data[0]
        sys_df = pd.DataFrame(raw_data[1:], columns=headers)
        
        # 列名検索 (部分一致)
        status_col = next((c for c in sys_df.columns if "Status" in c), None)
        vendor_col = next((c for c in sys_df.columns if "Vendor" in c), None)
        fb_col = next((c for c in sys_df.columns if "FB" in c and "Amount" in c), None)
        
        if not all([status_col, vendor_col, fb_col]):
            st.error(f"❌ 列が見つかりません。必要な列: Status, Vendor, FB Amount。 見つかった列: {list(sys_df.columns)}")
            st.stop()
            
        paid_invoices = sys_df[sys_df[status_col] == "Paid"].copy()
        
        # 金額列を数値化 (カンマ除去など)
        def clean_currency(x):
            try:
                if isinstance(x, str):
                    return int(float(x.replace(',', '').replace('¥', '').strip()))
                return int(x)
            except:
                return 0
                
        paid_invoices["CleanAmount"] = paid_invoices[fb_col].apply(clean_currency)
        paid_invoices = paid_invoices.rename(columns={vendor_col: "Vendor Name"})
        
    except Exception as e:
        st.error(f"Sheet Error: {e}")
        st.stop()

    # C. マッチング処理
    mapping = load_mapping(sheet_url)
    matches = []
    unmatched = []
    
    for _, row in bank_df.iterrows():
        bank_desc = row['Bank Description']
        amount = row['Amount']
        
        # 1. マッピング確認
        matched_name = None
        # 「銀行の明細名」の中に「マッピング表のキーワード」が含まれているか？
        for key, val in mapping.items():
            if key in bank_desc: 
                matched_name = val
                break
        
        # 2. 請求書データとの照合
        status = "❌ Missing"
        if matched_name:
            # Vendor名 と 金額 で検索
            sys_match = paid_invoices[
                (paid_invoices["Vendor Name"] == matched_name) & 
                (paid_invoices["CleanAmount"] == amount)
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

    # D. 結果表示
    st.divider()
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader(f"✅ マッチ ({len(matches)})")
        st.dataframe(matches, use_container_width=True)

    with c2:
        st.subheader(f"❌ 未マッチ ({len(unmatched)})")
        st.dataframe(unmatched, use_container_width=True)
        
        if unmatched:
            st.write("---")
            st.write("### 📝 マッピングに追加")
            options = [u['Bank Description'] for u in unmatched if u['Mapped Vendor'] == "Unknown"]
            if options:
                selected_desc = st.selectbox("銀行明細を選択", options)
                new_alias = st.text_input("キーワード (例: 'ヘ゛リ－' )", help="このキーワードが含まれていたらマッチさせます")
                
                if st.button("マッピング表に保存"):
                    if new_alias:
                        add_mapping(sheet_url, new_alias, "") 
                        st.success(f"'{new_alias}' を追加しました！スプレッドシートのB列に英語名を入力してください。")
                        time.sleep(3)
                        st.rerun()
