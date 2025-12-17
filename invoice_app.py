import time  # <--- Add this at the top with other imports
import streamlit as st
import pandas as pd
from google.cloud import documentai_v1 as documentai
from google.api_core.client_options import ClientOptions
import os
import re
from datetime import datetime
import io
import fitz  # PyMuPDF
from PIL import Image
import gspread
import json
import uuid
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
st.set_page_config(page_title="Invoice Processor Pro", layout="wide", page_icon="⚡")

# Initialize Session State
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = []
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False

# Unique ID to force the File Uploader to reset after saving
if 'uploader_key' not in st.session_state:
    st.session_state.uploader_key = str(uuid.uuid4())

# --- HELPER FUNCTIONS ---
def clean_amount(amount_str):
    if not amount_str: return 0.0
    cleaned = re.sub(r'[^\d.]', '', str(amount_str))
    try:
        val = float(cleaned)
        return int(val) if val.is_integer() else val
    except:
        return 0.0

def get_pdf_image(file_bytes, page_num=0):
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        if page_num >= len(doc): page_num = 0
        if page_num < 0: page_num = 0
        page = doc.load_page(page_num)
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        return img
    except:
        return None

def get_page_count(file_bytes):
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        return len(doc)
    except:
        return 1

def extract_vendor_and_items(filename):
    vendor = ""
    items = ""
    name_without_ext = os.path.splitext(filename)[0]
    
    items_match = re.search(r'[（(]([^）)]+)[）)]', name_without_ext)
    if items_match:
        items = items_match.group(1).strip()
        name_for_vendor = re.sub(r'[（(][^）)]+[）)]', '', name_without_ext)
    else:
        name_for_vendor = name_without_ext
        
    name_for_vendor = re.sub(r'^[〇○◯]?\d{6}\s*[-－]\s*', '', name_for_vendor)
    name_for_vendor = re.sub(r'(未払金|買掛金)(計上済|補助なし計上済|[(（]補助なし[)）]計上済)?', '', name_for_vendor)
    vendor = name_for_vendor.strip().strip('-_. ')
    if not vendor: vendor = name_without_ext
    return vendor, items

def process_document_ai(file_content, mime_type, project_id, loc, proc_id, creds_dict):
    opts = ClientOptions(api_endpoint=f"{loc}-documentai.googleapis.com")
    credentials = Credentials.from_service_account_info(creds_dict)
    client = documentai.DocumentProcessorServiceClient(client_options=opts, credentials=credentials)
    name = client.processor_path(project_id, loc, proc_id)
    raw_document = documentai.RawDocument(content=file_content, mime_type=mime_type)
    request = documentai.ProcessRequest(name=name, raw_document=raw_document)
    result = client.process_document(request=request)
    return result.document

def save_to_google_sheets(new_data, sheet_url, creds_dict):
    # Modern Authentication for GSpread
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    
    sheet = client.open_by_url(sheet_url).sheet1
    
    rows_to_add = []
    for item in new_data:
        fb_total = 0
        others_total = 0
        for amt in item['amounts']:
            val = float(amt['value'])
            cat = amt['category']
            if cat == 'FB Amount': fb_total += val
            elif cat == 'Others': others_total += val
            elif cat == 'Divide (50/50)':
                fb_total += val / 2
                others_total += val / 2
        
        if fb_total > 0 or others_total > 0:
            rows_to_add.append([
                item['vendor_name'],
                item['items_desc'],
                fb_total if fb_total > 0 else '',
                others_total if others_total > 0 else ''
            ])
            
    if rows_to_add:
        sheet.append_rows(rows_to_add)
        return len(rows_to_add)
    return 0

# --- CALLBACKS ---
def delete_amount_by_id(invoice_idx, amount_id):
    """Safely delete an amount using its Unique ID"""
    invoice = st.session_state.processed_data[invoice_idx]
    # Keep only amounts that DO NOT match the ID to be deleted
    invoice['amounts'] = [amt for amt in invoice['amounts'] if amt['id'] != amount_id]

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # 1. Credentials (Secrets First, File Second)
    creds_dict = None
    if "gcp_service_account" in st.secrets:
        creds_dict = dict(st.secrets["gcp_service_account"])
        st.success("🔑 Auto-logged in via Secrets")
    else:
        creds_file = st.file_uploader("Credentials JSON", type="json")
        if creds_file:
            creds_dict = json.load(creds_file)

    st.markdown("---")
    
    # 2. Database URL
    sheet_url = st.text_input("Google Sheet URL", placeholder="https://docs.google.com/spreadsheets/d/...")
    
    with st.expander("Doc AI Settings"):
        project_id = st.text_input("Project ID", value="receipt-processor-479605")
        location = st.selectbox("Location", ["us", "eu"], index=0)
        processor_id = st.text_input("Processor ID", value="88cff36a297265dc")

# --- MAIN APP ---
st.title("⚡ Invoice Processor Pro")

if not creds_dict or not sheet_url:
    st.info("👋 Please upload your `credentials.json` (or set Secrets) and provide a Google Sheet URL.")
    st.stop()

# 1. PROCESS SECTION
uploaded_files = st.file_uploader(
    "1. Upload New Invoices", 
    type=['pdf'], 
    accept_multiple_files=True,
    key=st.session_state.uploader_key  # Binds uploader to a dynamic key for resetting
)

start_btn = st.button("🚀 Process Batch", type="primary", disabled=(not uploaded_files))

if start_btn:
    st.session_state.processed_data = [] 
    progress_bar = st.progress(0)
    
    for idx, f in enumerate(uploaded_files):
        file_bytes = f.read()
        f.seek(0)
        total_pages = get_page_count(file_bytes)
        f.seek(0)
        
        vendor, items = extract_vendor_and_items(f.name)
        
        try:
            doc = process_document_ai(file_bytes, f.type, project_id, location, processor_id, creds_dict)
            
            extracted_amounts = []
            totals_by_page = {}
            for entity in doc.entities:
                if entity.type_ == 'total_amount':
                    page = entity.page_anchor.page_refs[0].page if entity.page_anchor.page_refs else 0
                    if page not in totals_by_page: totals_by_page[page] = []
                    totals_by_page[page].append(clean_amount(entity.mention_text))
            
            for page, amounts in totals_by_page.items():
                if amounts:
                    extracted_amounts.append({
                        "id": str(uuid.uuid4()),  # UNIQUE ID ASSIGNED HERE
                        "page": page + 1,
                        "value": max(amounts),
                        "category": "FB Amount"
                    })
            
            st.session_state.processed_data.append({
                "filename": f.name,
                "file_bytes": file_bytes,
                "page_count": total_pages,
                "vendor_name": vendor,
                "items_desc": items,
                "amounts": extracted_amounts
            })
            
        except Exception as e:
            st.error(f"Error {f.name}: {e}")
        progress_bar.progress((idx + 1) / len(uploaded_files))
    
    st.session_state.processing_complete = True

# 2. REVIEW SECTION
if st.session_state.processing_complete:
    st.divider()
    st.subheader("2. Review & Edit")
    
    for i, invoice in enumerate(st.session_state.processed_data):
        with st.expander(f"📄 {invoice['vendor_name']}", expanded=(i==0)):
            col_preview, col_data = st.columns([1, 1.2])
            
            with col_preview:
                total_pgs = invoice.get('page_count', 1)
                default_page = 1
                if invoice['amounts']: default_page = invoice['amounts'][0]['page']
                if default_page > total_pgs: default_page = 1
                
                selected_page = st.number_input(f"Page ({total_pgs})", 1, total_pgs, default_page, key=f"pg_{i}")
                img = get_pdf_image(invoice['file_bytes'], selected_page - 1)
                if img: st.image(img, use_container_width=True)

            with col_data:
                new_vendor = st.text_input("Vendor", value=invoice['vendor_name'], key=f"v_{i}")
                new_items = st.text_area("Items", value=invoice['items_desc'], height=1, key=f"d_{i}")
                st.session_state.processed_data[i]['vendor_name'] = new_vendor
                st.session_state.processed_data[i]['items_desc'] = new_items
                
                st.markdown("**Amounts**")
                
                for amount in invoice['amounts']:
                    c1, c2, c3 = st.columns([2, 3, 1])
                    u_id = amount['id'] # Grab the unique ID
                    
                    # Update Value
                    new_val = c1.number_input("¥", value=float(amount['value']), key=f"val_{u_id}")
                    for amt in st.session_state.processed_data[i]['amounts']:
                        if amt['id'] == u_id: amt['value'] = new_val
                    
                    # Update Category
                    cat_opts = ["FB Amount", "Others", "Divide (50/50)", "None"]
                    curr_idx = cat_opts.index(amount['category']) if amount['category'] in cat_opts else 0
                    new_cat = c2.selectbox("Cat", cat_opts, index=curr_idx, key=f"cat_{u_id}", label_visibility="collapsed")
                    for amt in st.session_state.processed_data[i]['amounts']:
                        if amt['id'] == u_id: amt['category'] = new_cat
                    
                    # DELETE BUTTON (Uses Callback)
                    c3.button(
                        "🗑️", 
                        key=f"del_{u_id}", 
                        on_click=delete_amount_by_id, 
                        args=(i, u_id)
                    )
                
                if st.button("➕ Add Amount", key=f"add_{i}"):
                    st.session_state.processed_data[i]['amounts'].append({
                        "id": str(uuid.uuid4()), # Assign ID for manual entry
                        "page": selected_page, 
                        "value": 0.0, 
                        "category": "FB Amount"
                    })
                    st.rerun()

    # 3. SAVE SECTION
    st.divider()
    st.subheader("3. Save to Database")
    
    if st.button("☁️ Save to Google Sheets", type="primary"):
        try:
            with st.spinner("Saving to Google Sheets..."):
                count = save_to_google_sheets(st.session_state.processed_data, sheet_url, creds_dict)
            
            # Show Success Message
            st.success(f"✅ SUCCESS! Saved {count} invoices to the Master Sheet.")
            st.balloons()
            
            # Wait 3 seconds so you can actually read the message
            time.sleep(3)
            
            # --- RESET LOGIC ---
            st.session_state.processed_data = []
            st.session_state.processing_complete = False
            
            # Change key to clear the file uploader
            st.session_state.uploader_key = str(uuid.uuid4())
            
            # Reload page
            st.rerun()
            
        except Exception as e:
            st.error(f"Failed to save: {e}")
