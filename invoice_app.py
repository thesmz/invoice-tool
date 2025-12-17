import streamlit as st
import pandas as pd
from google.cloud import documentai_v1 as documentai
from google.api_core.client_options import ClientOptions
import os
import re
from datetime import datetime
import io
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

# --- CONFIGURATION ---
st.set_page_config(page_title="Invoice Processor Pro", layout="wide", page_icon="📄")

# Initialize session state for persistence
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = []
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False

# --- HELPER FUNCTIONS (Ported from your script) ---
def clean_amount(amount_str):
    """Clean string to number"""
    if not amount_str: return 0.0
    # Remove everything except digits and dots
    cleaned = re.sub(r'[^\d.]', '', str(amount_str))
    try:
        val = float(cleaned)
        # Return int if it's a whole number (for display)
        return int(val) if val.is_integer() else val
    except:
        return 0.0

def extract_vendor_and_items(filename):
    """Ported Regex logic to guess Vendor and Items from filename"""
    vendor = ""
    items = ""
    name_without_ext = os.path.splitext(filename)[0]
    
    # 1. Extract Items (anything in parentheses)
    items_match = re.search(r'[（(]([^）)]+)[）)]', name_without_ext)
    if items_match:
        items = items_match.group(1).strip()
        name_for_vendor = re.sub(r'[（(][^）)]+[）)]', '', name_without_ext)
    else:
        name_for_vendor = name_without_ext
        
    # 2. Clean Vendor Name
    name_for_vendor = re.sub(r'^[〇○◯]?\d{6}\s*[-－]\s*', '', name_for_vendor) # Remove date
    name_for_vendor = re.sub(r'(未払金|買掛金)(計上済|補助なし計上済|[(（]補助なし[)）]計上済)?', '', name_for_vendor) # Remove status
    vendor = name_for_vendor.strip().strip('-_. ')
    
    if not vendor:
        vendor = name_without_ext
        
    return vendor, items

def process_document_ai(file_content, mime_type, project_id, loc, proc_id, credentials_file):
    # Authenticate using the uploaded JSON
    with open("temp_creds.json", "wb") as f:
        f.write(credentials_file.getbuffer())
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "temp_creds.json"

    opts = ClientOptions(api_endpoint=f"{loc}-documentai.googleapis.com")
    client = documentai.DocumentProcessorServiceClient(client_options=opts)
    name = client.processor_path(project_id, loc, proc_id)
    
    raw_document = documentai.RawDocument(content=file_content, mime_type=mime_type)
    request = documentai.ProcessRequest(name=name, raw_document=raw_document)
    result = client.process_document(request=request)
    return result.document

def create_excel_report(data_list):
    """Recreated exact Excel generation logic using openpyxl"""
    wb = openpyxl.Workbook()
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF")
    
    # --- Sheet 1: Summary ---
    ws = wb.active
    ws.title = "Invoice Summary"
    headers = ['Vendor Name', 'Items', 'FB Amount (Tax incld.)', 'Others Amount (Tax incld.)']
    ws.append(headers)
    
    # Style Header
    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_num)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal='center')

    # Add Data
    for item in data_list:
        fb_total = 0
        others_total = 0
        
        for amt in item['amounts']:
            val = float(amt['value'])
            cat = amt['category']
            
            if cat == 'FB Amount':
                fb_total += val
            elif cat == 'Others':
                others_total += val
            elif cat == 'Divide (50/50)':
                fb_total += val / 2
                others_total += val / 2
            # 'None' is ignored
            
        ws.append([
            item['vendor_name'],
            item['items_desc'],
            fb_total if fb_total > 0 else '',
            others_total if others_total > 0 else ''
        ])

    # Formatting
    ws.column_dimensions['A'].width = 25
    ws.column_dimensions['B'].width = 50
    ws.column_dimensions['C'].width = 25
    ws.column_dimensions['D'].width = 25
    
    for row in range(2, ws.max_row + 1):
        for col in [3, 4]: # C and D
            cell = ws.cell(row=row, column=col)
            cell.number_format = '#,##0'

    # Save to buffer
    output = io.BytesIO()
    wb.save(output)
    return output.getvalue()

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Configuration")
    creds_file = st.file_uploader("Credentials JSON", type="json", help="Upload your Google Cloud service account key")
    
    with st.expander("Advanced Settings"):
        project_id = st.text_input("Project ID", value="receipt-processor-479605")
        location = st.selectbox("Location", ["us", "eu"], index=0)
        processor_id = st.text_input("Processor ID", value="88cff36a297265dc")

# --- MAIN APP ---
st.title("📄 Invoice Processor Pro")

# 1. UPLOAD SECTION
uploaded_files = st.file_uploader("Upload Invoices (PDF/Image)", type=['pdf', 'png', 'jpg', 'jpeg', 'tif'], accept_multiple_files=True)

start_btn = st.button("🚀 Start Processing", type="primary", disabled=(not uploaded_files or not creds_file))

if start_btn:
    st.session_state.processed_data = [] # Clear old data
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for idx, f in enumerate(uploaded_files):
        status_text.text(f"Processing {f.name}...")
        
        # Regex Extraction
        vendor, items = extract_vendor_and_items(f.name)
        
        # Doc AI Extraction
        try:
            content = f.read()
            f.seek(0) # Reset pointer
            doc = process_document_ai(content, f.type, project_id, location, processor_id, creds_file)
            
            # Find totals
            extracted_amounts = []
            totals_by_page = {}
            
            # Group by page
            for entity in doc.entities:
                if entity.type_ == 'total_amount':
                    try:
                        page = entity.page_anchor.page_refs[0].page if entity.page_anchor.page_refs else 0
                    except:
                        page = 0
                    
                    if page not in totals_by_page: totals_by_page[page] = []
                    totals_by_page[page].append(clean_amount(entity.mention_text))
            
            # Max per page logic
            for page, amounts in totals_by_page.items():
                if amounts:
                    extracted_amounts.append({
                        "id": f"{f.name}_{page}", # Unique ID for streamlits keys
                        "page": page + 1,
                        "value": max(amounts),
                        "category": "FB Amount" # Default
                    })
            
            st.session_state.processed_data.append({
                "filename": f.name,
                "vendor_name": vendor,
                "items_desc": items,
                "amounts": extracted_amounts
            })
            
        except Exception as e:
            st.error(f"Error processing {f.name}: {e}")
            
        progress_bar.progress((idx + 1) / len(uploaded_files))
    
    status_text.text("✅ Processing Complete! Review your data below.")
    st.session_state.processing_complete = True

# 2. REVIEW SECTION
if st.session_state.processing_complete:
    st.divider()
    st.header("📋 Review & Edit")
    st.info("👇 Expand each invoice to edit Vendor, Items, or Categories (FB/Others/Divide).")

    # Iterate through invoices
    for i, invoice in enumerate(st.session_state.processed_data):
        with st.expander(f"📄 {invoice['vendor_name']} ({len(invoice['amounts'])} amounts)", expanded=(i==0)):
            col1, col2 = st.columns([1, 2])
            
            # Editable Header Fields
            new_vendor = col1.text_input("Vendor Name", value=invoice['vendor_name'], key=f"v_{i}")
            new_items = col2.text_area("Items Description", value=invoice['items_desc'], height=1, key=f"d_{i}")
            
            # Update state immediately
            st.session_state.processed_data[i]['vendor_name'] = new_vendor
            st.session_state.processed_data[i]['items_desc'] = new_items
            
            st.markdown("#### Amounts Detected")
            
            # Dynamic Amount List
            if not invoice['amounts']:
                st.warning("No amounts detected automatically.")
            
            # Existing Amounts
            amounts_to_remove = []
            for j, amount in enumerate(invoice['amounts']):
                c1, c2, c3, c4 = st.columns([1, 2, 2, 0.5])
                
                # Page Display
                c1.caption(f"Page {amount['page']}")
                
                # Editable Value
                new_val = c2.number_input("Amount (¥)", value=float(amount['value']), key=f"val_{i}_{j}")
                st.session_state.processed_data[i]['amounts'][j]['value'] = new_val
                
                # Category Selector
                cat_options = ["FB Amount", "Others", "Divide (50/50)", "None (Exclude)"]
                current_cat_idx = cat_options.index(amount['category']) if amount['category'] in cat_options else 0
                
                new_cat = c3.selectbox("Category", cat_options, index=current_cat_idx, key=f"cat_{i}_{j}", label_visibility="collapsed")
                st.session_state.processed_data[i]['amounts'][j]['category'] = new_cat
                
                # Delete Button
                if c4.button("🗑️", key=f"del_{i}_{j}"):
                    amounts_to_remove.append(j)

            # Remove deleted items
            if amounts_to_remove:
                for idx_to_rem in sorted(amounts_to_remove, reverse=True):
                    del st.session_state.processed_data[i]['amounts'][idx_to_rem]
                st.rerun()

            # Manual Add Button
            if st.button("➕ Add Manual Amount", key=f"add_{i}"):
                st.session_state.processed_data[i]['amounts'].append({
                    "id": f"manual_{datetime.now().timestamp()}",
                    "page": 1,
                    "value": 0.0,
                    "category": "FB Amount"
                })
                st.rerun()

    # 3. EXPORT SECTION
    st.divider()
    if st.session_state.processed_data:
        excel_data = create_excel_report(st.session_state.processed_data)
        
        st.download_button(
            label="📥 Download Final Excel Report",
            data=excel_data,
            file_name=f"invoice_report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary"
        )
