import streamlit as st
import pandas as pd
from google.cloud import documentai_v1 as documentai
from google.api_core.client_options import ClientOptions
import os
import re
from datetime import datetime
import io

# --- 1. SETTINGS & CONFIG ---
st.set_page_config(page_title="Invoice Processor", layout="wide")
st.title("📄 Online Invoice Processor")

# --- 2. HELPER FUNCTIONS (Reused from your script) ---
def clean_amount(amount_str):
    """Clean string to number"""
    if not amount_str: return 0.0
    cleaned = re.sub(r'[^\d.]', '', str(amount_str))
    try:
        return float(cleaned)
    except:
        return 0.0

def process_document_ai(file_content, mime_type, project_id, loc, proc_id, credentials_file):
    """Call Google Document AI"""
    # Create a temporary credentials file for the Google Library to read
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

# --- 3. SIDEBAR: CONFIGURATION ---
with st.sidebar:
    st.header("⚙️ Settings")
    creds_file = st.file_uploader("Upload 'credentials.json'", type="json")
    project_id = st.text_input("Project ID", value="receipt-processor-479605")
    location = st.selectbox("Location", ["us", "eu"], index=0)
    processor_id = st.text_input("Processor ID", value="88cff36a297265dc")

# --- 4. MAIN INTERFACE ---
uploaded_files = st.file_uploader("Drag and drop Invoices (PDF/Images)", 
                                  type=['pdf', 'png', 'jpg', 'jpeg'], 
                                  accept_multiple_files=True)

if st.button("🚀 Process Invoices", type="primary"):
    if not uploaded_files or not creds_file:
        st.error("Please upload both invoices and your credentials JSON file.")
    else:
        results = []
        progress_bar = st.progress(0)
        
        for idx, uploaded_file in enumerate(uploaded_files):
            # Update progress
            progress_bar.progress((idx + 1) / len(uploaded_files))
            
            try:
                # 1. Process with Google
                content = uploaded_file.read()
                doc = process_document_ai(content, uploaded_file.type, project_id, location, processor_id, creds_file)
                
                # 2. Extract Data (Simplified logic from your script)
                totals = []
                for entity in doc.entities:
                    if entity.type_ == 'total_amount':
                        val = clean_amount(entity.mention_text)
                        totals.append(val)
                
                # Pick biggest total if multiple found
                final_amount = max(totals) if totals else 0.0
                
                # 3. Append to results
                results.append({
                    "Filename": uploaded_file.name,
                    "Vendor": uploaded_file.name.split('.')[0], # Simplified vendor logic
                    "Amount": final_amount,
                    "Status": "Success"
                })
                
            except Exception as e:
                results.append({
                    "Filename": uploaded_file.name,
                    "Vendor": "Error",
                    "Amount": 0.0,
                    "Status": f"Error: {str(e)}"
                })

        st.success("Processing Complete!")
        
        # --- 5. REVIEW & EDIT (Replaces your complex Tkinter review window) ---
        st.subheader("📋 Review Results")
        st.caption("You can edit the values in the table below before downloading.")
        
        # Create a DataFrame
        df = pd.DataFrame(results)
        
        # Editable Data Table
        edited_df = st.data_editor(df, num_rows="dynamic")
        
        # --- 6. EXPORT ---
        # Convert DataFrame to Excel in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            edited_df.to_excel(writer, index=False, sheet_name='Invoices')
        
        st.download_button(
            label="📥 Download Excel Report",
            data=output.getvalue(),
            file_name=f"invoice_report_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.ms-excel"
        )