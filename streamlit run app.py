import streamlit as st
import pandas as pd
import io
import zipfile

# --- फ़ंक्शन: ZIP फ़ाइल हैंडलिंग (पुरानी) ---
def handle_zip_upload(zip_file):
    """
    ZIP फ़ाइल को एक्सट्रैक्ट करता है और Packed, RT.., RTO.csv को StringIO ऑब्जेक्ट के रूप में वापस करता है।
    """
    if zip_file is None:
        return None, None, None, False

    csv_data = {}
    required_files = ["Packed.csv", "RT..csv", "RTO.csv"]
    
    st.info("Extracting files from the Data ZIP archive...")

    try:
        with zipfile.ZipFile(zip_file, 'r') as z:
            for file_name in required_files:
                if file_name in z.namelist():
                    file_content = z.read(file_name).decode('utf-8')
                    csv_data[file_name] = io.StringIO(file_content)
                else:
                    st.error(f"Required file **{file_name}** not found in the Data ZIP archive.")
                    return None, None, None, False
        
        return csv_data.get("Packed.csv"), csv_data.get("RT..csv"), csv_data.get("RTO.csv"), True
    
    except zipfile.BadZipFile:
        st.error("Invalid Data ZIP file uploaded. Please upload a valid .zip archive.")
        return None, None, None, False
    except Exception as e:
        st.error(f"An error occurred during Data ZIP file extraction: {e}")
        return None, None, None, False

# --- फ़ंक्शन: SKU Merger प्रोसेसिंग (पुरानी) ---
def process_sku_merger(packed_file_obj, rt_file_obj, rto_file_obj, seller_listings_file):
    """
    Packed, RT, RTO में Seller Listings Report से SKU data को मर्ज करता है।
    """
    st.subheader("1. SKU Code Merger Process")
    
    # 1. Seller Listings File से मैपिंग डेटा निकालें
    try:
        seller_df = pd.read_csv(seller_listings_file, engine='python')
        
        # आवश्यक कॉलम को चुनें और कॉलम के नाम Normalize करें
        sku_map_df = seller_df[['sku id', 'sku code', 'seller sku code']].copy()
        sku_map_df.columns = sku_map_df.columns.str.strip().str.replace('"', '').str.replace(' ', '_')
        sku_map_df.rename(columns={'sku_id': 'sku_id', 'sku_code': 'sku_code', 'seller_sku_code': 'seller_sku_code'}, inplace=True)
        sku_map_df.drop_duplicates(subset=['sku_id'], inplace=True)
        sku_map_df['sku_id'] = sku_map_df['sku_id'].astype(str)
        
    except Exception as e:
        st.error(f"Seller Listings Report पढ़ने में त्रुटि या आवश्यक कॉलम नहीं मिले: {e}")
        return None, None, None

    file_list = [
        ("Packed.csv", packed_file_obj, 'packed_df'),
        ("RT..csv", rt_file_obj, 'rt_df'),
        ("RTO.csv", rto_file_obj, 'rto_df')
    ]
    
    processed_dfs = {}

    for file_name, file_obj, df_key in file_list:
        if file_obj is not None:
            try:
                df = pd.read_csv(file_obj)
                
                merge_column = None
                original_sku_id_name = None
                
                if 'sku_id' in df.columns:
                    merge_column = 'sku_id'
                    original_sku_id_name = 'sku_id'
                elif 'sku id' in df.columns:
                    original_sku_id_name = 'sku id'
                    df.rename(columns={'sku id': 'sku_id'}, inplace=True)
                    merge_column = 'sku_id'
                
                if merge_column is None:
                    st.warning(f"File **{file_name}** does not contain a suitable 'sku id' column. Data not merged.")
                    processed_dfs[df_key] = df
                    continue

                df[merge_column] = df[merge_column].astype(str)
                merged_df = pd.merge(df, sku_map_df, on=merge_column, how='left')
                
                merged_df['seller_sku_code'] = merged_df['seller_sku_code'].fillna('Not Found')
                merged_df['sku_code'] = merged_df['sku_code'].fillna('Not Found')

                sku_id_index = merged_df.columns.get_loc('sku_id')
                
                seller_sku_col = merged_df.pop('seller_sku_code')
                sku_code_col = merged_df.pop('sku_code')
                
                merged_df.insert(sku_id_index + 1, 'seller_sku_code', seller_sku_col)
                merged_df.insert(sku_id_index + 2, 'sku_code', sku_code_col)

                if original_sku_id_name == 'sku id':
                    merged_df.rename(columns={'sku_id': 'sku id'}, inplace=True)
                
                processed_dfs[df_key] = merged_df
                st.success(f"**{file_name}** successfully processed.")

            except Exception as e:
                st.error(f"Error reading or processing **{file_name}**: {e}")
                processed_dfs[df_key] = None
        else:
            processed_dfs[df_key] = None
    
    return processed_dfs.get('packed_df'), processed_dfs.get('rt_df'), processed_dfs.get('rto_df')

# --- फ़ंक्शन: मल्टी-शीट Excel डाउनलोडर (पुरानी) ---
def convert_dfs_to_excel(df_packed, df_rt, df_rto):
    """
    तीन DataFrames को एक Excel फ़ाइल की अलग-अलग शीट्स में लिखता है।
    """
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        if df_packed is not None:
            df_packed.to_excel(writer, sheet_name='Packed', index=False)
        if df_rt is not None:
            df_rt.to_excel(writer, sheet_name='RT', index=False)
        if df_rto is not None:
            df_rto.to_excel(writer, sheet_name='RTO', index=False)
    
    processed_excel_data = output.getvalue()
    return processed_excel_data


# --- फ़ंक्शन: Prepaid Settlement Pivot (नई) ---
def process_settlement_data(settlement_files):
    """
    बैच में अपलोड की गई सेटलमेंट फ़ाइलों को पढ़ता है और Order_released_ID के आधार पर Settled_amount का pivot table बनाता है।
    """
    st.subheader("2. Prepaid Settlement Pivot Process")
    
    if not settlement_files:
        st.warning("No settlement files uploaded to process.")
        return None

    all_dfs = []
    
    for uploaded_file in settlement_files:
        try:
            # CSV फ़ाइल को पढ़ें
            df = pd.read_csv(uploaded_file)
            
            # आवश्यक कॉलम की जाँच करें और उन्हें Normalize करें
            required_cols = ['Order_released_ID', 'Settled_amount']
            
            # कॉलम नामों को साफ करें ताकि वे मैच हो सकें
            df.columns = df.columns.str.strip().str.replace('"', '')
            
            # सुनिश्चित करें कि दोनों कॉलम मौजूद हैं
            if not all(col in df.columns for col in required_cols):
                st.warning(f"File **{uploaded_file.name}** is missing required columns ({', '.join(required_cols)}). Skipping.")
                continue

            # केवल आवश्यक कॉलम चुनें
            df_subset = df[required_cols].copy()
            
            # Settled_amount को numeric में बदलें (गलत formats को NaN में बदलें)
            df_subset['Settled_amount'] = pd.to_numeric(df_subset['Settled_amount'], errors='coerce')
            
            all_dfs.append(df_subset)
            st.success(f"**{uploaded_file.name}** read successfully.")
            
        except Exception as e:
            st.error(f"Error reading **{uploaded_file.name}**: {e}")
            
    if not all_dfs:
        st.error("No settlement file could be successfully processed.")
        return None
        
    # सभी DataFrames को Concatenate करें
    combined_df = pd.concat(all_dfs, ignore_index=True)
    st.info(f"Total {len(combined_df)} rows combined from all settlement files.")
    
    # Pivot Table बनाएँ: Order_released_ID के आधार पर Settled_amount का योग
    pivot_table = combined_df.groupby('Order_released_ID')['Settled_amount'].sum().reset_index()
    pivot_table.rename(columns={'Settled_amount': 'Total_Settled_Amount'}, inplace=True)
    
    st.success("Pivot Table created successfully.")
    return pivot_table


# --- Streamlit डैशबोर्ड लेआउट ---
def main():
    st.set_page_config(
        page_title="SKU & Settlement Data Processor",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🛍️ SKU & Settlement Data Processor")
    st.markdown("---")
    
    # ----------------------------------------------------
    #                  SIDEBAR UPLOADERS
    # ----------------------------------------------------
    st.sidebar.header("📁 Upload Files for SKU Merger")
    
    seller_listings_file = st.sidebar.file_uploader(
        "Upload **Seller Listings Report.csv** (Required)", 
        type=['csv'],
        key="seller"
    )
    data_zip_file = st.sidebar.file_uploader(
        "Upload **Packed, RT, RTO files as a ZIP**", 
        type=['zip'],
        key="data_zip"
    )
    
    st.sidebar.markdown("---")
    st.sidebar.header("🧾 Upload Prepaid Settlement Files")
    
    # बैच अपलोडर
    settlement_files = st.sidebar.file_uploader(
        "Upload **Prepaid Settlement CSVs** (Batch Upload)", 
        type=['csv'],
        accept_multiple_files=True, # बैच अपलोड के लिए
        key="settlement"
    )
    
    st.markdown("---")
    
    if st.sidebar.button("🚀 Start All Processing"):
        
        # ----------------------------------------------------
        #                  SKU Merger Execution
        # ----------------------------------------------------
        st.header("--- SKU Code Merger Results ---")
        if seller_listings_file is None or data_zip_file is None:
            st.warning("Skipping SKU Merger: Required files not uploaded.")
        else:
            packed_obj, rt_obj, rto_obj, success = handle_zip_upload(data_zip_file)
            
            if success:
                with st.spinner("Merging SKU data and generating Excel workbook..."):
                    packed_df_merged, rt_df_merged, rto_df_merged = process_sku_merger(
                        packed_obj, rt_obj, rto_obj, seller_listings_file
                    )

                if packed_df_merged is not None or rt_df_merged is not None or rto_df_merged is not None:
                    excel_data = convert_dfs_to_excel(packed_df_merged, rt_df_merged, rto_df_merged)
                    st.success("SKU Merger: Multi-sheet Excel file is ready.")
                    
                    st.download_button(
                        label="⬇️ Download Merged SKU Data (Excel)",
                        data=excel_data,
                        file_name='Merged_SKU_Report.xlsx',
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        key='download_excel'
                    )
                    st.markdown("---")
                else:
                    st.error("SKU Merger: No files could be processed successfully.")
            
        # ----------------------------------------------------
        #             Settlement Pivot Execution
        # ----------------------------------------------------
        st.header("--- Prepaid Settlement Pivot Results ---")
        if settlement_files:
            with st.spinner("Processing settlement files and creating Pivot Table..."):
                pivot_df = process_settlement_data(settlement_files)

            if pivot_df is not None:
                st.success("Pivot Table generated successfully!")
                st.dataframe(pivot_df)
                
                # Pivot Table CSV डाउनलोडर
                csv_pivot = pivot_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="⬇️ Download Settlement Pivot Table (CSV)",
                    data=csv_pivot,
                    file_name='Settlement_Pivot_Table.csv',
                    mime='text/csv',
                    key='download_pivot_csv'
                )
            else:
                st.error("Settlement Pivot: Could not generate pivot table.")
        else:
            st.warning("Skipping Settlement Pivot: No settlement files uploaded.")


# Streamlit App को रन करें
if __name__ == "__main__":
    main()
