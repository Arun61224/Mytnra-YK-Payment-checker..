import streamlit as st
import pandas as pd
import io
import zipfile

# --- ZIP हैंडलिंग फ़ंक्शन्स (नो चेंज) ---

def handle_packed_rto_zip_upload(zip_file):
    """Packed/RT/RTO ZIP फ़ाइल को एक्सट्रैक्ट करता है।"""
    if zip_file is None:
        return None, None, None, False
    csv_data = {}
    required_files = ["Packed.csv", "RT..csv", "RTO.csv"]
    st.info("Extracting files from the Data ZIP archive...")
    try:
        with zipfile.ZipFile(zip_file, 'r') as z:
            for file_name in required_files:
                try:
                    file_content = z.read(file_name).decode('utf-8', errors='ignore')
                    csv_data[file_name] = io.StringIO(file_content)
                except KeyError:
                    st.error(f"Required file **{file_name}** not found in the Data ZIP archive.")
                    return None, None, None, False
        return csv_data.get("Packed.csv"), csv_data.get("RT..csv"), csv_data.get("RTO.csv"), True
    except Exception as e:
        st.error(f"An error occurred during Data ZIP file extraction: {e}")
        return None, None, None, False

def handle_settlement_zip(zip_file, process_name):
    """Settlement ZIP फ़ाइल को एक्सट्रैक्ट करता है और सभी CSV फ़ाइलों को list of StringIO objects के रूप में वापस करता है।"""
    if zip_file is None:
        return None
    extracted_csv_objects = []
    st.info(f"Extracting files from the {process_name} ZIP archive...")
    try:
        with zipfile.ZipFile(zip_file, 'r') as z:
            for file_name in z.namelist():
                if file_name.lower().endswith('.csv') and not file_name.startswith('__'):
                    st.write(f"Found CSV: {file_name}")
                    file_content = z.read(file_name).decode('utf-8', errors='ignore')
                    extracted_csv_objects.append(io.StringIO(file_content))
            if not extracted_csv_objects:
                st.error(f"No CSV files found inside the {process_name} ZIP.")
                return None
            return extracted_csv_objects
    except Exception as e:
        st.error(f"An error occurred during {process_name} ZIP file extraction: {e}")
        return None

# --- SKU Merger (नो चेंज) ---

def process_sku_merger(packed_file_obj, rt_file_obj, rto_file_obj, seller_listings_file):
    # ... (Logic remains the same as previous response) ...
    st.subheader("1. SKU Code Merger Process")
    
    try:
        seller_df = pd.read_csv(seller_listings_file, engine='python')
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


# --- फ़ंक्शन: Settlement Pivot Processor (Prepaid and Postpaid दोनों के लिए उपयोग होगा) ---

def process_settlement_pivot(settlement_csv_objects, pivot_type):
    """
    Settlement data को Process करता है और Order ID के आधार पर Settled Amount का Pivot Table बनाता है।
    'pivot_type' (Prepaid या Postpaid) के आधार पर Subheader सेट करता है।
    """
    st.subheader(f"2. {pivot_type} Settlement Pivot Process")
    
    if not settlement_csv_objects:
        return None

    all_dfs = []
    
    # अपेक्षित कॉलम नाम (Normalization के लिए)
    TARGET_COL_ID = 'order_release_id'
    TARGET_COL_AMOUNT = 'Settled_Amount'
    
    # उन नामों को जिन्हें हमें मैच करना है (lowercase, underscores removed)
    MATCH_ID = TARGET_COL_ID.lower().replace('_', '')
    MATCH_AMOUNT = TARGET_COL_AMOUNT.lower().replace('_', '')

    for i, file_obj in enumerate(settlement_csv_objects):
        file_name = f"{pivot_type}_Settlement_File_{i+1}"
        try:
            df = pd.read_csv(file_obj)
            
            # कॉलम नामों को Normalize करें: Lowercase + Spaces/Quotes हटाएँ
            normalized_cols = {col: col.strip().replace('"', '').lower().replace('_', '') for col in df.columns}
            
            # फ़ाइल के कॉलम नामों में TARGET कॉलम को खोजें
            found_id_name = None
            found_amount_name = None

            for original_name, norm_name in normalized_cols.items():
                if norm_name == MATCH_ID:
                    found_id_name = original_name
                if norm_name == MATCH_AMOUNT:
                    found_amount_name = original_name
            
            if not found_id_name or not found_amount_name:
                st.error(f"File **{file_name}** is missing required columns. Expected '{TARGET_COL_ID}' and '{TARGET_COL_AMOUNT}'.")
                continue

            # केवल आवश्यक कॉलम चुनें
            df_subset = df[[found_id_name, found_amount_name]].copy()
            
            # कॉलम को अपेक्षित नाम दें ताकि Pivot Table सही से बन सके
            df_subset.rename(columns={
                found_id_name: TARGET_COL_ID, 
                found_amount_name: TARGET_COL_AMOUNT
            }, inplace=True)
            
            # Settled_Amount को numeric में बदलें 
            df_subset[TARGET_COL_AMOUNT] = pd.to_numeric(df_subset[TARGET_COL_AMOUNT], errors='coerce')
            
            all_dfs.append(df_subset)
            st.success(f"**{file_name}** read successfully.")
            
        except Exception as e:
            st.error(f"Error reading **{file_name}**: {e}")
            
    if not all_dfs:
        st.error(f"No {pivot_type} settlement file could be successfully processed.")
        return None
        
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Pivot Table बनाएँ
    pivot_table = combined_df.groupby(TARGET_COL_ID)[TARGET_COL_AMOUNT].sum().reset_index()
    pivot_table.rename(columns={TARGET_COL_AMOUNT: 'Total_Settled_Amount'}, inplace=True)
    
    st.success(f"{pivot_type} Pivot Table created successfully.")
    return pivot_table


# --- फ़ंक्शन: मल्टी-शीट Excel डाउनलोडर (अपडेटेड) ---

def convert_dfs_to_excel(df_packed, df_rt, df_rto, df_prepaid_pivot, df_postpaid_pivot):
    """
    पांच DataFrames को एक Excel फ़ाइल की अलग-अलग शीट्स में लिखता है।
    """
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        if df_packed is not None:
            df_packed.to_excel(writer, sheet_name='Packed', index=False) # Sheet 1
        if df_rt is not None:
            df_rt.to_excel(writer, sheet_name='RT', index=False)         # Sheet 2
        if df_rto is not None:
            df_rto.to_excel(writer, sheet_name='RTO', index=False)       # Sheet 3
        if df_prepaid_pivot is not None:
            df_prepaid_pivot.to_excel(writer, sheet_name='Prepaid_Pivot', index=False) # Sheet 4
        if df_postpaid_pivot is not None:
            df_postpaid_pivot.to_excel(writer, sheet_name='Postpaid_Pivot', index=False) # Sheet 5 (NEW)
    
    processed_excel_data = output.getvalue()
    return processed_excel_data


# --- Streamlit डैशबोर्ड लेआउट (अपडेटेड) ---

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
    st.sidebar.header("📁 1. Files for SKU Merger")
    
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
    st.sidebar.header("🧾 2. Prepaid Settlement Files")
    
    prepaid_zip_file = st.sidebar.file_uploader(
        "Upload **Prepaid Settlement CSVs as a single ZIP**", 
        type=['zip'],
        key="prepaid_zip"
    )
    
    st.sidebar.markdown("---")
    st.sidebar.header("💳 3. Postpaid Settlement Files (New)")
    
    postpaid_zip_file = st.sidebar.file_uploader(
        "Upload **Postpaid Settlement CSVs as a single ZIP**", 
        type=['zip'],
        key="postpaid_zip"
    )
    
    st.markdown("---")
    
    df_prepaid_pivot = None 
    df_postpaid_pivot = None
    packed_df_merged, rt_df_merged, rto_df_merged = None, None, None
    
    if st.sidebar.button("🚀 Start All Processing"):
        
        # ----------------------------------------------------
        #             Settlement Pivot Execution
        # ----------------------------------------------------
        
        # --- Prepaid ---
        st.header("--- Prepaid Settlement Pivot Results ---")
        if prepaid_zip_file:
            with st.spinner("Processing Prepaid settlement files and creating Pivot Table..."):
                settlement_csv_objects = handle_settlement_zip(prepaid_zip_file, "Prepaid")
                if settlement_csv_objects:
                    df_prepaid_pivot = process_settlement_pivot(settlement_csv_objects, "Prepaid")
                else:
                    st.error("Prepaid Settlement Pivot: ZIP file extraction failed.")
        else:
            st.warning("Skipping Prepaid Settlement Pivot: No Prepaid ZIP file uploaded.")

        # --- Postpaid (NEW) ---
        st.header("--- Postpaid Settlement Pivot Results ---")
        if postpaid_zip_file:
            with st.spinner("Processing Postpaid settlement files and creating Pivot Table..."):
                settlement_csv_objects = handle_settlement_zip(postpaid_zip_file, "Postpaid")
                if settlement_csv_objects:
                    df_postpaid_pivot = process_settlement_pivot(settlement_csv_objects, "Postpaid")
                else:
                    st.error("Postpaid Settlement Pivot: ZIP file extraction failed.")
        else:
            st.warning("Skipping Postpaid Settlement Pivot: No Postpaid ZIP file uploaded.")
        
        
        # ----------------------------------------------------
        #                  SKU Merger Execution
        # ----------------------------------------------------
        st.header("--- SKU Code Merger Results ---")
        if seller_listings_file is None or data_zip_file is None:
            st.warning("Skipping SKU Merger: Required files not uploaded.")
            packed_df_merged, rt_df_merged, rto_df_merged = None, None, None
        else:
            packed_obj, rt_obj, rto_obj, success = handle_packed_rto_zip_upload(data_zip_file)
            
            if success:
                with st.spinner("Merging SKU data..."):
                    packed_df_merged, rt_df_merged, rto_df_merged = process_sku_merger(
                        packed_obj, rt_obj, rto_obj, seller_listings_file
                    )
            
        
        # ----------------------------------------------------
        #             Final Excel Generation
        # ----------------------------------------------------
        st.header("--- 💾 Final Excel Download ---")
        
        if packed_df_merged is not None or rt_df_merged is not None or rto_df_merged is not None or df_prepaid_pivot is not None or df_postpaid_pivot is not None:
            with st.spinner("Generating Multi-Sheet Excel Workbook (Packed, RT, RTO, Prepaid_Pivot, Postpaid_Pivot)..."):
                excel_data = convert_dfs_to_excel(packed_df_merged, rt_df_merged, rto_df_merged, df_prepaid_pivot, df_postpaid_pivot)
            
            st.success("✅ Multi-sheet Excel file is ready. It contains: Packed, RT, RTO, Prepaid_Pivot (Sheet 4), and Postpaid_Pivot (Sheet 5).")
            
            st.download_button(
                label="⬇️ Download Complete Merged Data (Excel)",
                data=excel_data,
                file_name='Merged_SKU_Settlement_Report_Complete.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                key='download_excel'
            )
            st.markdown("---")
            
            st.subheader("Preview of Postpaid Pivot (Sheet 5)")
            if df_postpaid_pivot is not None:
                 st.dataframe(df_postpaid_pivot.head(10))
            else:
                st.info("Postpaid Pivot data was not generated.")

        else:
            st.error("No data files could be processed successfully to generate the final Excel report.")


# Streamlit App को रन करें
if __name__ == "__main__":
    main()
