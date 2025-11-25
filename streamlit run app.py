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
        return []
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
                st.warning(f"No CSV files found inside the {process_name} ZIP.")
                return []
            return extracted_csv_objects
    except Exception as e:
        st.error(f"An error occurred during {process_name} ZIP file extraction: {e}")
        return []

# --- फ़ंक्शन: Outstanding CSV हैंडलिंग (नया) ---
def handle_outstanding_csv(csv_file):
    """Outstanding CSV फ़ाइल को StringIO ऑब्जेक्ट में बदलता है।"""
    if csv_file is None:
        return []
    try:
        # फ़ाइल को सीधे StringIO ऑब्जेक्ट में बदलें
        file_content = csv_file.getvalue().decode('utf-8', errors='ignore')
        return [io.StringIO(file_content)]
    except Exception as e:
        st.error(f"An error occurred during Outstanding CSV file handling: {e}")
        return []

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

# --- फ़ंक्शन: कंबाइंड सेटलमेंट Pivot Processor (अपडेटेड) ---

def process_combined_settlement(all_csv_objects):
    """
    सभी Prepaid, Postpaid, और Outstanding data को पढ़ता है और एक Final Merged Pivot Table बनाता है।
    """
    st.subheader("2. Combined Settlement & Outstanding Pivot")
    
    if not all_csv_objects:
        st.warning("No settlement or outstanding files were uploaded or extracted successfully.")
        return None

    all_dfs = []
    
    # अपेक्षित कॉलम नाम (Normalization के लिए)
    TARGET_COL_ID = 'order_release_id'
    
    # दो संभावित राशि कॉलम - Settled_Amount (Prepaid/Postpaid) और Unsettled_Amount (Outstanding)
    TARGET_COL_AMOUNT_SETTLED = 'Settled_Amount'
    TARGET_COL_AMOUNT_UNSETTLED = 'Unsettled_Amount'
    
    # उन नामों को जिन्हें हमें मैच करना है (lowercase, underscores removed)
    MATCH_ID = TARGET_COL_ID.lower().replace('_', '')
    MATCH_SETTLED = TARGET_COL_AMOUNT_SETTLED.lower().replace('_', '')
    MATCH_UNSETTLED = TARGET_COL_AMOUNT_UNSETTLED.lower().replace('_', '')


    for i, file_obj in enumerate(all_csv_objects):
        file_name = f"Combined_Payment_File_{i+1}"
        try:
            df = pd.read_csv(file_obj)
            
            # कॉलम नामों को Normalize करें: Lowercase + Spaces/Quotes हटाएँ
            normalized_cols = {col: col.strip().replace('"', '').lower().replace('_', '') for col in df.columns}
            
            found_id_name = None
            found_amount_name = None
            amount_type = None # Settled या Unsettled

            for original_name, norm_name in normalized_cols.items():
                if norm_name == MATCH_ID:
                    found_id_name = original_name
                # Settled_Amount को पहले खोजें
                if norm_name == MATCH_SETTLED:
                    found_amount_name = original_name
                    amount_type = 'Settled'
                # अगर Settled_Amount नहीं मिला, तो Unsettled_Amount खोजें
                elif norm_name == MATCH_UNSETTLED:
                    found_amount_name = original_name
                    amount_type = 'Unsettled'
            
            if not found_id_name or not found_amount_name:
                st.error(f"File **{file_name}** is missing required ID or Amount columns. Skipping.")
                continue

            # केवल आवश्यक कॉलम चुनें
            df_subset = df[[found_id_name, found_amount_name]].copy()
            
            # राशि कॉलम को एक स्टैंडर्ड नाम दें (Total_Amount)
            df_subset.rename(columns={
                found_id_name: TARGET_COL_ID, 
                found_amount_name: 'Total_Amount'
            }, inplace=True)
            
            # Total_Amount को numeric में बदलें 
            df_subset['Total_Amount'] = pd.to_numeric(df_subset['Total_Amount'], errors='coerce')
            
            all_dfs.append(df_subset)
            st.success(f"**{file_name}** read successfully. Amount found: **{amount_type}**.")
            
        except Exception as e:
            st.error(f"Error reading **{file_name}**: {e}")
            
    if not all_dfs:
        st.error("No combined payment data could be processed successfully.")
        return None
        
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Final Pivot Table बनाएँ (सभी डेटा को मिलाकर)
    pivot_table = combined_df.groupby(TARGET_COL_ID)['Total_Amount'].sum().reset_index()
    pivot_table.rename(columns={'Total_Amount': 'Total_Settled_Outstanding_Amount'}, inplace=True)
    
    st.success("Final Merged Payment & Outstanding Pivot Table created successfully.")
    return pivot_table


# --- फ़ंक्शन: मल्टी-शीट Excel डाउनलोडर (नो चेंज) ---

def convert_dfs_to_excel(df_packed, df_rt, df_rto, df_merged_pivot):
    """
    चार DataFrames को एक Excel फ़ाइल की अलग-अलग शीट्स में लिखता है (Sheet 4 पर Merged Pivot Table)।
    """
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        if df_packed is not None:
            df_packed.to_excel(writer, sheet_name='Packed', index=False) # Sheet 1
        if df_rt is not None:
            df_rt.to_excel(writer, sheet_name='RT', index=False)         # Sheet 2
        if df_rto is not None:
            df_rto.to_excel(writer, sheet_name='RTO', index=False)       # Sheet 3
        if df_merged_pivot is not None:
            df_merged_pivot.to_excel(writer, sheet_name='Merged_Payment_Pivot', index=False) # Sheet 4 (Final Pivot)
    
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
    st.sidebar.header("🧾 2. Payment Files (Settled & Outstanding)")
    
    prepaid_zip_file = st.sidebar.file_uploader(
        "Upload **Prepaid Settlement CSVs as a single ZIP**", 
        type=['zip'],
        key="prepaid_zip"
    )
    
    postpaid_zip_file = st.sidebar.file_uploader(
        "Upload **Postpaid Settlement CSVs as a single ZIP**", 
        type=['zip'],
        key="postpaid_zip"
    )
    
    # नया CSV अपलोडर
    outstanding_csv_file = st.sidebar.file_uploader(
        "Upload **Outstanding Payment CSV**", 
        type=['csv'],
        key="outstanding_csv"
    )
    
    st.markdown("---")
    
    df_merged_pivot = None 
    packed_df_merged, rt_df_merged, rto_df_merged = None, None, None
    
    if st.sidebar.button("🚀 Start All Processing"):
        
        # ----------------------------------------------------
        #             Combined Payment Execution
        # ----------------------------------------------------
        
        st.header("--- Combined Payment & Outstanding Pivot Results ---")
        
        # सभी sources से CSV objects इकट्ठा करें
        prepaid_objects = handle_settlement_zip(prepaid_zip_file, "Prepaid")
        postpaid_objects = handle_settlement_zip(postpaid_zip_file, "Postpaid")
        outstanding_objects = handle_outstanding_csv(outstanding_csv_file) # नया
        
        all_csv_objects = prepaid_objects + postpaid_objects + outstanding_objects
        
        if all_csv_objects:
            with st.spinner("Processing all payment files and creating Merged Pivot Table..."):
                df_merged_pivot = process_combined_settlement(all_csv_objects)
        else:
            st.warning("Skipping Combined Pivot: No payment files were uploaded successfully.")

        
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
        
        if packed_df_merged is not None or rt_df_merged is not None or rto_df_merged is not None or df_merged_pivot is not None:
            with st.spinner("Generating Multi-Sheet Excel Workbook (Packed, RT, RTO, Merged_Payment_Pivot)..."):
                excel_data = convert_dfs_to_excel(packed_df_merged, rt_df_merged, rto_df_merged, df_merged_pivot)
            
            st.success("✅ Multi-sheet Excel file is ready. It contains: Packed (Sheet 1), RT (Sheet 2), RTO (Sheet 3), and Merged_Payment_Pivot (Sheet 4).")
            
            st.download_button(
                label="⬇️ Download Complete Merged Data (Excel)",
                data=excel_data,
                file_name='Merged_SKU_Settlement_Outstanding_Report_Final.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                key='download_excel'
            )
            st.markdown("---")
            
            st.subheader("Preview of Merged Payment & Outstanding Pivot (Sheet 4)")
            if df_merged_pivot is not None:
                 st.dataframe(df_merged_pivot.head(10))
            else:
                st.info("Merged Payment & Outstanding Pivot data was not generated.")

        else:
            st.error("No data files could be processed successfully to generate the final Excel report.")


# Streamlit App को रन करें
if __name__ == "__main__":
    main()
