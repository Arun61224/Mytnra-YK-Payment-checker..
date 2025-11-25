import streamlit as st
import pandas as pd
import io
import zipfile
import os # zipfile extraction के लिए

# --- फ़ंक्शन: ZIP फ़ाइल हैंडलिंग (नो चेंज) ---
def handle_zip_upload(zip_file):
    if zip_file is None:
        return None, None, None, False

    csv_data = {}
    required_files = ["Packed.csv", "RT..csv", "RTO.csv"]
    
    st.info("Extracting files from the Data ZIP archive...")

    try:
        with zipfile.ZipFile(zip_file, 'r') as z:
            for file_name in required_files:
                # ZIP फ़ाइल से content पढ़ने के लिए .read() का उपयोग करें
                try:
                    file_content = z.read(file_name).decode('utf-8')
                    csv_data[file_name] = io.StringIO(file_content)
                except KeyError:
                    st.error(f"Required file **{file_name}** not found in the Data ZIP archive. Please check the file name inside the ZIP.")
                    return None, None, None, False
        
        return csv_data.get("Packed.csv"), csv_data.get("RT..csv"), csv_data.get("RTO.csv"), True
    
    except zipfile.BadZipFile:
        st.error("Invalid Data ZIP file uploaded. Please upload a valid .zip archive.")
        return None, None, None, False
    except Exception as e:
        st.error(f"An error occurred during Data ZIP file extraction: {e}")
        return None, None, None, False

# --- फ़ंक्शन: SKU Merger प्रोसेसिंग (नो चेंज) ---
def process_sku_merger(packed_file_obj, rt_file_obj, rto_file_obj, seller_listings_file):
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

# --- फ़ंक्शन: Prepaid Settlement Pivot (नो चेंज) ---
def process_settlement_data(settlement_files):
    st.subheader("2. Prepaid Settlement Pivot Process")
    
    if not settlement_files:
        return None

    all_dfs = []
    
    for uploaded_file in settlement_files:
        try:
            df = pd.read_csv(uploaded_file)
            required_cols = ['Order_released_ID', 'Settled_amount']
            df.columns = df.columns.str.strip().str.replace('"', '')
            
            if not all(col in df.columns for col in required_cols):
                st.warning(f"File **{uploaded_file.name}** is missing required columns ({', '.join(required_cols)}). Skipping.")
                continue

            df_subset = df[required_cols].copy()
            df_subset['Settled_amount'] = pd.to_numeric(df_subset['Settled_amount'], errors='coerce')
            
            all_dfs.append(df_subset)
            st.success(f"**{uploaded_file.name}** read successfully.")
            
        except Exception as e:
            st.error(f"Error reading **{uploaded_file.name}**: {e}")
            
    if not all_dfs:
        st.error("No settlement file could be successfully processed.")
        return None
        
    combined_df = pd.concat(all_dfs, ignore_index=True)
    pivot_table = combined_df.groupby('Order_released_ID')['Settled_amount'].sum().reset_index()
    pivot_table.rename(columns={'Settled_amount': 'Total_Settled_Amount'}, inplace=True)
    
    st.success("Pivot Table created successfully.")
    return pivot_table


# --- फ़ंक्शन: मल्टी-शीट Excel डाउनलोडर (अपडेटेड) ---
def convert_dfs_to_excel(df_packed, df_rt, df_rto, df_pivot):
    """
    चार DataFrames को एक Excel फ़ाइल की अलग-अलग शीट्स में लिखता है (Sheet 4 पर Pivot Table)।
    """
    output = io.BytesIO()
    
    # xlsxwriter engine का उपयोग करें
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        if df_packed is not None:
            df_packed.to_excel(writer, sheet_name='Packed', index=False) # Sheet 1
        if df_rt is not None:
            df_rt.to_excel(writer, sheet_name='RT', index=False)         # Sheet 2
        if df_rto is not None:
            df_rto.to_excel(writer, sheet_name='RTO', index=False)       # Sheet 3
        if df_pivot is not None:
            df_pivot.to_excel(writer, sheet_name='Settlement_Pivot', index=False) # Sheet 4 (NEW)
    
    processed_excel_data = output.getvalue()
    return processed_excel_data


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
    
    settlement_files = st.sidebar.file_uploader(
        "Upload **Prepaid Settlement CSVs** (Batch Upload)", 
        type=['csv'],
        accept_multiple_files=True,
        key="settlement"
    )
    
    st.markdown("---")
    
    # Initialize pivot_df outside the if block
    pivot_df = None 
    
    if st.sidebar.button("🚀 Start All Processing"):
        
        # ----------------------------------------------------
        #             Settlement Pivot Execution (Moved to top to get pivot_df)
        # ----------------------------------------------------
        st.header("--- Prepaid Settlement Pivot Results ---")
        if settlement_files:
            with st.spinner("Processing settlement files and creating Pivot Table..."):
                pivot_df = process_settlement_data(settlement_files)
        else:
            st.warning("Skipping Settlement Pivot: No settlement files uploaded.")
        
        
        # ----------------------------------------------------
        #                  SKU Merger Execution
        # ----------------------------------------------------
        st.header("--- SKU Code Merger Results ---")
        if seller_listings_file is None or data_zip_file is None:
            st.warning("Skipping SKU Merger: Required files not uploaded.")
            packed_df_merged, rt_df_merged, rto_df_merged = None, None, None
        else:
            packed_obj, rt_obj, rto_obj, success = handle_zip_upload(data_zip_file)
            
            if success:
                with st.spinner("Merging SKU data..."):
                    packed_df_merged, rt_df_merged, rto_df_merged = process_sku_merger(
                        packed_obj, rt_obj, rto_obj, seller_listings_file
                    )
            else:
                 packed_df_merged, rt_df_merged, rto_df_merged = None, None, None

        
        # ----------------------------------------------------
        #             Final Excel Generation
        # ----------------------------------------------------
        st.header("--- 💾 Final Excel Download ---")
        
        if packed_df_merged is not None or rt_df_merged is not None or rto_df_merged is not None or pivot_df is not None:
            with st.spinner("Generating Multi-Sheet Excel Workbook (Packed, RT, RTO, Settlement_Pivot)..."):
                excel_data = convert_dfs_to_excel(packed_df_merged, rt_df_merged, rto_df_merged, pivot_df)
            
            st.success("✅ Multi-sheet Excel file is ready. It contains: Packed, RT, RTO, and Settlement_Pivot (Sheet 4).")
            
            st.download_button(
                label="⬇️ Download Complete Merged Data (Excel)",
                data=excel_data,
                file_name='Merged_SKU_Settlement_Report.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                key='download_excel'
            )
            st.markdown("---")
            
            # प्रीव्यू
            st.subheader("Preview of Settlement Pivot (Sheet 4)")
            if pivot_df is not None:
                 st.dataframe(pivot_df.head(10))
            else:
                st.info("Settlement Pivot data was not generated.")

        else:
            st.error("No data files could be processed successfully to generate the final Excel report.")


# Streamlit App को रन करें
if __name__ == "__main__":
    main()
