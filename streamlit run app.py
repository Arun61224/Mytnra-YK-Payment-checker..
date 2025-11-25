import streamlit as st
import pandas as pd
import io
import zipfile # ZIP फ़ाइल को हैंडल करने के लिए

# --- फ़ंक्शन: ZIP फ़ाइल हैंडलिंग ---
def handle_zip_upload(zip_file):
    """
    ZIP फ़ाइल को एक्सट्रैक्ट करता है और Packed, RT.., RTO.csv को StringIO ऑब्जेक्ट के रूप में वापस करता है।
    """
    if zip_file is None:
        return None, None, None, False

    csv_data = {}
    required_files = ["Packed.csv", "RT..csv", "RTO.csv"]
    
    st.info("Extracting files from the ZIP archive...")

    try:
        with zipfile.ZipFile(zip_file, 'r') as z:
            for file_name in required_files:
                # ZIP में फ़ाइल का नाम केस-सेंसिटिव हो सकता है, इसलिए exact match ज़रूरी है
                if file_name in z.namelist():
                    # फ़ाइल की सामग्री को पढ़ें और utf-8 में डिकोड करें
                    file_content = z.read(file_name).decode('utf-8')
                    # StringIO का उपयोग करके इसे Pandas के लिए एक फ़ाइल ऑब्जेक्ट की तरह बनाएं
                    csv_data[file_name] = io.StringIO(file_content)
                else:
                    st.error(f"Required file **{file_name}** not found in the ZIP archive.")
                    return None, None, None, False
        
        # StringIO ऑब्जेक्ट्स को अपेक्षित क्रम में वापस करें
        return csv_data.get("Packed.csv"), csv_data.get("RT..csv"), csv_data.get("RTO.csv"), True
    
    except zipfile.BadZipFile:
        st.error("Invalid ZIP file uploaded. Please upload a valid .zip archive.")
        return None, None, None, False
    except Exception as e:
        st.error(f"An error occurred during ZIP file extraction: {e}")
        return None, None, None, False

# --- फ़ंक्शन: डेटा प्रोसेसिंग (कोर लॉजिक वही है) ---
def process_data(packed_file_obj, rt_file_obj, rto_file_obj, seller_listings_file):
    """
    अपलोड की गई फ़ाइलों को पढ़ता है, SKU ID के आधार पर SKU Code और Seller SKU Code को मर्ज करता है।
    """
    
    # 1. Seller Listings File से मैपिंग डेटा निकालें
    try:
        seller_df = pd.read_csv(seller_listings_file, engine='python')
        
        # आवश्यक कॉलम को चुनें और कॉलम के नाम Normalize करें
        sku_map_df = seller_df[['sku id', 'sku code', 'seller sku code']].copy()
        sku_map_df.columns = sku_map_df.columns.str.strip().str.replace('"', '').str.replace(' ', '_')
        sku_map_df.rename(columns={
            'sku_id': 'sku_id', 
            'sku_code': 'sku_code',
            'seller_sku_code': 'seller_sku_code'
        }, inplace=True)
        
        sku_map_df.drop_duplicates(subset=['sku_id'], inplace=True)
        sku_map_df['sku_id'] = sku_map_df['sku_id'].astype(str) # Data type normalization
        
    except Exception as e:
        st.error(f"Seller Listings Report पढ़ने में त्रुटि या आवश्यक कॉलम नहीं मिले: {e}")
        return None, None, None

    # डेटाफ़्रेम की सूची बनाएं
    file_list = [
        ("Packed.csv", packed_file_obj, 'packed_df'),
        ("RT..csv", rt_file_obj, 'rt_df'),
        ("RTO.csv", rto_file_obj, 'rto_df')
    ]
    
    processed_dfs = {}

    for file_name, file_obj, df_key in file_list:
        if file_obj is not None:
            st.info(f"Merging data for {file_name}...")
            try:
                # StringIO ऑब्जेक्ट से डेटा पढ़ें
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

                # 'sku_id' कॉलम को string में बदलें ताकि merging ठीक से हो
                df[merge_column] = df[merge_column].astype(str)
                
                # 'sku_id' के आधार पर मर्ज करें
                merged_df = pd.merge(df, sku_map_df, on=merge_column, how='left')
                
                # 'Not Found' से मिसिंग values भरें
                merged_df['seller_sku_code'] = merged_df['seller_sku_code'].fillna('Not Found')
                merged_df['sku_code'] = merged_df['sku_code'].fillna('Not Found')

                # कॉलम को 'sku_id' के आगे Insert करें
                sku_id_index = merged_df.columns.get_loc('sku_id')
                
                seller_sku_col = merged_df.pop('seller_sku_code')
                sku_code_col = merged_df.pop('sku_code')
                
                # 'seller_sku_code' को 'sku_id' के ठीक आगे Insert करें
                merged_df.insert(sku_id_index + 1, 'seller_sku_code', seller_sku_col)
                
                # 'sku_code' को 'seller_sku_code' के ठीक आगे Insert करें
                merged_df.insert(sku_id_index + 2, 'sku_code', sku_code_col)

                # यदि मूल 'sku id' कॉलम का नाम 'sku id' था, तो उसे वापस ठीक करें
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

# --- फ़ंक्शन: मल्टी-शीट Excel डाउनलोडर ---
def convert_dfs_to_excel(df_packed, df_rt, df_rto):
    """
    तीन DataFrames को एक Excel फ़ाइल की अलग-अलग शीट्स में लिखता है।
    """
    output = io.BytesIO()
    
    # Pandas ExcelWriter का उपयोग करके BytesIO ऑब्जेक्ट में लिखें
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        if df_packed is not None:
            df_packed.to_excel(writer, sheet_name='Packed', index=False)
        if df_rt is not None:
            df_rt.to_excel(writer, sheet_name='RT', index=False)
        if df_rto is not None:
            df_rto.to_excel(writer, sheet_name='RTO', index=False)
    
    # BytesIO से बाइट्स प्राप्त करें
    processed_excel_data = output.getvalue()
    return processed_excel_data

# --- Streamlit डैशबोर्ड लेआउट ---
def main():
    st.set_page_config(
        page_title="SKU Data Merger & Excel Generator",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🛍️ SKU Data Merger & Excel Generator")
    st.markdown("---")
    
    st.sidebar.header("📁 Upload Your Files")
    
    # फ़ाइल अपलोडर्स
    # 1. Seller Listings Report (CSV)
    seller_listings_file = st.sidebar.file_uploader(
        "Upload **Seller Listings Report.csv** (Required)", 
        type=['csv'],
        key="seller"
    )
    
    # 2. Packed, RT, RTO ZIP file
    data_zip_file = st.sidebar.file_uploader(
        "Upload **Packed, RT, RTO files as a ZIP** (e.g., Data.zip)", 
        type=['zip'],
        key="data_zip"
    )
    
    st.markdown("---")
    
    if st.sidebar.button("🚀 Start Processing & Generate Excel"):
        if seller_listings_file is None or data_zip_file is None:
            st.error("Please upload both the **Seller Listings Report.csv** and the **Data ZIP file** to start the process.")
        else:
            # 1. ZIP फ़ाइल को हैंडल करें
            packed_obj, rt_obj, rto_obj, success = handle_zip_upload(data_zip_file)
            
            if success:
                # 2. डेटा प्रोसेसिंग
                with st.spinner("Merging SKU data and generating Excel workbook... Please wait."):
                    packed_df_merged, rt_df_merged, rto_df_merged = process_data(
                        packed_obj, rt_obj, rto_obj, seller_listings_file
                    )

                st.header("✅ Processing Complete")
                
                # सुनिश्चित करें कि कम से कम एक फ़ाइल प्रोसेस हुई है
                if packed_df_merged is not None or rt_df_merged is not None or rto_df_merged is not None:
                    # 3. Excel फ़ाइल जनरेट करें
                    excel_data = convert_dfs_to_excel(packed_df_merged, rt_df_merged, rto_df_merged)
                    
                    st.success("Your multi-sheet Excel file is ready for download.")
                    
                    # 4. सिंगल Excel फ़ाइल डाउनलोड बटन
                    st.download_button(
                        label="⬇️ Download Merged Data (Excel)",
                        data=excel_data,
                        file_name='Merged_SKU_Report.xlsx',
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        key='download_excel'
                    )
                    
                    st.markdown("---")
                    st.subheader("Preview (First 5 Rows of Packed Data)")
                    if packed_df_merged is not None:
                         st.dataframe(packed_df_merged.head())
                    else:
                        st.info("Packed data was not processed successfully to show preview.")
                        
                else:
                    st.error("No data files were successfully processed. Please check file names inside the ZIP (Packed.csv, RT..csv, RTO.csv) and the columns in the Seller Listings Report.")

# Streamlit App को रन करें
if __name__ == "__main__":
    main()
