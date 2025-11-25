import streamlit as st
import pandas as pd
import io

# --- फ़ंक्शन: डेटा प्रोसेसिंग ---
def process_data(packed_file, rt_file, rto_file, seller_listings_file):
    """
    अपलोड की गई फ़ाइलों को पढ़ता है, SKU ID के आधार पर SKU Code और Seller SKU Code को मर्ज करता है, 
    और प्रोसेस किए गए डेटाफ़्रेम को वापस करता है।
    """
    
    # 1. Seller Listings File से मैपिंग डेटा निकालें
    try:
        # seller listings file को पढ़कर 'sku id' और "seller sku code" कॉलम निकालें
        # 'sku id' और "seller sku code" कॉलम को कोटेशन मार्क के साथ पढ़ने के लिए engine='python' का उपयोग करें
        seller_df = pd.read_csv(seller_listings_file, engine='python')
        
        # आवश्यक कॉलम को चुनें और कॉलम के नाम से अतिरिक्त कोटेशन मार्क हटाएं
        # ध्यान दें: अब हम 'sku code' के बजाय "seller sku code" का उपयोग कर रहे हैं
        sku_map_df = seller_df[['sku id', 'sku code', 'seller sku code']].copy()
        sku_map_df.columns = sku_map_df.columns.str.strip().str.replace('"', '').str.replace(' ', '_')
        
        # कॉलम के नाम Normalize करें
        sku_map_df.rename(columns={
            'sku_id': 'sku_id', 
            'sku_code': 'sku_code',
            'seller_sku_code': 'seller_sku_code'
        }, inplace=True)
        
        # डुप्लिकेट को हटा दें ताकि merging clean हो
        sku_map_df.drop_duplicates(subset=['sku_id'], inplace=True)
        
    except Exception as e:
        st.error(f"Seller Listings Report पढ़ने में त्रुटि या आवश्यक कॉलम नहीं मिले: {e}")
        return None, None, None

    # डेटाफ़्रेम की सूची बनाएं
    file_list = [
        ("Packed.csv", packed_file, 'packed_df'),
        ("RT..csv", rt_file, 'rt_df'),
        ("RTO.csv", rto_file, 'rto_df')
    ]
    
    processed_dfs = {}

    for file_name, uploaded_file, df_key in file_list:
        if uploaded_file is not None:
            st.info(f"Processing {file_name}...")
            try:
                # अन्य तीन फ़ाइलों को पढ़ें
                df = pd.read_csv(uploaded_file)
                
                # 'sku_id' कॉलम का नाम Normalize करें और मर्ज कॉलम को पहचानें
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
                    st.warning(f"File **{file_name}** does not contain a suitable 'sku id' column. Skipping merge.")
                    processed_dfs[df_key] = df
                    continue

                # 'sku_id' कॉलम को string में बदलें ताकि merging ठीक से हो
                df[merge_column] = df[merge_column].astype(str)
                sku_map_df['sku_id'] = sku_map_df['sku_id'].astype(str)
                
                # 'sku_id' के आधार पर 'seller_sku_code' और 'sku_code' को मर्ज करें
                merged_df = pd.merge(df, sku_map_df, on=merge_column, how='left')
                
                # seller_sku_code और sku_code के मिसिंग values को 'Not Found' से भरें
                merged_df['seller_sku_code'] = merged_df['seller_sku_code'].fillna('Not Found')
                merged_df['sku_code'] = merged_df['sku_code'].fillna('Not Found')

                # 2. कॉलम को 'sku_id' के आगे Insert करें
                # 'sku_id' कॉलम का Index पता करें
                sku_id_index = merged_df.columns.get_loc('sku_id')
                
                # 'seller_sku_code' और 'sku_code' को हटाने से पहले उनका डेटा निकाल लें
                seller_sku_col = merged_df.pop('seller_sku_code')
                sku_code_col = merged_df.pop('sku_code')
                
                # 'seller_sku_code' को 'sku_id' के ठीक आगे Insert करें (index + 1)
                merged_df.insert(sku_id_index + 1, 'seller_sku_code', seller_sku_col)
                
                # 'sku_code' को 'seller_sku_code' के ठीक आगे Insert करें (index + 2)
                merged_df.insert(sku_id_index + 2, 'sku_code', sku_code_col)

                # यदि मूल 'sku id' कॉलम का नाम 'sku id' था, तो उसे वापस ठीक करें (यह optional है, लेकिन अच्छा अभ्यास है)
                if original_sku_id_name == 'sku id':
                    merged_df.rename(columns={'sku_id': 'sku id'}, inplace=True)
                
                processed_dfs[df_key] = merged_df
                st.success(f"**{file_name}** successfully processed. 'seller_sku_code' and 'sku_code' added next to 'sku id'.")

            except Exception as e:
                st.error(f"Error reading or processing **{file_name}**: {e}")
                processed_dfs[df_key] = None
        else:
            processed_dfs[df_key] = None
    
    return processed_dfs.get('packed_df'), processed_dfs.get('rt_df'), processed_dfs.get('rto_df')

# --- फ़ंक्शन: CSV डाउनलोडर ---
def convert_df_to_csv(df):
    """
    Pandas DataFrame को CSV string में बदलता है।
    """
    return df.to_csv(index=False).encode('utf-8')

# --- Streamlit डैशबोर्ड लेआउट ---
def main():
    st.set_page_config(
        page_title="SKU Code Merger Dashboard",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🛍️ SKU Code Merger Dashboard (Updated)")
    st.markdown("---")
    
    st.sidebar.header("📁 Upload Your Files")
    
    # फ़ाइल अपलोडर्स
    seller_listings_file = st.sidebar.file_uploader(
        "Upload **Seller Listings Report.csv** (Required)", 
        type=['csv'],
        key="seller"
    )
    packed_file = st.sidebar.file_uploader(
        "Upload **Packed.csv**", 
        type=['csv'],
        key="packed"
    )
    rt_file = st.sidebar.file_uploader(
        "Upload **RT..csv**", 
        type=['csv'],
        key="rt"
    )
    rto_file = st.sidebar.file_uploader(
        "Upload **RTO.csv**", 
        type=['csv'],
        key="rto"
    )
    
    st.markdown("---")
    
    if st.sidebar.button("🚀 Start Processing & Merge"):
        if seller_listings_file is None:
            st.error("Please upload the **Seller Listings Report.csv** to start the process.")
        else:
            with st.spinner("Merging Seller SKU and SKU Codes... Please wait."):
                # डेटा प्रोसेसिंग
                packed_df_merged, rt_df_merged, rto_df_merged = process_data(
                    packed_file, rt_file, rto_file, seller_listings_file
                )

            st.header("✅ Processing Complete")
            
            # --- परिणाम और डाउनलोड सेक्शन ---
            
            # Packed Dataframe
            if packed_df_merged is not None:
                st.subheader("1. Packed Data (Merged)")
                st.dataframe(packed_df_merged.head())
                csv_packed = convert_df_to_csv(packed_df_merged)
                st.download_button(
                    label="Download Packed_Merged.csv",
                    data=csv_packed,
                    file_name='Packed_Merged.csv',
                    mime='text/csv',
                )
                st.markdown("---")
                
            # RT Dataframe
            if rt_df_merged is not None:
                st.subheader("2. RT Data (Merged)")
                st.dataframe(rt_df_merged.head())
                csv_rt = convert_df_to_csv(rt_df_merged)
                st.download_button(
                    label="Download RT_Merged.csv",
                    data=csv_rt,
                    file_name='RT_Merged.csv',
                    mime='text/csv',
                )
                st.markdown("---")
                
            # RTO Dataframe
            if rto_df_merged is not None:
                st.subheader("3. RTO Data (Merged)")
                st.dataframe(rto_df_merged.head())
                csv_rto = convert_df_to_csv(rto_df_merged)
                st.download_button(
                    label="Download RTO_Merged.csv",
                    data=csv_rto,
                    file_name='RTO_Merged.csv',
                    mime='text/csv',
                )
                st.markdown("---")
                
            if packed_df_merged is None and rt_df_merged is None and rto_df_merged is None:
                st.warning("No other data files were successfully uploaded or processed.")

# Streamlit App को रन करें
if __name__ == "__main__":
    main()
