import streamlit as st
import pandas as pd
import io

# --- फ़ंक्शन: डेटा प्रोसेसिंग ---
def process_data(packed_file, rt_file, rto_file, seller_listings_file):
    """
    अपलोड की गई फ़ाइलों को पढ़ता है, SKU ID के आधार पर SKU Code को मर्ज करता है, 
    और प्रोसेस किए गए डेटाफ़्रेम को वापस करता है।
    """
    
    # 1. फ़ाइलों को पढ़ें
    try:
        # seller listings file को पढ़कर SKU ID और SKU Code निकालें
        # 'sku id' और 'sku code' कॉलम को कोटेशन मार्क के साथ पढ़ने के लिए engine='python' का उपयोग करें
        seller_df = pd.read_csv(seller_listings_file, engine='python')
        
        # आवश्यक कॉलम को चुनें और कॉलम के नाम से अतिरिक्त कोटेशन मार्क हटाएं
        sku_map_df = seller_df[['sku id', 'sku code']].copy()
        sku_map_df.columns = sku_map_df.columns.str.strip().str.replace('"', '')
        sku_map_df.rename(columns={'sku id': 'sku_id', 'sku code': 'sku_code'}, inplace=True)
        
        # डुप्लिकेट को हटा दें ताकि merging clean हो
        sku_map_df.drop_duplicates(subset=['sku_id'], inplace=True)
        
    except Exception as e:
        st.error(f"Seller Listings Report पढ़ने में त्रुटि: {e}")
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
                
                # 'sku_id' कॉलम का नाम RT और Packed/RTO में थोड़ा अलग हो सकता है, इसलिए इसे Normalize करें
                if 'sku_id' in df.columns:
                    merge_column = 'sku_id'
                elif 'sku id' in df.columns:
                    df.rename(columns={'sku id': 'sku_id'}, inplace=True)
                    merge_column = 'sku_id'
                else:
                    st.warning(f"File **{file_name}** does not contain a suitable 'sku id' column. Skipping merge.")
                    processed_dfs[df_key] = df
                    continue

                # 'sku_id' कॉलम को string में बदलें ताकि merging ठीक से हो
                df[merge_column] = df[merge_column].astype(str)
                sku_map_df['sku_id'] = sku_map_df['sku_id'].astype(str)
                
                # 'sku_id' के आधार पर 'sku_code' को मर्ज करें
                merged_df = pd.merge(df, sku_map_df, on=merge_column, how='left')
                
                # sku_code के मिसिंग values को 'Not Found' से भरें
                merged_df['sku_code'] = merged_df['sku_code'].fillna('Not Found')
                
                processed_dfs[df_key] = merged_df
                st.success(f"**{file_name}** successfully processed and merged. New column 'sku_code' added.")

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
    # Excel compatibility के लिए index=False का उपयोग करें
    return df.to_csv(index=False).encode('utf-8')

# --- Streamlit डैशबोर्ड लेआउट ---
def main():
    st.set_page_config(
        page_title="SKU Code Merger Dashboard",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🛍️ SKU Code Merger Dashboard")
    st.markdown("---")
    
    st.sidebar.header("📁 Upload Your Files")
    
    # फ़ाइल अपलोडर्स
    # `key` attribute का उपयोग करें ताकि Streamlit files को सही ढंग से differentiate कर सके
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
            with st.spinner("Merging SKU Codes... Please wait."):
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
