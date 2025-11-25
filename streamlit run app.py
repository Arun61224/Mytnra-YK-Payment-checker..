import streamlit as st
import pandas as pd
import io
import zipfile
from datetime import datetime # datetime import kiya gaya hai

# --- ZIP, Settlement & Excel Handling Functions (NO CHANGE) ---

def handle_packed_rto_zip_upload(zip_file):
    """Packed/RT/RTO ZIP फ़ाइल को एक्सट्रैक्ट करता है।"""
    if zip_file is None:
        return None, None, None, False
    csv_data = {}
    required_files = ["Packed.csv", "RT..csv", "RTO.csv"]
    st.info("Extracting files from the Data ZIP archive...")
    try:
        with zipfile.ZipFile(zip_file, 'r') as z:
            # We only need Packed.csv for now, but will extract all for robust future use
            for file_name in required_files:
                try:
                    file_content = z.read(file_name).decode('utf-8', errors='ignore')
                    csv_data[file_name] = io.StringIO(file_content)
                except KeyError:
                    # RTO/RT are optional for this stage, so warning instead of error
                    st.warning(f"Optional file **{file_name}** not found in the Data ZIP archive. Skipping.")
        # Return all three objects (Packed, RT, RTO)
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
                    file_content = z.read(file_name).decode('utf-8', errors='ignore')
                    extracted_csv_objects.append(io.StringIO(file_content))
            return extracted_csv_objects
    except Exception as e:
        st.error(f"An error occurred during {process_name} ZIP file extraction: {e}")
        return []

def handle_outstanding_csv(csv_file):
    """Outstanding CSV फ़ाइल को StringIO ऑब्जेक्ट में बदलता है।"""
    if csv_file is None:
        return []
    try:
        # csv_file is already a File object, use getvalue() if it's a BytesIO object
        file_content = csv_file.getvalue().decode('utf-8', errors='ignore')
        return [io.StringIO(file_content)]
    except AttributeError:
        # If it's a regular file upload, pandas will handle it later
        st.warning("Handling Outstanding file as a direct CSV stream.")
        return [csv_file]
    except Exception as e:
        st.error(f"An error occurred during Outstanding CSV file handling: {e}")
        return []
        
# ---------------------------------------------------------------------------------
# --- SKU Merger (Invoice Logic REMOVED) (NO CHANGE) ---
# ---------------------------------------------------------------------------------

def process_sku_merger(packed_file_obj, rt_file_obj, rto_file_obj, seller_listings_file, cost_sheet_file):
    st.subheader("1. SKU Code & Cost Price Merger Process")
    
    # --- 1. Seller Listings Report Process (SKU Mapping) ---
    try:
        seller_df = pd.read_csv(seller_listings_file, engine='python')
        sku_map_df = seller_df[['sku id', 'sku code', 'seller sku code']].copy()
        sku_map_df.columns = sku_map_df.columns.str.strip().str.replace('"', '').str.replace(' ', '_')
        sku_map_df.rename(columns={'sku_id': 'sku_id', 'sku_code': 'sku_code', 'seller_sku_code': 'seller_sku_code'}, inplace=True)
        sku_map_df.drop_duplicates(subset=['sku_id'], inplace=True)
        sku_map_df['sku_id'] = sku_map_df['sku_id'].astype(str)
        st.success("Seller SKU Map created.")
    except Exception as e:
        st.error(f"Seller Listings Report पढ़ने में त्रुटि या आवश्यक कॉलम नहीं मिले: {e}")
        return None, None, None

    # --- 2. Cost Sheet Process (Cost Price Mapping) ---
    cost_map_df = None
    if cost_sheet_file is not None:
        try:
            cost_df = pd.read_excel(cost_sheet_file) if cost_sheet_file.name.endswith('.xlsx') else pd.read_csv(cost_sheet_file)
            cost_df.columns = cost_df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('"', '')
            sku_col_name = next((col for col in cost_df.columns if 'seller_sku' in col or 'sku_code' in col), None)
            cost_col_name = next((col for col in cost_df.columns if 'cost' in col or 'price' in col), None)
            
            if sku_col_name and cost_col_name:
                cost_map_df = cost_df[[sku_col_name, cost_col_name]].copy()
                cost_map_df.columns = ['seller_sku_code', 'Cost_Price']
                cost_map_df['seller_sku_code'] = cost_map_df['seller_sku_code'].astype(str)
                cost_map_df['Cost_Price'] = pd.to_numeric(cost_map_df['Cost_Price'], errors='coerce').fillna(0.0)
                cost_map_df.drop_duplicates(subset=['seller_sku_code'], keep='first', inplace=True)
                st.success("Cost Price Map created successfully.")
            else:
                st.warning("Cost Sheet में आवश्यक कॉलम 'Seller SKU Code' या 'Cost Price' नहीं मिले। Cost Price मैप नहीं किया जाएगा।")
        except Exception as e:
            st.error(f"Error reading or processing Cost Sheet: {e}")

    # --- 3. Merging DataFrames ---
    file_list = [
        ("Packed.csv", packed_file_obj, 'packed_df', ['order_id']),
        ("RT..csv", rt_file_obj, 'rt_df', ['old_parent_order_id']),
        ("RTO.csv", rto_file_obj, 'rto_df', ['order_id', 'old_parent_id'])
    ]
    
    processed_dfs = {}

    # ------------------ Process ONLY PACKED DF (and skip RT/RTO) ------------------
    for file_name, file_obj, df_key, order_id_cols in file_list:
        if df_key != 'packed_df': # Skip RT and RTO for now
            processed_dfs[df_key] = None
            continue

        if file_obj is not None:
            try:
                df = pd.read_csv(file_obj)
                
                # --- Step 3a: Merge SKU ID to get Seller SKU Code ---
                merge_column = next((col for col in ['sku_id', 'sku id'] if col in df.columns), None)
                if merge_column:
                    original_sku_id_name = merge_column
                    if merge_column != 'sku_id':
                        df.rename(columns={merge_column: 'sku_id'}, inplace=True)
                    
                    df['sku_id'] = df['sku_id'].astype(str)
                    merged_df = pd.merge(df, sku_map_df, on='sku_id', how='left')
                    merged_df['seller_sku_code'] = merged_df['seller_sku_code'].fillna('Not Found')
                    merged_df['sku_code'] = merged_df['sku_code'].fillna('Not Found')
                    
                    if original_sku_id_name != 'sku_id':
                        merged_df.rename(columns={'sku_id': original_sku_id_name}, inplace=True)
                else:
                     merged_df = df
                     st.warning(f"**{file_name}**: SKU ID column not found, skipping SKU merger.")

                # --- Step 3b: Merge Cost Price using Seller SKU Code ---
                if cost_map_df is not None:
                    sku_col_in_df = 'seller_sku_code'
                    
                    if sku_col_in_df in merged_df.columns:
                        df_for_cost_merge = merged_df.copy()
                        df_for_cost_merge[sku_col_in_df] = df_for_cost_merge[sku_col_in_df].astype(str)
                        
                        merged_with_cost = pd.merge(
                            df_for_cost_merge, 
                            cost_map_df[['seller_sku_code', 'Cost_Price']], 
                            on='seller_sku_code', 
                            how='left'
                        )
                        merged_with_cost['Cost_Price'] = merged_with_cost['Cost_Price'].fillna(0.0)
                        merged_df = merged_with_cost
                        st.success(f"**{file_name}** merged with Cost Prices.")
                    else:
                        st.warning(f"**{file_name}**: 'seller_sku_code' column not found, skipping Cost merger.")


                # --- Step 3c: Invoice Number Merge is REMOVED ---
                # Invoice mapping logic removed as per user request.
                
                processed_dfs[df_key] = merged_df
                st.success(f"**{file_name}** successfully processed.")

            except Exception as e:
                st.error(f"Error reading or processing **{file_name}**: {e}")
                processed_dfs[df_key] = None
        else:
            processed_dfs[df_key] = None
    
    return processed_dfs.get('packed_df'), processed_dfs.get('rt_df'), processed_dfs.get('rto_df')

# ---------------------------------------------------------------------------------
# --- Combined Settlement Pivot Processor (NO CHANGE) ---
# ---------------------------------------------------------------------------------

def process_combined_settlement(all_csv_objects):
    """
    सभी Prepaid, Postpaid, और Outstanding data को पढ़ता है और Merged Pivot Table
    को Settled और Outstanding अमाउंट के Bifurcation के साथ और **Total Receivable**
    के साथ बनाता है।
    """
    TARGET_COL_ID = 'order_release_id'
    MATCH_IDS = ['orderreleaseid', 'releaseid']
    MATCH_SETTLED = 'settledamount'
    MATCH_UNSETTLED = 'unsettledamount'

    st.subheader("2. Combined Settlement & Outstanding Pivot")
    
    if not all_csv_objects:
        st.warning("No payment files were uploaded or extracted successfully.")
        return None

    all_dfs = []

    for i, file_obj in enumerate(all_csv_objects):
        file_name = f"Combined_Payment_File_{i+1}"
        try:
            df = pd.read_csv(file_obj)
            
            normalized_cols = {col: col.strip().replace('"', '').lower().replace('_', '') for col in df.columns}
            
            found_id_name = None
            found_amount_name = None
            amount_type = None

            for original_name, norm_name in normalized_cols.items():
                
                if norm_name in MATCH_IDS and found_id_name is None:
                    found_id_name = original_name
                
                if norm_name == MATCH_SETTLED:
                    found_amount_name = original_name
                    amount_type = 'Settled'
                elif norm_name == MATCH_UNSETTLED and amount_type is None:
                    found_amount_name = original_name
                    amount_type = 'Unsettled'
            
            if not found_id_name or not found_amount_name:
                st.error(f"File **{file_name}** is missing required ID or Amount columns. Skipping.")
                continue

            df_subset = df[[found_id_name, found_amount_name]].copy()
            
            df_subset.rename(columns={
                found_id_name: TARGET_COL_ID, 
                found_amount_name: 'Amount_Value'
            }, inplace=True)
            
            df_subset['Amount_Value'] = pd.to_numeric(df_subset['Amount_Value'], errors='coerce')
            
            if amount_type == 'Settled':
                df_subset['Settled_Amount_Type'] = df_subset['Amount_Value']
                df_subset['Outstanding_Amount_Type'] = 0.0
            else:
                df_subset['Settled_Amount_Type'] = 0.0
                df_subset['Outstanding_Amount_Type'] = df_subset['Amount_Value']
            
            df_subset['Total_Amount_Type'] = df_subset['Amount_Value']
            
            all_dfs.append(df_subset)
            
        except Exception as e:
            st.error(f"Error reading **{file_name}**: {e}")
            
    if not all_dfs:
        st.error("No combined payment data could be processed successfully.")
        return None
        
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    pivot_table = combined_df.groupby(TARGET_COL_ID).agg(
        Total_Settled_Amount=('Settled_Amount_Type', 'sum'),
        Total_Outstanding_Amount=('Outstanding_Amount_Type', 'sum') 
    ).reset_index()
    
    # Calculate Total Receivable (B + C)
    pivot_table['Total_Receivable'] = pivot_table['Total_Settled_Amount'] + pivot_table['Total_Outstanding_Amount']

    # Renaming 'order_release_id' to 'order_id'
    pivot_table.rename(columns={
        TARGET_COL_ID: 'order_id',
        'Total_Receivable': 'Total Receivable' 
    }, inplace=True) 
    
    # --- NUMERIC CONVERSION (FOR EXCEL OUTPUT) ---
    try:
        pivot_table['order_id'] = pd.to_numeric(
            pivot_table['order_id'], errors='coerce'
        ).astype(pd.Int64Dtype())
        st.success("✅ **'order_id'** in Payment_Pivot converted to large numeric format (Int64).")
    except Exception as e:
        st.warning(f"⚠️ Warning: Could not convert 'order_id' to Int64 type in Payment_Pivot. Keeping as string for merge/safety. ({e})")
        # Ensure it is at least a clean string if Int64 conversion fails
        pivot_table['order_id'] = pivot_table['order_id'].astype(str)
    # ------------------------------------------------------------------------

    # Ensure the order is correct (A, B, C, D)
    pivot_table = pivot_table[['order_id', 'Total_Settled_Amount', 'Total_Outstanding_Amount', 'Total Receivable']]
    
    st.success("Final Merged Payment Pivot Table created successfully with 'Total Receivable'.")
    return pivot_table

# ---------------------------------------------------------------------------------
# --- MODIFIED: Create Final Report Sheet (Date Format and Column Order Updated) ---
# ---------------------------------------------------------------------------------

def create_final_packed_sheet(packed_df, payment_pivot_df):
    """
    Packed data को Payment Pivot data के साथ merge करता है, Total Payment Received/Outstanding 
    कैलकुलेट करता है, और columns को final format में select करता है।
    
    इसमें Order_Packed_Date का Format (YYYYMMDD to DD-MMM-YYYY) और Order भी बदला गया है।
    """
    if packed_df is None:
        st.error("Packed data is not available for final report generation.")
        return None
        
    st.subheader("3. Generating Final Report Sheet")

    # Normalize column names in packed_df first
    packed_df.columns = packed_df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('"', '')
    
    # Check if 'order_id' exists in Packed DF
    if 'order_id' not in packed_df.columns:
         st.error("Critical Error: 'order_id' column not found in Packed.csv, cannot proceed with payment merger.")
         return packed_df # Return packed data without merge

    # 1. Merge Payment Data (using 'order_id')
    if payment_pivot_df is not None:
        
        # --- SAFE MERGE KEY CONVERSION (Force String for Merge Compatibility) ---
        packed_df['order_id'] = packed_df['order_id'].astype(str).str.strip().fillna('NO_PACKED_ID')
        payment_pivot_df['order_id'] = payment_pivot_df['order_id'].astype(str).str.strip().fillna('NO_PIVOT_ID')
        
        st.info("Ensuring 'order_id' is clean string format for safe merging.")
        # -----------------------------------------------------------------------


        # Merge both Settled and Outstanding amounts
        final_df = pd.merge(
            packed_df,
            payment_pivot_df[['order_id', 'Total_Settled_Amount', 'Total_Outstanding_Amount']],
            on='order_id',
            how='left'
        )
        # Fill NaN values (where no match was found) with 0.0
        final_df['Total_Settled_Amount'] = final_df['Total_Settled_Amount'].fillna(0.0)
        final_df['Total_Outstanding_Amount'] = final_df['Total_Outstanding_Amount'].fillna(0.0)
        
        # Calculate Total Payment
        final_df['Total_Payment'] = final_df['Total_Settled_Amount'] + final_df['Total_Outstanding_Amount']
        
        # --- DEBUGGING CHECK ---
        if (final_df['Total_Settled_Amount'] > 0).any():
             st.success("✅ Payment received amounts found and merged successfully.")
        else:
             st.warning("⚠️ Warning: Payment merge successful, but Total_Settled_Amount column is all zeros. Check if Order IDs match between Packed and Payment data.")
        # -----------------------

    else:
        final_df = packed_df.copy()
        final_df['Total_Settled_Amount'] = 0.0
        final_df['Total_Outstanding_Amount'] = 0.0
        final_df['Total_Payment'] = 0.0 
        st.warning("Payment Pivot data not available. Payment amounts set to 0.")


    # --- 2. Date Formatting (YYYYMMDD to DD-MMM-YYYY) ---
    date_col = 'order_packed_date'
    if date_col in final_df.columns:
        def format_packed_date(date_str):
            if pd.isna(date_str) or not date_str:
                return None
            try:
                # Assuming date_str is either 20251021 or 2025-10-21 (or similar)
                date_str = str(int(date_str)) if isinstance(date_str, (int, float)) else str(date_str).split(' ')[0].replace('-', '')
                if len(date_str) == 8:
                    return datetime.strptime(date_str, '%Y%m%d').strftime('%d-%b-%Y')
                return date_str # Return as is if format is unknown
            except:
                return None
        
        final_df[date_col] = final_df[date_col].apply(format_packed_date)
        st.success(f"✅ Date format in **'{date_col}'** converted to **DD-MMM-YYYY**.")
    else:
        st.warning(f"⚠️ Warning: Date column '{date_col}' not found for formatting.")


    # --- 3. Select and format required columns (New Order) ---
    required_cols_order = [
        'order_packed_date', # NEW POSITION: First
        'order_id', 
        'brand', 
        'seller_sku_code', 
        'shipment_value', 
        'tax_amount', 
        'quantity', 
        'cost_price', 
        'total_settled_amount',      
        'total_outstanding_amount', 
        'total_payment'             
    ]

    # Map normalized names to the desired final column names
    col_mapping = {
        'order_packed_date': 'Order_Packed_Date',
        'order_id': 'Order_ID',
        'brand': 'Brand',
        'seller_sku_code': 'Seller_SKU_Code',
        'shipment_value': 'Shipment_Value',
        'tax_amount': 'Tax_Amount',
        'quantity': 'Quantity',
        'cost_price': 'Cost_Price',
        'total_settled_amount': 'Payment_Received', 
        'total_outstanding_amount': 'Total_Outstanding_Amount',
        'total_payment': 'Total_Payment_Settled_Plus_Outstanding' 
    }
    
    selected_cols = []
    for col in required_cols_order:
        normalized_col = col.lower().replace(' ', '_').replace('"', '') 
        if normalized_col in final_df.columns:
            selected_cols.append(normalized_col)
            
    final_report_df = final_df[selected_cols].copy()
    
    final_report_df.columns = [col_mapping.get(col, col) for col in final_report_df.columns]
    
    # --- ORDER_ID NUMERIC CONVERSION (FOR FINAL DISPLAY/EXCEL) ---
    if 'Order_ID' in final_report_df.columns:
        try:
            # Convert to numeric for Excel output (safe Int64 with NaN support)
            final_report_df['Order_ID'] = pd.to_numeric(
                final_report_df['Order_ID'], errors='coerce'
            ).astype(pd.Int64Dtype())
            st.success("✅ **'Order_ID'** in Final_Report is now large numeric format (Int64).")
        except Exception as e:
            st.warning(f"⚠️ Warning: Could not ensure 'Order_ID' is Int64 in Final_Report. Keeping as string for safety. ({e})")

    
    st.success("Final Packed Report sheet created. Check the bottom right of the table for payment columns.")
    return final_report_df

# ---------------------------------------------------------------------------------
# --- Multi-Sheet Excel Downloader (Only Final_Report and Payment_Pivot) (NO CHANGE) ---

def convert_dataframes_to_excel(df_packed, df_rt, df_rto, df_merged_pivot, df_final_report):
    """DataFrames को एक Excel फ़ाइल की अलग-अलग शीट्स में लिखता है। केवल Final_Report और Payment_Pivot को शामिल करता है।"""
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        if df_final_report is not None:
            df_final_report.to_excel(writer, sheet_name='Final_Report', index=False)
        
        # Packed, RT, RTO sheets are now ignored as per user request.

        if df_merged_pivot is not None:
            df_merged_pivot.to_excel(writer, sheet_name='Payment_Pivot', index=False) 
    
    processed_excel_data = output.getvalue()
    return processed_excel_data


# --- Streamlit डैशबोर्ड लेआउट (NO CHANGE from last modification) ---

def main():
    st.set_page_config(
        page_title="SKU, Cost & Settlement Data Processor",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🛍️ SKU, Cost & Settlement Data Processor")
    st.markdown("---")
    
    # ----------------------------------------------------
    #                  SIDEBAR UPLOADERS 
    # ----------------------------------------------------
    st.sidebar.header("📁 1. Files for SKU & Cost Merger") # Header Updated
    
    seller_listings_file = st.sidebar.file_uploader(
        "Upload **Seller Listings Report.csv** (Required for SKU Merger)", 
        type=['csv'],
        key="seller"
    )
    data_zip_file = st.sidebar.file_uploader(
        "Upload **Packed, RT, RTO files as a ZIP**", 
        type=['zip'],
        key="data_zip"
    )
    
    cost_sheet_file = st.sidebar.file_uploader(
        "Upload **Cost Sheet (Excel/CSV)** (Optional for Cost Price)", 
        type=['xlsx', 'csv'],
        key="cost_sheet"
    )

    # sales_b2c_file (Invoice Mapping) Uploader is REMOVED
    
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
    
    outstanding_csv_file = st.sidebar.file_uploader(
        "Upload **Outstanding Payment CSV**", 
        type=['csv'],
        key="outstanding_csv"
    )
    
    st.markdown("---")
    
    df_merged_pivot = None 
    packed_df_merged, rt_df_merged, rto_df_merged = None, None, None
    df_final_report = None
    
    if st.sidebar.button("🚀 Start All Processing"):
        
        # ----------------------------------------------------
        #                  1. Payment Pivot Execution
        # ----------------------------------------------------
        st.header("--- Combined Payment & Outstanding Pivot Results ---")
        
        prepaid_objects = handle_settlement_zip(prepaid_zip_file, "Prepaid")
        postpaid_objects = handle_settlement_zip(postpaid_zip_file, "Postpaid")
        # Ensure that handle_outstanding_csv returns a list of file objects
        outstanding_objects = handle_outstanding_csv(outstanding_csv_file) 
        
        all_csv_objects = prepaid_objects + postpaid_objects + outstanding_objects
        
        if all_csv_objects:
            with st.spinner("Processing all payment files and creating Merged Pivot Table..."):
                df_merged_pivot = process_combined_settlement(all_csv_objects)
        else:
            st.warning("Skipping Combined Pivot: No payment files were uploaded successfully.")

        
        # ----------------------------------------------------
        #                  2. SKU & Cost Merger Execution (Only Packed)
        # ----------------------------------------------------
        st.header("--- SKU Code & Cost Price Merger Results ---") # Header Updated
        if seller_listings_file is None or data_zip_file is None:
            st.warning("Skipping SKU Merger: Required files not uploaded.")
        else:
            packed_obj, rt_obj, rto_obj, success = handle_packed_rto_zip_upload(data_zip_file)
            
            if success:
                with st.spinner("Merging SKU and Cost data into Packed Sheet..."):
                    # NOTE: Removed sales_b2c_file argument
                    packed_df_merged, rt_df_merged, rto_df_merged = process_sku_merger(
                        packed_obj, rt_obj, rto_obj, seller_listings_file, cost_sheet_file
                    )
            
        # ----------------------------------------------------
        #                  3. Final Report Generation
        # ----------------------------------------------------
        st.header("--- Final Report Sheet Generation ---")
        if packed_df_merged is not None:
             df_final_report = create_final_packed_sheet(packed_df_merged, df_merged_pivot)
        
        
        # ----------------------------------------------------
        #             4. Final Excel Generation
        # ----------------------------------------------------
        st.header("--- 💾 Final Excel Download ---")
        
        if df_final_report is not None:
            with st.spinner("Generating Multi-Sheet Excel Workbook..."):
                excel_data = convert_dataframes_to_excel(
                    packed_df_merged, rt_df_merged, rto_df_merged, df_merged_pivot, df_final_report
                )
            
            st.success("✅ Multi-sheet Excel file is ready. Your **Final_Report** sheet is the first sheet.")
            
            st.download_button(
                label="⬇️ Download Complete Merged Data (Excel)",
                data=excel_data,
                file_name='Final_Merged_Report.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                key='download_excel'
            )
            st.markdown("---")
            
            st.subheader("Preview of Final Report Sheet (Payment Columns Clearly Grouped)")
            st.dataframe(df_final_report.head(10))

        else:
            st.error("Final Excel report could not be generated. Please check all file uploads and processing steps.")


# Streamlit App को रन करें
if __name__ == "__main__":
    main()
