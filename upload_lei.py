import streamlit as st
import pandas as pd
import asyncio
from get_hierarchy import main,save_data, aggregate_hierarchy_data


st.markdown('<h2 style="font-size:16px;">Upload Legal Entity Identifier Data</h2>', unsafe_allow_html=True)

# Option 1: User inserts comma-separated LEI codes
lei_codes = st.text_input("Enter comma-separated LEI codes")
lei_list = lei_codes.split(",") if lei_codes else []

# Option 2: User uploads a file
uploaded_file = st.file_uploader("Upload a file", type=["csv", "xlsx"])
if uploaded_file is not None:
    file_extension = uploaded_file.name.split(".")[-1]
    
    # CSV file
    if file_extension == "csv":
        lei_list = uploaded_file.read().decode().split(",")
    
    # XLSX file
    elif file_extension == "xlsx":
        
        sheet_name = st.text_input("Enter sheet name")
        column_name = st.text_input("Enter column name")
        
        df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
        lei_list = df[column_name].tolist() if column_name in df.columns else []

if st.button("Fetch Records"):

    async def call_main(lei_list):
        return await main(lei_list)

    extracted_data, json_data = asyncio.run(call_main(lei_list))
    

    # Store the data in session state
    st.session_state['extracted_data'] = extracted_data
    st.session_state['json_data'] = json_data


    st.write("Data fetched...")

if st.button("Save Data"):
    # Retrieve the data from session state
    extracted_data = st.session_state.get('extracted_data', [])
    json_data = st.session_state.get('json_data', [])
    
    #check if either of the data exists
    if extracted_data.empty:
        st.write("No data to save. Please fetch the data first.")
    else:
        save_data(extracted_data, json_data)
        aggregate_hierarchy_data(extracted_data)
        st.write("Data saved successfully.")


