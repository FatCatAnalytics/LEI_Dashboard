import asyncio
from get_hierarchy import main, save_hierarchies_to_file, aggregate_hierarchy_data, process_uploaded_file
import streamlit as st
import pandas as pd


async def call_main(lei_list):
    return await main(lei_list)


# Initialize session state variables if they do not exist
if 'lei_single' not in st.session_state:
    st.session_state['lei_single'] = None

if 'lei_multiple' not in st.session_state:
    st.session_state['lei_multiple'] = None

if 'json_data_multiple' not in st.session_state:
    st.session_state['json_data_multiple'] = None

if 'json_data_single' not in st.session_state:
    st.session_state['json_data_single'] = None

# Create tabs for single and multiple LEI input
tab1, tab2 = st.tabs(["Add Single LEI", "Add Multiple LEI"])

# Handling Single LEI input
with tab1:
    lei_list = st.text_input("Upload a LEI to the database:", placeholder="1LZEPUPYJQ6SU0JEGH12",
                             key='company_lei_single',
                             help="Multiple LEIs can be separated by commas")

    if st.button("Fetch LEI Data", key='button_lei_single'):
        search_terms = [lei.strip() for lei in lei_list.split(",")]
        with st.spinner("Fetching data..."):
            extracted_data, json_data = asyncio.run(call_main(search_terms))
            st.session_state['lei_single'] = extracted_data
            st.session_state['json_data_single'] = json_data
        st.write(st.session_state['lei_single'])
        st.write("Data fetched and stored in session state.")

    # Button to save data to the database
    if st.session_state['json_data_single'] is not None:
        if st.button("Save Data to Database", key='save_to_db_single'):
            st.write("Save button pressed, calling save_hierarchies_to_file.")
            save_hierarchies_to_file(st.session_state['json_data_single'])
            aggregate_hierarchy_data(st.session_state['lei_single'])
            st.success("Data successfully saved to the database.")

# Handling Multiple LEI input
with tab2:
    uploaded_file = st.file_uploader("Upload a CSV or Excel file", type=['csv', 'xlsx'])
    if uploaded_file is not None:
        lei_list = process_uploaded_file(uploaded_file)

        if st.button("Fetch LEI Data", key='button_lei_multiple'):
            with st.spinner("Fetching data..."):
                extracted_data, json_data = asyncio.run(call_main(lei_list))
                st.session_state['lei_multiple'] = extracted_data
                st.session_state['json_data_multiple'] = json_data
            st.write(st.session_state['lei_multiple'])
            st.write("Data fetched and stored in session state.")

        # Button to save data to the database
        if st.session_state['json_data_multiple'] is not None:
            if st.button("Save Data to Database", key='save_to_db_multiple'):
                save_hierarchies_to_file(st.session_state['json_data_multiple'])
                aggregate_hierarchy_data(st.session_state['lei_multiple'])
                st.success("Data successfully saved to the database.")
