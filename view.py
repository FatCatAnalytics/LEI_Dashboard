import pandas as pd
import streamlit as st
from get_hierarchy import generate_interactive_network
import streamlit.components.v1 as components

# Load the data
aggregated_data = pd.read_csv("Datasources/aggregated_hierarchy.csv")  # Aggregated hierarchy data
extracted_data = pd.read_csv("Datasources/extracted_data.csv")  # Extracted data

# Title for the hierarchy overview
st.markdown('<h2 style="font-size:16px;">Hierarchy Overview</h2>', unsafe_allow_html=True)

# Combine LEI and Name into a single list for auto-complete search
lei_and_names = aggregated_data['ID'].astype(str).tolist() + aggregated_data['Name'].tolist()

# Insert an empty string as the placeholder for the selectbox
lei_and_names.insert(0, "")  # This will act as a placeholder option

# Auto-complete search box with empty default value
search_string = st.selectbox("Search by LEI or Name", options=lei_and_names, index=0)

# Ensure a valid selection is made (ignore empty string)
if search_string:
    # Identify columns that contain "ID" or "Name"
    search_columns = [col for col in extracted_data.columns if "ID" in col or "Name" in col]

    # Filter the extracted data based on search string in any of the "ID" or "Name" columns
    filtered_rows = extracted_data[search_columns].apply(
        lambda row: row.astype(str).str.contains(search_string, case=False).any(),
        axis=1
    )

    # Extract matching rows
    final_filtered_data = extracted_data[filtered_rows]

    # Display graph function
    def display_graph():
        graph_file = generate_interactive_network(final_filtered_data, search_string)

        with open(graph_file, 'r') as f:
            html_data = f.read()
            components.html(html_data, height=800, width=1200)

    # Display graph
    st.markdown(f"<h3 style='text-align: center; font-size: 20px;'>Filtered Data for {search_string}</h3>", unsafe_allow_html=True)
    display_graph()

    # Rename columns for better readability
    final_filtered_data.columns = final_filtered_data.columns.str.replace('Level_1', 'Ultimate Parent')
    final_filtered_data.columns = final_filtered_data.columns.str.replace('Level_', 'Child')

    # Display the filtered data
    st.write(final_filtered_data)
else:
    st.write("Please select a LEI or Name to filter.")
