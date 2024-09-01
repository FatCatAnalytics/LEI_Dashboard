import pandas as pd
import streamlit as st
from get_hierarchy import generate_interactive_network
import streamlit.components.v1 as components


aggregated_data = pd.read_csv("Datasources/aggregated_hierarchy.csv") # Load the aggregated data
extracted_data = pd.read_csv("Datasources/extracted_data.csv")  # Load the extracted data


st.markdown('<h2 style="font-size:16px;">Hierarchy Overview</h2>', unsafe_allow_html=True)

filter = st.radio("Search by", ["LEI", "Name", "CapIQ ID"])


if filter == "LEI":
    search_column = "ID"
elif filter == "Name":
    search_column = "Name"
elif filter == "CapIQ ID":
    search_column = "SP_Global"

search_string = st.selectbox("Select Columns", aggregated_data[search_column])


# Identify columns containing "name"
name_columns = [col for col in extracted_data.columns if search_column in col]

# Filter the DataFrame to find rows where any of these columns contain the search string
filtered_rows = extracted_data[name_columns].apply(lambda x: x.str.contains(search_string, case=False, na=False)).any(axis=1)

# Extract the value from the first column containing "name" for the matched row
matched_row = extracted_data[filtered_rows].iloc[0]
first_name_column_value = matched_row[name_columns[0]]

# Filter the entire DataFrame using the extracted value
final_filtered_data = extracted_data[extracted_data[name_columns[0]].str.contains(first_name_column_value, case=False, na=False)]


def display_graph():
    graph_file = generate_interactive_network(final_filtered_data,search_string)
    
    with open(graph_file,'r') as f: 
        html_data = f.read()
        components.html(html_data, height=800, width=1200)


st.markdown(f"<h3 style='text-align: center; font-size: 20px;'>Filtered Data for {search_string}</h3>", unsafe_allow_html=True)
display_graph()

final_filtered_data.columns = final_filtered_data.columns.str.replace('Level_1', 'Ultimate Parent')
final_filtered_data.columns = final_filtered_data.columns.str.replace('Level_', 'Child')

st.write(final_filtered_data)



