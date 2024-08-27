import streamlit as st
import json
import os

# Load the JSON file into a dictionary
json_file_path = 'Data Sources/hierarchies.json'  # Adjust the path as needed

def load_existing_json(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)
    else:
        return {}

hierarchy_data = load_existing_json(json_file_path)

# Streamlit layout for the hierarchy page
st.title("View Single Corporate Hierarchy")

# Radio button to choose search method
search_method = st.radio("Search by", ("LEI", "Name"))

# Function to collect LEIs and names from a given dictionary, including children
def collect_leis_and_names_from_dict(data):
    leis = []
    names = []

    def collect(data):
        for key, value in data.items():
            leis.append(key)
            names.append(value.get('name', 'Unknown'))
            if 'children' in value:
                collect(value['children'])

    collect(data)
    return leis, names

# Collect all LEIs and names by iterating through each top-level key
all_leis, all_names = collect_leis_and_names_from_dict(hierarchy_data)

# Handling the search
if search_method == "LEI":
    selected_value = st.selectbox("Select LEI", all_leis)
else:
    selected_value = st.selectbox("Select Entity Name", all_names)

# Function to find the top-level key for a given LEI or Name
def find_top_level_key(data, search_key, search_value):
    def search(data, search_key, search_value):
        for key, value in data.items():
            if (search_key == 'LEI' and key == search_value) or (search_key == 'name' and value.get('name') == search_value):
                return key  # Found at this level, return the current key
            if 'children' in value:
                found_key = search(value['children'], search_key, search_value)
                if found_key:
                    return key  # Return the top-level key if a match is found in children
        return None

    return search(data, search_key, search_value)

# Function to retrieve the entire hierarchy for a given top-level key
def get_hierarchy_for_key(data, top_level_key):
    return data.get(top_level_key, {})

# Search and display results
if selected_value:
    search_key = 'LEI' if search_method == "LEI" else 'name'
    top_level_key = find_top_level_key(hierarchy_data, search_key, selected_value)

    if top_level_key:
        hierarchy_json = get_hierarchy_for_key(hierarchy_data, top_level_key)
        st.json(hierarchy_json)
    else:
        st.write("No related hierarchy found.")
else:
    st.write("Please select a value to search.")
