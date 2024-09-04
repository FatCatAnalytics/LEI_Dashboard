import json
import os
import asyncio
import aiohttp
import pandas as pd
import logging
import networkx as nx
import tempfile
from pyvis.network import Network


logging.basicConfig(level=logging.INFO)


def save_data(extracted_data, json_data):
    # Paths to the data files
    json_data_file = 'Datasources/lei_data.json'
    extracted_data_file_path = 'Datasources/extracted_data.csv'

    # Load existing JSON data if the file exists
    if os.path.exists(json_data_file):
        with open(json_data_file, 'r') as f:
            existing_json_data = json.load(f)
    else:
        existing_json_data = []

    # Ensure json_data is a dictionary
    if not isinstance(json_data, dict):
        raise ValueError("json_data must be a dictionary")

    # Convert json_data to a list of dictionaries
    json_data_list = [{lei: details} for lei, details in json_data.items()]

    # Extract LEIs from the new json_data
    new_leis = set(json_data.keys())

    # Extract LEIs from the existing json_data
    existing_leis = {list(entry.keys())[0] for entry in existing_json_data}

    # Filter out LEIs that already exist in the existing JSON data
    filtered_json_data_list = [entry for entry in json_data_list if list(entry.keys())[0] not in existing_leis]

    # Append new data to the json_data_file
    if filtered_json_data_list:
        existing_json_data.extend(filtered_json_data_list)
        with open(json_data_file, 'w') as f:
            json.dump(existing_json_data, f)
        logging.info(f"Appended {len(filtered_json_data_list)} new LEIs to the JSON file.")
    else:
        logging.info("No new LEIs to add.")

    # Save only new extracted_data to the CSV file
    if os.path.exists(extracted_data_file_path):
        existing_extracted_data = pd.read_csv(extracted_data_file_path)
        new_extracted_data = extracted_data[~extracted_data['Level_1_ID'].isin(existing_extracted_data['Level_1_ID'])]
        if not new_extracted_data.empty:
            combined_data = pd.concat([existing_extracted_data, new_extracted_data])
            combined_data.to_csv(extracted_data_file_path, index=False)
            logging.info(f"Appended {len(new_extracted_data)} new LEIs to the CSV file.")
        else:
            logging.info("No new LEIs to add to the CSV file.")
    else:
        extracted_data.to_csv(extracted_data_file_path, index=False)
        logging.info("Extracted data saved to new CSV file.")
    

# Load previously saved hierarchies
def load_saved_hierarchies(file_path='Datasources/lei_data.json'):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            logging.info(f"Loading cached hierarchies from {file_path}")
            data = json.load(f)
            
            if isinstance(data, list):
                all_hierarchies = {}
                for item in data:
                    if isinstance(item, dict):
                        all_hierarchies.update(item)
                    else:
                        logging.warning(f"Expected a dictionary but got {type(item)} in the list. Skipping item.")
                return all_hierarchies
            else:
                logging.warning(f"Expected a list but got {type(data)}. Returning an empty dictionary.")
                return {}
    else:
        logging.warning(f"File {file_path} does not exist. Returning an empty dictionary.")
        return {}


async def fetch(session, url):
    try:
        async with session.get(url) as response:
            logging.info(f"Fetching URL: {url} - Status: {response.status}")
            if response.status == 429:
                logging.warning(f"Rate limit exceeded. Waiting for 10 seconds before retrying...")
                await asyncio.sleep(10)
                return await fetch(session, url)
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        logging.error(f"Error fetching data from {url}: {e}")
        return None


async def get_ultimate_parent(session, lei):
    url = f"https://api.gleif.org/api/v1/lei-records/{lei}/ultimate-parent-relationship"
    data = await fetch(session, url)
    if data and 'data' in data:
        relationship_data = data['data'][0] if isinstance(data['data'], list) else data['data']
        return relationship_data.get('attributes', {}).get('relationship', {}).get('endNode', {}).get('id')
    logging.warning(f"No ultimate parent found for LEI: {lei}")
    return None


async def get_direct_children(session, lei):
    children = []
    url = f"https://api.gleif.org/api/v1/lei-records/{lei}/direct-child-relationships"
    while url:
        data = await fetch(session, url)
        if data and 'data' in data:
            children.extend([
                child.get('attributes', {}).get('relationship', {}).get('startNode', {}).get('id')
                for child in data['data']
            ])
            url = data.get('links', {}).get('next')
        else:
            break
    return children


async def get_legal_entity_name(session, lei):
    url = f"https://api.gleif.org/api/v1/lei-records/{lei}"
    data = await fetch(session, url)
    if data and 'data' in data and 'attributes' in data['data']:
        lei = data['data']['attributes'].get('lei')
        name = data['data']['attributes'].get('entity', {}).get('legalName', {}).get('name')
        spglobal_list = data['data']['attributes'].get('spglobal', {})
        spglobal = spglobal_list[0] if spglobal_list else None
        return lei, name, spglobal
    logging.error(f"No legal entity name found for LEI: {lei}, API response: {data}")
    return None, None, None  # Ensure it returns a tuple


async def build_hierarchy(session, lei):
    try:
        lei, name, spglobal = await get_legal_entity_name(session, lei)
        if lei is None and name is None:
            logging.error(f"No name and spglobal data for LEI: {lei}")
        hierarchy = {lei: {"name": name, "spglobal": spglobal, "children": {}}}
        children = await get_direct_children(session, lei)
        for child in children:
            child_hierarchy = await build_hierarchy(session, child)
            if child_hierarchy:
                hierarchy[lei]["children"][child] = child_hierarchy[child]
        return hierarchy
    except Exception as e:
        logging.error(f"Error building hierarchy for LEI: {lei}, error: {e}")
        return {lei: {"name": None, "spglobal": None, "children": {}}}


async def process_single_lei(session, lei, all_hierarchies):
    try:
        ultimate_parent = await get_ultimate_parent(session, lei) or lei
        hierarchy = await build_hierarchy(session, ultimate_parent)
        all_hierarchies[lei] = hierarchy
    except Exception as e:
        logging.error(f"Error processing LEI: {lei}, error: {e}")


async def process_leis(lei_list, batch_size=10, delay=15):
    all_hierarchies = load_saved_hierarchies()
    keys = all_hierarchies.keys()


    leis_to_process = [lei for lei in lei_list if lei not in all_hierarchies]
    if not leis_to_process:
        logging.info("All requested LEIs found in cache. No processing needed.")
        return all_hierarchies

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(leis_to_process), batch_size):
            batch = leis_to_process[i:i + batch_size]
            tasks = [process_single_lei(session, lei, all_hierarchies) for lei in batch]
            await asyncio.gather(*tasks)
            logging.info(f"Processed {i + len(batch)}/{len(leis_to_process)} LEIs")

            # Introduce a small delay to ensure Streamlit has time to render the UI updates
            await asyncio.sleep(0.1)

            # Wait for the delay time before processing the next batch
            if i + batch_size < len(leis_to_process):
                logging.info(f"Waiting for {delay} seconds before processing the next batch...")
                await asyncio.sleep(delay)

    return all_hierarchies


def flatten_hierarchy(data, path=[]):
    print(data)
    rows = []
    if data is None:
        return rows  # Return empty list if data is None

    for entry in data:
        if entry is None or not isinstance(entry, dict):
            continue  # Skip if the entry is None or not a dictionary

        for key, value in entry.items():
            if not isinstance(value, dict) or 'children' not in value:
                continue  # Skip if no 'children' key or value is not a dictionary

            # Safely access 'children' after checking 'value' is a dictionary
            children = value.get('children', {})

            # Construct current path only if necessary keys exist
            current_path = path + [key, value.get('name', 'Unknown'), value.get('spglobal', 'Unknown')]

            if children:  # Recursively process children if they exist
                child_data = [{k: v} for k, v in children.items() if v is not None]
                rows.extend(flatten_hierarchy(child_data, current_path))
            else:
                rows.append(current_path)

    return rows


def process_uploaded_file(uploaded_file):

    if uploaded_file.name.endswith('.xlsx'):

        xls = pd.ExcelFile(uploaded_file)

        sheet_name =  xls.sheet_names

        df = pd.read_excel(uploaded_file, sheet_name=sheet_name)
    else:

        df = pd.read_csv(uploaded_file)
        column_name = df.columns

    return df[column_name].tolist()


def generate_interactive_network(df, entity_name):
    title = f"Hierarchy for {entity_name}"
    # Initialize a directed graph
    G = nx.DiGraph()

    for index, row in df.iterrows():
        parent = row['Level_1_ID']
        parent_name = row.get('Level_1_Name', '')

        # Set the parent node color to light red
        if parent not in G:
            node_color = "lightcoral" if parent_name != entity_name else "green"
            G.add_node(parent, label=f"{parent_name}\n({parent})", color=node_color)

        for level in range(2, 10):  # Adjust this range if more levels exist
            child = row.get(f'Level_{level}_ID')
            child_name = row.get(f'Level_{level}_Name', '')

            if pd.notna(child):  # Check if the child ID is not NaN
                G.add_edge(parent, child)

                # Set the node color based on the level and entity name
                if child_name == entity_name:
                    node_color = "green"
                elif level == 2:
                    node_color = "blue"  # Next level to blue
                else:
                    node_color = "lightblue"  # Subsequent levels to light blue

                G.add_node(child, label=f"{child_name}\n({child})", color=node_color)

                parent = child  # The current child becomes the next parent

    # Create a PyVis network with basic settings
    net = Network(height="750px", width="100%", directed=True)

    # Add nodes and edges to the network
    for node in G.nodes:
        net.add_node(node, label=G.nodes[node]['label'], color=G.nodes[node]['color'])
    for source, target in G.edges():
        net.add_edge(source, target)

    # Customize the appearance to remove borders with correct JSON formatting
    net.set_options('''
    {
      "nodes": {
        "borderWidth": 0,
        "borderWidthSelected": 0,
        "color": {
          "border": "rgba(0, 0, 0, 0)",
          "highlight": {
            "border": "rgba(0, 0, 0, 0)",
            "background": "rgba(255,0,0,0.5)"
          }
        }
      },
      "edges": {
        "color": {
          "color": "gray",
          "highlight": "rgba(0, 0, 0, 0)",
          "inherit": false,
          "opacity": 1.0
        },
        "width": 1,
        "arrows": {
          "to": {
            "enabled": true,
            "scaleFactor": 1
          },
          "from": {
            "enabled": false
          }
        }
      },
      "physics": {
        "enabled": true,
        "solver": "forceAtlas2Based",
        "forceAtlas2Based": {
          "gravitationalConstant": -50,
          "centralGravity": 0.005,
          "springLength": 230,
          "springConstant": 0.18,
          "damping": 0.4,
          "avoidOverlap": 1
        }
      }
    }
    ''')

    # Save the network to a temporary file and load it into Streamlit
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
            net.write_html(tmp_file.name)

            # Inject the title into the HTML
            with open(tmp_file.name, 'r') as f:
                html_content = f.read()

            # Comment out the title injection
            # html_content = html_content.replace(
            #     '<body>',
            #     f'<body><h2 style="text-align: center;">{title}</h2>'
            # )

            # Remove the border and shadow from the overall frame
            html_content = html_content.replace(
                '<div id="mynetwork"',
                '<div id="mynetwork" style="border: none; box-shadow: none;"'
            )

            # Write the modified HTML back to the file
            with open(tmp_file.name, 'w') as f:
                f.write(html_content)

            return tmp_file.name

    except Exception as e:
        logging.error(f"Failed to generate network HTML: {e}")
        return None

def aggregate_hierarchy_data(df, output_file_path='Datasources/aggregated_hierarchy.csv'):
    # Identify columns that contain 'ID', 'Name', and 'SP_Global'
    id_columns = [col for col in df.columns if 'ID' in col]
    name_columns = [col for col in df.columns if 'Name' in col]
    spglobal_columns = [col for col in df.columns if 'SP_Global' in col]

    # Ensure that the columns do not contain unhashable types (like lists)
    ids = pd.concat([df[col].astype(str) for col in id_columns]).dropna().unique()
    names = pd.concat([df[col].astype(str) for col in name_columns]).dropna().unique()
    spglobal = pd.concat([df[col].astype(str) for col in spglobal_columns]).dropna().unique()

    # Create a new DataFrame with the gathered data
    new_data_df = pd.DataFrame({
        "ID": pd.Series(ids).reset_index(drop=True),
        "Name": pd.Series(names).reset_index(drop=True),
        "SP_Global": pd.Series(spglobal).reset_index(drop=True)
    })

    # Check if the output file already exists
    if os.path.exists(output_file_path):
        # Load the existing data
        existing_df = pd.read_csv(output_file_path)
        # Concatenate the new data with the existing data
        combined_df = pd.concat([existing_df, new_data_df])
    else:
        # If the file does not exist, the combined DataFrame is just the new data
        combined_df = new_data_df
        

    # Remove duplicates across the entire DataFrame based on 'ID', 'Name', and 'SP_Global'
    final_df = combined_df.drop_duplicates(subset=["ID", "Name", "SP_Global"]).reset_index(drop=True)

    # Save the final DataFrame to a CSV file, overwriting the existing file
    final_df.to_csv(output_file_path, index=False)

# Process a subset of the LEI codes
async def main(lei_list):
    try:
        all_hierarchies = await process_leis(lei_list)

        # Flattening the hierarchical data
        flat_data = flatten_hierarchy([all_hierarchies[lei] for lei in lei_list if lei in all_hierarchies])

        # Find the longest row to set DataFrame column width
        max_length = max(len(row) for row in flat_data)

        # Create a DataFrame with adequate columns
        columns = ['Level_{}_{}'.format((i//3)+1, x) for i, x in enumerate(['ID', 'Name', 'SP_Global'] * (max_length//3))]
        final_df = pd.DataFrame(flat_data, columns=columns)

        # Return the DataFrame and the hierarchical data
        return final_df, all_hierarchies

    except Exception as e:
        logging.error(f"Error in main processing: {e}")
        # Ensure that None is returned if an error occurs
        return None, None
