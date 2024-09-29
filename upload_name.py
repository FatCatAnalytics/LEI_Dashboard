import asyncio
import aiohttp
import pandas as pd
import logging
import re
import torch
import streamlit as st
from sentence_transformers import SentenceTransformer, util

# Setup logging
logging.basicConfig(level=logging.INFO)

# Load the pre-trained SentenceTransformer model and force it to run on CPU
device = torch.device('cpu')
model = SentenceTransformer('all-MiniLM-L6-v2', device=device)


# Preprocess company name to remove unnecessary suffixes and special characters
def preprocess_company_name(name):
    name = name.lower()
    suffixes = [' inc', ' corp', ' corporation', ' ltd', ' limited', ' llc', ' llp', ' company', ' co', ' group']
    for suffix in suffixes:
        name = name.replace(suffix, '')
    name = re.sub(r'[^a-z0-9\s]', '', name)
    return ' '.join(name.split())


# Asynchronous function to fetch data from the GLEIF API
async def fetch_company_data(session, name):
    url = f"https://api.gleif.org/api/v1/lei-records?page[size]=10&page[number]=1&filter[entity.names]={name}"
    try:
        async with session.get(url) as response:
            logging.info(f"Fetching data for {name} - Status: {response.status}")
            response.raise_for_status()
            data = await response.json()
            return name, data
    except aiohttp.ClientError as e:
        logging.error(f"Error fetching data for {name}: {e}")
        return name, None


# Asynchronous function to process a list of company names
async def fetch_all_companies(names):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_company_data(session, name) for name in names]
        results = await asyncio.gather(*tasks)
        return results


# Function to get embeddings using SentenceTransformer
def get_embedding(text):
    preprocessed_text = preprocess_company_name(text)
    return model.encode(preprocessed_text, convert_to_tensor=True, device=device)


# Function to calculate cosine similarity using SentenceTransformer utilities
def cosine_similarity(embedding1, embedding2):
    return util.pytorch_cos_sim(embedding1, embedding2).item()


# Function to process and flatten the fetched data into a DataFrame
def process_results(results):
    records = []
    for name, data in results:
        if data and 'data' in data:
            for entry in data['data']:
                attributes = entry.get('attributes', {})
                entity_name = attributes.get('entity', {}).get('legalName', {}).get('name', 'N/A')
                lei = attributes.get('lei', 'N/A')
                address = attributes.get('entity', {}).get('legalAddress', {}).get('addressLines', ['N/A'])
                city = attributes.get('entity', {}).get('legalAddress', {}).get('city', 'N/A')
                country = attributes.get('entity', {}).get('legalAddress', {}).get('country', 'N/A')
                legal_form = attributes.get('entity', {}).get('legalForm', {}).get('abbreviation', 'N/A')

                # Calculate similarity score between the query name and the matched entity name
                query_embedding = get_embedding(name)
                entity_embedding = get_embedding(entity_name)
                similarity = cosine_similarity(query_embedding, entity_embedding)

                # Refine the score based on whether the entity is a corporation and is based in the expected country
                keywords = ["corporation", "inc", "limited", "company"]
                if any(keyword in entity_name.lower() for keyword in keywords):
                    similarity *= 1.5  # Give a boost to entities with legal identifiers like Corporation or Inc.

                # Further boost the score if the entity is based in the United States
                if country.lower() in ["us", "united states"]:
                    similarity *= 1.2  # Higher boost for entities based in the U.S.

                # Include legal form in the records
                records.append({
                    "Query Name": name,
                    "Matched Entity Name": entity_name,
                    "LEI": lei,
                    "Address": ', '.join(address),
                    "City": city,
                    "Country": country,
                    "Legal Form": legal_form,
                    "Similarity Score": similarity
                })
        else:
            logging.warning(f"No data found for {name}")
            records.append({
                "Query Name": name,
                "Matched Entity Name": "N/A",
                "LEI": "N/A",
                "Address": "N/A",
                "City": "N/A",
                "Country": "N/A",
                "Legal Form": "N/A",
                "Similarity Score": 0
            })

    df = pd.DataFrame(records)
    return df


# Input area for company names
company_names_input = st.text_input("Enter company names split by comma")

if st.button("Fetch and Match"):
    # Split input into list of names
    company_names = company_names_input.split(",") if company_names_input else []

    # Run the main function asynchronously and get results
    if company_names:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(fetch_all_companies(company_names))

        # Process and display the results in a DataFrame
        df = process_results(results)

        # Sort the DataFrame by similarity score in descending order
        df_sorted = df.sort_values(by="Similarity Score", ascending=False)

        # Display the sorted results in Streamlit
        st.write("### Matched Results")
        st.dataframe(df_sorted)
