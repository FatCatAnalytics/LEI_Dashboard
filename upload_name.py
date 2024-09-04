import asyncio
import aiohttp
import pandas as pd
import logging
from transformers import BertTokenizer, BertModel
import torch

# Setup logging
logging.basicConfig(level=logging.INFO)

# Initialize BERT model and tokenizer
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
model = BertModel.from_pretrained('bert-base-uncased')


# Asynchronous function to fetch data from the API
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


# Function to get BERT embeddings using PyTorch only
def get_bert_embedding(text):
    inputs = tokenizer(text, return_tensors='pt', max_length=512, truncation=True, padding=True)
    outputs = model(**inputs)
    embeddings = outputs.last_hidden_state.mean(dim=1)
    return embeddings


# Function to calculate cosine similarity using PyTorch
def cosine_similarity_torch(vec1, vec2):
    return torch.nn.functional.cosine_similarity(vec1, vec2).item()


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

                # Calculate BERT similarity score between the query name and the matched entity name
                query_embedding = get_bert_embedding(name)
                entity_embedding = get_bert_embedding(entity_name)
                similarity = cosine_similarity_torch(query_embedding, entity_embedding)

                records.append({
                    "Query Name": name,
                    "Matched Entity Name": entity_name,
                    "LEI": lei,
                    "Address": ', '.join(address),
                    "City": city,
                    "Country": country,
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
                "Similarity Score": 0
            })

    df = pd.DataFrame(records)
    return df


# Main function to fetch and process company data
def main(names):
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(fetch_all_companies(names))
    df = process_results(results)
    return df


# Example usage
if __name__ == "__main__":
    company_names = ["Apple", "Google", "Microsoft"]
    result_df = main(company_names)

    # Sort the DataFrame by similarity score in descending order
    result_df_sorted = result_df.sort_values(by="Similarity Score", ascending=False)

    # Print the sorted results
    print(result_df_sorted)
