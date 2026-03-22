import os
import random
import pdb
import argparse
import requests
import pandas as pd
from tqdm import tqdm
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore

# Define your Semantic Scholar API url
SEMANTIC_SCHOLAR_API_URL = "https://api.semanticscholar.org/graph/v1"

# Define the fields to be retrieved
fields = "url,year,citationCount,tldr"

def append_to_csv(df, filepath):
    """Append dataframe to existing CSV without writing headers"""
    df.to_csv(filepath, mode='a', header=False, index=False)

def append_to_json(failed_ids, filepath):
    """Append new failed IDs to existing JSON file"""
    existing_failed_ids = []
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            existing_failed_ids = json.load(f)
    
    all_failed_ids = existing_failed_ids + failed_ids
    with open(filepath, 'w') as f:
        json.dump(all_failed_ids, f)

def save_citations_to_jsonl(citations, filename="citations.jsonl"):
    with open(filename, 'a') as f:
        for citation in citations:
            if len(citation) != 0:
                f.write(json.dumps(citation) + '\n')


def load_processed_paper_ids(filename="citations.jsonl"):
    if not os.path.exists(filename):
        return set()

    processed_paper_ids = set()
    with open(filename, 'r') as f:
        for line in f:
            citation = json.loads(line)
            processed_paper_ids.add(citation['citedPaperId'])

    return processed_paper_ids


# Function to fetch papers for a given year with pagination
def fetch_paper_ids(request_ids, max_retries=8):
    failed_response = "failed"
    url = f"{SEMANTIC_SCHOLAR_API_URL}/paper/batch/"
    params = {'fields': fields}
    json_ = {"ids": request_ids}

    backoff_base = 2

    for attempt in range(0,max_retries):
        try:
            response = requests.post(url,
                                     params=params,
                                     json=json_,
                                    #  headers={'x-api-key': SEMANTIC_SCHOLAR_API_KEY}
                                     ).json()
            
            if not ('message' in response or 'error' in response):
                return "", response
            
            if 'error' in response and response['error'] == 'No valid paper ids given':
                return failed_response, "_"
            
            if ('message' in response or 'error' in response):
                wait_time = backoff_base ** attempt
                print(f"Error in response (attempt {attempt + 1}): {response}. Retrying in {wait_time} seconds.")
                time.sleep(wait_time)
            # if len(response)!=500:
            #     print(f"Received {len(response)} results for {len(request_ids)} IDs, expected 500.")
        except Exception as e:
            print(f"Exception fetching papers (attempt {attempt + 1}): {e}")
            wait_time = backoff_base ** attempt
            time.sleep(wait_time)

    # time.sleep(60)  # Wait 60 seconds between requests

    return failed_response, response


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--new_data", action="store_true",
                       help="Process new data and append to existing files")
    args = parser.parse_args()

    args_new_data = args.new_data

    # Read arxiv paper ids
    input_file = "data/arxiv_data.pkl"
    df = pd.read_pickle(input_file)

    # Filter out already processed papers when appending new data
    if args_new_data and os.path.exists("data/arxiv_papers_with_semantic_scholar_ids.csv"):
        existing_df = pd.read_csv("data/arxiv_papers_with_semantic_scholar_ids.csv")
        existing_ids = set(existing_df['id'])
        df = df[~df['id'].isin(existing_ids)]
        print(f'Filtered out {len(existing_ids)} existing papers, processing {len(df)} new papers')

    if args_new_data and len(df) == 0:
        print("No new papers to process. Exiting.")
        return

    df_keys = df['id'].tolist()
    arxiv_ids = df_keys
    print(f'Total arxiv paper to process {len(arxiv_ids)}')

    failed_paper_ids = []
    paper_results = []
    iterator = 500
    for i in tqdm(range(0, len(arxiv_ids), iterator)):
        ids = arxiv_ids[i:i + iterator]
        ids = [f'ARXIV:{id_}' for id_ in ids]
        failed_response, semantic_scholar_results = fetch_paper_ids(ids)
        if failed_response == "failed":
            print(f"Failed to fetch results for IDs: {ids}")
            failed_paper_ids.extend(ids)
            continue

        for df_details, result in zip(df_keys[i:i + iterator], semantic_scholar_results):
            try:
                if result is not None:
                    # Extract tldr text safely
                    if result.get('tldr') is not None and isinstance(result['tldr'], dict):
                        result['tldr'] = result['tldr'].get('text', "No TLDR available")
                    else:
                        result['tldr'] = "No TLDR available"
                    paper_results.append({'id': df_details, **result})
                else:
                    failed_paper_ids.append(df_details)
            except Exception as e:
                failed_paper_ids.append(df_details)

        time.sleep(1) 

    print(f'Total failed paper ids {len(failed_paper_ids)}')

    headers = ['id', 'citationCount', 'year', 'paperId', 'url', 'tldr']
    results_df = pd.DataFrame(paper_results, columns=headers)
    merged_df = pd.merge(results_df, df, on='id', how='inner')

    if args_new_data:
        # Append with headers if file doesn't exist
        file1 = "data/arxiv_papers_with_semantic_scholar_ids.csv"
        file2 = "data/semantic_scholar_paper_details_for_c_code.csv"
        file3 = "data/arxiv_papers_with_no_semantic_scholar_ids.json"

        header_needed1 = not os.path.exists(file1)
        merged_df.to_csv(file1, mode='a', header=header_needed1)

        df_for_c_code = merged_df[['paperId', 'url', 'title', 'year', 'citationCount']]
        header_needed2 = not os.path.exists(file2)
        df_for_c_code.to_csv(file2, mode='a', header=header_needed2, index=False)

        append_to_json(failed_paper_ids, file3)

        print(f'Appended {len(merged_df)} new entries')
    else:
        merged_df.to_csv("data/arxiv_papers_with_semantic_scholar_ids.csv")
        print(f'Total entries {len(merged_df)}')

        df_for_c_code = merged_df[['paperId', 'url', 'title', 'year', 'citationCount']]
        df_for_c_code.to_csv("data/semantic_scholar_paper_details_for_c_code.csv", index=False)

        with open("data/arxiv_papers_with_no_semantic_scholar_ids.json", "w") as f:
            json.dump(failed_paper_ids, f)


if __name__ == "__main__":
    main()
