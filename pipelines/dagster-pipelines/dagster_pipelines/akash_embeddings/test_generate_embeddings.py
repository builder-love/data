import os
import pickle
import pandas as pd
from sentence_transformers import SentenceTransformer
from google.cloud import storage
import logging
import numpy as np
import base64
from dotenv import load_dotenv

def main():
    """
    Main function to generate embeddings.
    Reads configuration from environment variables, downloads data from GCS,
    computes embeddings using a SentenceTransformer model on a GPU,
    and uploads the results back to GCS.
    """
    print("Starting embedding generation process.")

    # Get the base64 encoded credentials string from the environment
    creds_base64 = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_BASE64")
    
    if creds_base64:
        # Decode the base64 string
        creds_json_str = base64.b64decode(creds_base64).decode('utf-8')
        
        # Write the decoded JSON to a temporary file
        creds_file_path = "/tmp/gcs_creds.json"
        with open(creds_file_path, "w") as f:
            f.write(creds_json_str)
            
        # Set the environment variable to the path of the temporary file
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = creds_file_path
        print("Successfully configured Google Cloud credentials from environment variable.")
    else:
        print("GOOGLE_APPLICATION_CREDENTIALS_BASE64 environment variable is not set.")
        return

    # Now that credentials are set, the rest of your script can run
    load_dotenv()

    # Configuration from Environment Variables
    gcs_bucket_name = "bl-repo-corpus-public"
    input_parquet_path = "embeddings_data/7b83aedd-ddbf-4787-90e5-08c33a67fa4d.parquet"
    output_pickle_path = "embeddings_data/repo_embeddings.pkl"
    model_name = 'all-mpnet-base-v2'
    # Download Data from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)
    
    input_blob = bucket.blob(input_parquet_path)
    local_input_path = "/tmp/input_data.parquet"
    
    print(f"Downloading {input_parquet_path} from GCS bucket {gcs_bucket_name}...")
    input_blob.download_to_filename(local_input_path)
    print("Download complete.")

    df = pd.read_parquet(local_input_path)
    # df = df.head(30)
    print(f"Loaded {len(df)} records from Parquet file.")
    
    if 'corpus_text' not in df.columns:
        print("Parquet file must contain a 'corpus_text' column.")
        return
        
    # Load Model and Generate Embeddings
    print(f"Loading SentenceTransformer model: {model_name}")
    model = SentenceTransformer(model_name)
    print("Model loaded.")

    corpus = df['corpus_text'].tolist()
    repo = df['repo'].tolist()
    
    # Process in batches with explicit logging
    batch_size = 128
    num_batches = (len(corpus) + batch_size - 1) // batch_size
    all_embeddings = []

    print(f"Starting embedding encoding for {len(corpus)} sentences in {num_batches} batches of size {batch_size}.")

    for i in range(0, len(corpus), batch_size):
        batch_corpus = corpus[i:i+batch_size]
        batch_num = (i // batch_size) + 1
        print(f"Processing batch {batch_num}/{num_batches}...")
        # model.encode returns a numpy array
        batch_embeddings = model.encode(batch_corpus)
        all_embeddings.extend(batch_embeddings)

    embeddings = np.array(all_embeddings)
    print("Embeddings generated successfully.")

    # --- 4. Prepare and Upload Results ---
    results = {repo[i]: embeddings[i] for i in range(len(repo))}
    
    local_output_path = "/tmp/repo_embeddings.pkl"
    with open(local_output_path, 'wb') as f_out:
        pickle.dump(results, f_out)
        
    output_blob = bucket.blob(output_pickle_path)
    print(f"Uploading results to {output_pickle_path} in GCS bucket {gcs_bucket_name}...")
    output_blob.upload_from_filename(local_output_path)
    print("Upload complete. Process finished.")

if __name__ == "__main__":
    main()