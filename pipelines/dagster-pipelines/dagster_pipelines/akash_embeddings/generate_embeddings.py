import os
import pickle
import pandas as pd
from sentence_transformers import SentenceTransformer
from google.cloud import storage
import logging
import numpy as np

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """
    Main function to generate embeddings.
    Reads configuration from environment variables, downloads data from GCS,
    computes embeddings using a SentenceTransformer model on a GPU,
    and uploads the results back to GCS.
    """
    logging.info("Starting embedding generation process.")

    # Configuration from Environment Variables
    gcs_bucket_name = os.environ.get("GCS_BUCKET")
    input_parquet_path = os.environ.get("INPUT_PARQUET_PATH")
    output_pickle_path = os.environ.get("OUTPUT_PICKLE_PATH")
    model_name = 'all-mpnet-base-v2'

    if not all([gcs_bucket_name, input_parquet_path, output_pickle_path]):
        logging.error("Missing one or more environment variables: GCS_BUCKET, INPUT_PARQUET_PATH, OUTPUT_PICKLE_PATH")
        return

    # Download Data from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)
    
    input_blob = bucket.blob(input_parquet_path)
    local_input_path = "/tmp/input_data.parquet"
    
    logging.info(f"Downloading {input_parquet_path} from GCS bucket {gcs_bucket_name}...")
    input_blob.download_to_filename(local_input_path)
    logging.info("Download complete.")

    df = pd.read_parquet(local_input_path)
    logging.info(f"Loaded {len(df)} records from Parquet file.")
    
    if 'corpus_text' not in df.columns:
        logging.error("Parquet file must contain a 'corpus_text' column.")
        return
        
    # Load Model and Generate Embeddings
    logging.info(f"Loading SentenceTransformer model: {model_name}")
    model = SentenceTransformer(model_name)
    logging.info("Model loaded.")

    corpus = df['corpus_text'].tolist()
    repo = df['repo'].tolist()
    
    # Process in batches with explicit logging
    batch_size = 128 
    num_batches = (len(corpus) + batch_size - 1) // batch_size
    all_embeddings = []

    logging.info(f"Starting embedding encoding for {len(corpus)} sentences in {num_batches} batches of size {batch_size}.")

    for i in range(0, len(corpus), batch_size):
        batch_corpus = corpus[i:i+batch_size]
        batch_num = (i // batch_size) + 1
        logging.info(f"Processing batch {batch_num}/{num_batches}...")
        # model.encode returns a numpy array
        batch_embeddings = model.encode(batch_corpus)
        all_embeddings.extend(batch_embeddings)

    embeddings = np.array(all_embeddings)
    logging.info("Embeddings generated successfully.")

    # --- 4. Prepare and Upload Results ---
    results = {repo[i]: embeddings[i] for i in range(len(repo))}
    
    local_output_path = "/tmp/repo_embeddings.pkl"
    with open(local_output_path, 'wb') as f_out:
        pickle.dump(results, f_out)
        
    output_blob = bucket.blob(output_pickle_path)
    logging.info(f"Uploading results to {output_pickle_path} in GCS bucket {gcs_bucket_name}...")
    output_blob.upload_from_filename(local_output_path)
    logging.info("Upload complete. Process finished.")

if __name__ == "__main__":
    main()