import os
import gc
import logging
import psutil

import gcsfs
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from google.cloud import storage

# --- Basic Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Helper Functions ---
def log_memory_usage(message: str):
    """Logs the current RSS memory usage of the process."""
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / (1024 * 1024)
    logging.info(f"{message} - Memory Usage: {memory_mb:.2f} MB")

def sanitize_embedding_list(embedding_list, original_dim):
    """
    Takes a list of embeddings, validates each one, and returns a clean list.
    """
    if not isinstance(embedding_list, list) or not embedding_list:
        return None
    
    valid_embeddings = []
    for emb in embedding_list:
        if emb is None:
            continue
        if isinstance(emb, list):
            emb = np.array(emb, dtype=np.float32)
        if isinstance(emb, np.ndarray) and emb.shape[0] == original_dim:
            valid_embeddings.append(emb)
    return valid_embeddings if valid_embeddings else None

def main():
    """Main function to read and debug Parquet files from GCS."""
    log_memory_usage("Start of script execution")

    # --- Configuration ---
    GCS_BUCKET_NAME = "bl-repo-corpus-public"
    GCS_PARQUET_FOLDER_PATH = "embeddings_data/akash-qwen-checkpoints/20250828-002628"
    ORIGINAL_DIM = 2560
    PARQUET_PROCESSING_CHUNK_SIZE = 100

    # --- Initialize GCS Client ---
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    gcs_fs = gcsfs.GCSFileSystem()

    all_processed_chunks = []
    try:
        # 1. List Parquet files from GCS
        logging.info(f"Listing files from gs://{GCS_BUCKET_NAME}/{GCS_PARQUET_FOLDER_PATH}...")
        parquet_blobs = list(bucket.list_blobs(prefix=GCS_PARQUET_FOLDER_PATH))
        parquet_blobs = [b for b in parquet_blobs if b.name.endswith('.parquet')]

        if not parquet_blobs:
            logging.warning("No Parquet files found in GCS path. Exiting.")
            return

        logging.info(f"Found {len(parquet_blobs)} Parquet files. Starting processing...")

        # 2. Process each Parquet file
        # just sample the first file
        for i, blob in enumerate(parquet_blobs[:1]):
            logging.info(f"--- Processing File {i+1}/{len(parquet_blobs)}: {blob.name} ---")
            gcs_file_path = f"gs://{GCS_BUCKET_NAME}/{blob.name}"
            
            try:
                parquet_file = pq.ParquetFile(gcs_file_path, filesystem=gcs_fs)

                # print parquet file meta data and schema
                logging.info(f"Parquet file metadata: {parquet_file.metadata}")
                logging.info(f"Parquet file schema: {parquet_file.schema}")
                
                for batch_num, record_batch in enumerate(parquet_file.iter_batches(batch_size=PARQUET_PROCESSING_CHUNK_SIZE)):
                    log_memory_usage(f"File {i+1}, Batch {batch_num}")
                    
                    df_chunk = record_batch.to_pandas()
                    if df_chunk.empty: continue
                    
                    # Process embeddings
                    df_chunk['corpus_embedding'] = df_chunk['corpus_embedding'].apply(
                        lambda lst: sanitize_embedding_list(lst, ORIGINAL_DIM)
                    )
                    df_chunk.dropna(subset=['corpus_embedding'], inplace=True)
                    if df_chunk.empty: continue

                    df_chunk['corpus_embedding'] = df_chunk['corpus_embedding'].apply(
                        lambda valid_list: np.mean(valid_list, axis=0)
                    )
                    
                    # --- DEBUGGING OUTPUT ---
                    print(f"\n--- DEBUG: Processed Batch {batch_num} from File {i+1} ---")
                    print(f"Shape of this processed chunk: {df_chunk.shape}")
                    print(df_chunk.head())
                    print("-" * 50)
                    
                    all_processed_chunks.append(df_chunk)
                    
                    del df_chunk; gc.collect()
            
            except Exception as e:
                logging.error(f"Error processing file {blob.name}: {e}")
                # We continue to the next file in case of an error with one
                continue
            finally:
                log_memory_usage(f"After processing file {i+1}")

        # 3. Final Summary
        if not all_processed_chunks:
            logging.warning("No data was processed.")
            return
            
        logging.info("Combining all processed chunks into a final DataFrame...")
        final_df = pd.concat(all_processed_chunks, ignore_index=True)
        
        print("\n\n" + "="*25 + " FINAL SUMMARY " + "="*25)
        print(f"Total number of processed repos: {len(final_df)}")
        print("\nDataFrame Info:")
        final_df.info()
        print("\nFirst 5 rows of final combined data:")
        print(final_df.head())
        print("\nExample of a processed embedding vector:")
        print(final_df['corpus_embedding'].iloc[0])
        print("="*67)


    except Exception as e:
        logging.error(f"A critical error occurred: {e}")
        raise
    finally:
        log_memory_usage("End of script execution")

if __name__ == "__main__":
    main()