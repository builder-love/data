import gc

import gcsfs
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from google.cloud import storage

def sanitize_embedding_list(embedding_list, original_dim):
    """
    Takes a list of embedding vectors, validates each one, and returns a
    clean list of NumPy arrays.
    """
    if not isinstance(embedding_list, list) or not embedding_list:
        print(f"Embedding list is not a list or is empty. Embedding is type: {type(embedding_list)}. Returning None.")
        return None

    valid_embeddings = []
    # The `embedding_list` is the outer list, e.g., [[...vector_1...], [...vector_2...]]
    # `emb` will be the inner list, e.g., [...vector_1...]
    for emb in embedding_list:
        if emb is None:
            print("Embedding is None. Skipping.")
            continue
            
        # Ensure the item is a NumPy array for the shape check
        if isinstance(emb, list):
            print(f"Embedding is a list. Converting to NumPy array.")
            emb = np.array(emb, dtype=np.float32)
        
        # Check if it's a 1D array of the correct length
        if isinstance(emb, np.ndarray) and len(emb.shape) == 1 and emb.shape[0] == original_dim:
            print(f"Embedding is a NumPy array and has the correct dimension. Adding to valid embeddings.")
            valid_embeddings.append(emb)
        else:
            # This will help debug if some vectors have the wrong dimension
            shape = emb.shape if hasattr(emb, 'shape') else 'N/A'
            print(f"Skipping an invalid embedding with shape: {shape}")

    # Return the list of valid NumPy arrays, or None if the list is empty
    return valid_embeddings if valid_embeddings else None

def main():
    """Main function to read and debug Parquet files from GCS."""

    # --- Configuration ---
    GCS_BUCKET_NAME = "bl-repo-corpus-public"
    GCS_PARQUET_FOLDER_PATH = "embeddings_data/akash-qwen-checkpoints/20250828-002628"
    ORIGINAL_DIM = 2560
    PARQUET_PROCESSING_CHUNK_SIZE = 1000

    # --- Initialize GCS Client ---
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    gcs_fs = gcsfs.GCSFileSystem()

    all_processed_chunks = []
    try:
        # 1. List Parquet files from GCS
        print(f"Listing files from gs://{GCS_BUCKET_NAME}/{GCS_PARQUET_FOLDER_PATH}...")
        parquet_blobs = list(bucket.list_blobs(prefix=GCS_PARQUET_FOLDER_PATH))
        parquet_blobs = [b for b in parquet_blobs if b.name.endswith('.parquet')]

        if not parquet_blobs:
            print("No Parquet files found in GCS path. Exiting.")
            return

        print(f"Found {len(parquet_blobs)} Parquet files. Starting processing...")

        # 2. Process each Parquet file
        # just sample the first file
        for i, blob in enumerate(parquet_blobs[:1]):
            gcs_file_path = f"gs://{GCS_BUCKET_NAME}/{blob.name}"
            
            try:
                parquet_file = pq.ParquetFile(gcs_file_path, filesystem=gcs_fs)

                # print parquet file meta data and schema
                print(f"Parquet file metadata: {parquet_file.metadata}")
                print(f"Parquet file schema: {parquet_file.schema}")
                
                for batch_num, record_batch in enumerate(parquet_file.iter_batches(batch_size=PARQUET_PROCESSING_CHUNK_SIZE)):
                    
                    df_chunk = record_batch.to_pandas()
                    if df_chunk.empty: 
                        print(f"to_pandas() operation on parquet record_batch resulted in an empty dataframe for batch {batch_num} in file {i+1}. Skipping.")
                        continue

                    # force print the complete output of the first row in the dataframe
                    print(f"df_chunk has {len(df_chunk)} rows")
                    print(f"Complete view of dataframe row 1: {df_chunk.iloc[0]}")
                    
                    # Process embeddings
                    print(f"Sanitizing embeddings for batch {batch_num} in file {i+1}")
                    df_chunk['corpus_embedding'] = df_chunk['corpus_embedding'].apply(
                        lambda lst: sanitize_embedding_list(lst, ORIGINAL_DIM)
                    )
                    print(f"Dropping rows where the embedding list is now empty or None for batch {batch_num} in file {i+1}")
                    df_chunk.dropna(subset=['corpus_embedding'], inplace=True)

                    if df_chunk.empty:
                        print(f"The aggregated dataframe for batch {batch_num} in file {i+1} has no records. Skipping.")
                        continue

                    print(f"Applying numpy average to the clean list for batch {batch_num} in file {i+1}")
                    df_chunk['corpus_embedding'] = df_chunk['corpus_embedding'].apply(
                        lambda valid_list: np.mean(valid_list, axis=0)
                    )
                    
                    # --- DEBUGGING OUTPUT ---
                    print(f"\n--- DEBUG: Processed Batch {batch_num} from File {i+1} ---")
                    print(f"Shape of this processed chunk: {df_chunk.shape}")
                    print(df_chunk.head())
                    print("-" * 50)
                    
                    del df_chunk; gc.collect()
            
            except Exception as e:
                print(f"Error processing file {blob.name}: {e}")
                # We continue to the next file in case of an error with one
                continue

        # 3. Final Summary
        if not all_processed_chunks:
            print("No data was processed.")
            return


    except Exception as e:
        print(f"A critical error occurred: {e}")
        raise

if __name__ == "__main__":
    main()