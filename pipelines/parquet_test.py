import os
import logging
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.decomposition import IncrementalPCA

# --- Basic Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def test_pca_partial_fit():
    """
    Connects to the database, fetches a sample of embeddings, and tests
    the IncrementalPCA using a manual partial_fit loop.
    """
    # --- Configuration ---
    RAW_SCHEMA = "raw_stg"  # Manually set your schema
    AGGREGATED_TABLE = "stg_repo_embeddings_aggregated"
    full_aggregated_table = f"{RAW_SCHEMA}.{AGGREGATED_TABLE}"

    ORIGINAL_DIM = 2560
    REDUCED_DIM = 2000
    PCA_TRAINING_SAMPLE_SIZE = 10000  
    PCA_BATCH_SIZE = 2500

    # --- Database Connection ---
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        logging.error("DATABASE_URL environment variable not set.")
        raise ValueError("DATABASE_URL must be set for the script to run.")
    
    try:
        engine = create_engine(db_url)
        logging.info("Successfully connected to the database.")
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        return

    # 1. Fetch Sample Data from the Staging Table
    logging.info(f"Fetching {PCA_TRAINING_SAMPLE_SIZE} records from {full_aggregated_table}...")
    try:
        with engine.connect() as conn:
            df_sample = pd.read_sql(
                text(f"SELECT corpus_embedding FROM {full_aggregated_table} LIMIT {PCA_TRAINING_SAMPLE_SIZE}"),
                conn
            )
        
        if df_sample.empty:
            logging.error("No data returned from the database. Cannot test PCA.")
            return
        
        logging.info(f"Successfully fetched {len(df_sample)} records.")

    except Exception as e:
        logging.error(f"Failed to fetch data from the database: {e}")
        return

    # 2. Prepare the Training Data Array
    logging.info("Preparing data for PCA training by stacking vectors...")
    try:
        # Using .tolist() first is a robust way to convert a Series of arrays
        training_data = np.array(df_sample['corpus_embedding'].tolist(), dtype=np.float32)
        logging.info(f"Created training data array with shape: {training_data.shape}")
        
        # Verify that the number of features matches what you expect
        if training_data.shape[1] != ORIGINAL_DIM:
            logging.error(
                f"Data feature mismatch! Expected {ORIGINAL_DIM} but got {training_data.shape[1]}"
            )
            return

    except Exception as e:
        logging.error(f"Failed to create NumPy array from DataFrame: {e}")
        return

    # 3. Initialize and Fit the IncrementalPCA Model
    logging.info(
        f"Initializing IncrementalPCA to reduce from {ORIGINAL_DIM} to {REDUCED_DIM} dimensions."
    )
    pca = IncrementalPCA(n_components=REDUCED_DIM, batch_size=PCA_BATCH_SIZE)

    logging.info(f"Fitting model in batches of {PCA_BATCH_SIZE} using partial_fit...")
    try:
        # This is the explicit loop that avoids the .fit() bug
        for i in range(0, training_data.shape[0], PCA_BATCH_SIZE):
            batch = training_data[i:i + PCA_BATCH_SIZE]
            pca.partial_fit(batch)
            if (i // PCA_BATCH_SIZE) % 10 == 0: # Log progress every 10 batches
                 logging.info(f"  Processed batch starting at index {i}...")

        logging.info("Successfully completed partial_fit loop without errors.")

    except Exception as e:
        logging.error(f"An error occurred during the partial_fit loop: {e}")
        return

    # 4. Final Verification
    logging.info(f"PCA model fitting complete.")
    logging.info(f"Number of components seen by PCA model: {pca.n_components_}")
    logging.info(f"Number of features seen by PCA model: {pca.n_features_in_}")
    logging.info(f"Total samples seen by PCA model: {pca.n_samples_seen_}")
    print("\n✅ PCA partial_fit test completed successfully!")


if __name__ == "__main__":
    test_pca_partial_fit()