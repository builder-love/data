from sentence_transformers import SentenceTransformer

def test_generate_embeddings():
    model_name = 'BAAI/bge-m3'
        
    # Load Model and Generate Embeddings
    print(f"Loading SentenceTransformer model: {model_name}")
    model = SentenceTransformer(model_name)
    print("Model loaded.")

    corpus = 'developer tooling repo for building a smart contract'
    
    embedding = model.encode(corpus)

    # Convert the numpy array to the required string format
    embedding_str = str(embedding.tolist())

    print("--- Your SQL-ready embedding string ---")
    print(embedding_str)
    print("--- End of string ---")

# Run the function
test_generate_embeddings()