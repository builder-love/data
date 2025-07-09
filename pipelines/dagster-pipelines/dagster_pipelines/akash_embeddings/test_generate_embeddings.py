import os
import pickle
import pandas as pd
from sentence_transformers import SentenceTransformer
from google.cloud import storage
import logging
import numpy as np
import base64
from dotenv import load_dotenv

model = SentenceTransformer("BAAI/bge-m3")

sentences = [
    "That is a happy person",
    "That is a happy dog",
    "That is a very happy person",
    "Today is a sunny day"
]
embeddings = model.encode(sentences)

similarities = model.similarity(embeddings, embeddings)
print(similarities.shape)
# [4, 4]