# Use a base image with Python and CUDA support for the GPU
FROM pytorch/pytorch:1.13.1-cuda11.6-cudnn8-runtime

# Set the working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the worker script into the container
COPY generate_embeddings.py .

# Command to run when the container starts
CMD ["python", "generate_embeddings.py"]