# Base image with Python and CUDA support
FROM pytorch/pytorch:2.6.0-cuda12.6-cudnn9-runtime

# Set the working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .

# Install the required libraries
RUN pip install --no-cache-dir -r requirements.txt

# Copy the worker script into the container
COPY generate_embeddings.py .

# Command to run when the container starts
CMD ["python", "generate_embeddings.py"]