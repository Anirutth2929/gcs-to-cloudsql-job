# Use lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy dependency file first (better caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .

# Run the job
CMD ["python", "main.py"]
