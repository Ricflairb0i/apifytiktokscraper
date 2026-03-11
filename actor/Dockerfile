FROM apify/actor-python:3.11

# Copy just requirements.txt first to leverage Docker cache
COPY requirements.txt ./

# Install Python requirements
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the workspace
COPY . ./

# Run the app
CMD ["python3", "main.py"]
