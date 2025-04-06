# Use the official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy the script and dependencies
COPY main.py .  # Change to load_data.py if that’s your file name
COPY requirements.txt .
COPY cities.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the script
CMD ["python", "main.py"]  # Change to load_data.py if applicable