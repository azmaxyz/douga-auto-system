# Use the official lightweight Python image.
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
        && rm -rf /var/lib/apt/lists/*

        # Set the working directory
        WORKDIR /app

        # Copy requirements and install Python dependencies
        COPY requirements.txt .
        RUN pip install --no-cache-dir -r requirements.txt

        # Copy the source code and watermark image
        COPY main.py .
        COPY watermark.png .

        # Expose the port the app runs on
        EXPOSE 8080

        # Run the application
        CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]
