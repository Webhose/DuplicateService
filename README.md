# Duplicate Detection Service

## Overview
This service provides a duplicate document detection functionality based on MinHash and Locality-Sensitive Hashing (LSH). It exposes an API endpoint `/is_duplicate` to check if a given document is a duplicate of any previously processed documents. The service uses FastAPI, a modern, fast web framework for building APIs with Python 3.7+.

## Requirements
- Python 3.7 or higher
- FastAPI
- uvicorn
- Redis (for storing LSH index)

## Installation
1. Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

2. Ensure that Redis is installed and running.

3. Run the service:
   ```bash
   python3 server.py
   ```

## API Endpoints

### `POST /is_duplicate`
Check if a document is a duplicate of any previously processed documents.

#### Request
- **Method:** `POST`
- **Body:**
  ```json
  {
    "content": "The content of the document",
    "domain": "example.com",
    "language": "english",
    "article_id": "optional-article-id"
  }
  ```

#### Response
- **Success:**
  ```json
  {
    "is_duplicate_document": true|false
  }
  ```
- **Error:**
  - Status Code: 500 Internal Server Error
  - Body:
    ```json
    {
      "detail": "Internal Server Error: Error message"
    }
    ```

### `GET /health_check`
Health check endpoint to verify the service status.

#### Response
- **Success:**
  ```json
  {
    "message": "I'm OK"
  }
  ```

## Configuration
- The service runs on `0.0.0.0` at port `9037` by default. You can modify the `ADDRESS` and `PORT` variables in the code to change the service address and port.
- The Redis server runs on `localhost` at port `6379` by default. You can modify the `REDIS_HOST` and `REDIS_PORT` variables in the code to change the Redis server address and port.
