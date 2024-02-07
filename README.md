# Duplicate Detection Service

## Overview
This service employs MinHash and Locality-Sensitive Hashing (LSH) to identify duplicate documents. The functionality is exposed through the API endpoint `/is_duplicate`, enabling users to check if a given document duplicates any previously processed documents. The service is built on FastAPI, a modern and rapid web framework designed for creating APIs with Python 3.7+.

## Setup

1. **Create Virtual Environment:**
   ```bash
   python -m venv venv
   ```

2. **Activate Virtual Environment:**
     ```bash
     source venv/bin/activate
     ```

3. **Install Requirements:**
   ```bash
   pip install -r requirements.txt
   ```

To run the Docker container locally:

```bash
docker run -d --name duplicateservice -v /etc/hosts:/etc/hosts -v /home/omgili/log/:/home/omgili/log/ -p 9039:9039 --hostname $(hostname) webzio/duplicateservice:latest_tag
```

Replace `latest_tag` with the specific tag you want to use for the Docker image.