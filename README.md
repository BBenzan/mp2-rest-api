# MP2: Data Processing REST API

Open `http://127.0.0.1:8000/docs` to use the endpoints.

This project is a FastAPI application that exposes the MP1 data-cleaning pipeline through the following HTTP endpoints.

 -`GET /health` reports the application status and version, and the pytest suite in `tests/` covers the upload, process, and retrieve workflow.

 -`POST /upload` accepts a CSV file, parses it with pandas, and stores it in memory under an integer ID. 

 -`GET /datasets/{dataset_id}` returns up to `limit` rows of a stored dataset along with its total row count. 

 -`POST /datasets/{dataset_id}/process` takes a JSON configuration, validates the dataset's required and numeric columns, then applies the enabled cleaning steps (using the reused MP1 modules in `data_pipeline`) The stored dataset is replaced only after validation and processing both succeed, and the endpoint returns a report showing how many rows each stage removed. 

Notes: 
-Request and response formats are defined with Pydantic models in `src/models.py`

-Invalid uploads, missing datasets, and bad configurations return 400, 404, or 422 responses

-Logger middleware assigns each request an ID, logs its method, path, status code, and duration, and returns the ID in an `X-Request-ID` header. 

## Running the API

```bash
python -m pip install -r requirements.txt
python -m uvicorn main:app
```

## Running the Tests
```bash
python -m pytest tests -v
```

