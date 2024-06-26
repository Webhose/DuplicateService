from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from utils import *
import uvicorn

ADDRESS = "0.0.0.0"
PORT = 9039

app = FastAPI()
batch_size = 10000
batch_counter = 0
lsh_cache_dict = dict()
counter = 0


@app.on_event("startup")
async def startup_event():
    global lsh_cache_dict
    lsh_cache_dict = {
        "english": get_lsh_from_redis(lsh_key="english:lsh_index"),
        # if you have more languages, add them here
    }


@app.post("/is_duplicate")
async def is_duplicate(request: Request):
    global batch_counter, counter, lsh_cache_dict
    try:
        # get parameters from request
        json_data = await request.json()
        language = json_data.get('language')
        lsh_cache = lsh_cache_dict.get(language)
        status = await run_lsh_check(content=json_data.get('content'), language=language, lsh_cache=lsh_cache,
                                     article_domain=json_data.get('domain'), article_id=json_data.get('article_id'))

        # Update request counter and add to pending updates
        batch_counter += 1

        # Check if it's time to update Redis
        if batch_counter >= batch_size:
            logger.info(f"Updating LSH in Redis... {counter}")
            await update_lsh_in_redis_batch(lsh_cache, language)
            batch_counter = 0
            counter += 1

        return JSONResponse(content={"status": status})
    except Exception as e:
        logger.critical(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.get('/health_check')
async def health_endpoint():
    return {"message": "I'm OK"}


if __name__ == "__main__":
    uvicorn.run(app, host=ADDRESS, port=PORT)
