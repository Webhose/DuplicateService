from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from utils import *
import asyncio
import uvicorn
from contextlib import asynccontextmanager

ADDRESS = "0.0.0.0"
PORT = 9039
batch_size = 5000
batch_counter = 0
lsh_cache_dict = {}
counter = 0

cleanup_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global lsh_cache_dict, cleanup_task

    logger.info("Starting up...")
    lsh_cache_dict = {
        "english": get_lsh_from_redis(lsh_key="english:lsh_index"),
        # add more languages if necessary
    }
    logger.info("Initialized LSH cache with TTL for supported languages.")

    # Start the background cleanup task
    # cleanup_task = asyncio.create_task(background_cleanup_task())

    yield  # Control is returned to FastAPI here

    logger.info("Shutting down...")
    await save_lsh_to_redis(lsh_cache_dict)
    logger.info("Saved LSH cache to Redis.")

    # # Cancel the cleanup task
    # if cleanup_task:
    #     cleanup_task.cancel()
    #     try:
    #         await cleanup_task
    #     except asyncio.CancelledError:
    #         logger.info("Cleanup task was cancelled")


app = FastAPI(lifespan=lifespan)


# async def background_cleanup_task():
#     while True:
#         logger.info("Running background cleanup task...")
#         metrics.count(Consts.BACKGROUND_CLEANUP_TASK_TOTAL)
#         for language, lsh_cache in lsh_cache_dict.items():
#             lsh_cache.cleanup_expired_keys()
#         # Sleep for 1 hour before the next cleanup
#         await asyncio.sleep(10)


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
        return JSONResponse(content={"status": status})
    except ValueError as e:
        return JSONResponse(content={"status": "duplicate_keys"})
    except Exception as e:
        logger.critical(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.get('/health_check')
async def health_endpoint():
    return {"message": "I'm OK"}


if __name__ == "__main__":
    uvicorn.run(app, host=ADDRESS, port=PORT)
