from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from utils import *
import asyncio
import uvicorn
from contextlib import asynccontextmanager
import logging
import signal

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger()

ADDRESS = "0.0.0.0"
PORT = 9039
lsh_cache_dict = {}


class GracefulShutdown:
    def __init__(self):
        self.shutdown_event = asyncio.Event()

    def __enter__(self):
        signal.signal(signal.SIGTERM, self.handle_exit)
        signal.signal(signal.SIGINT, self.handle_exit)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def handle_exit(self, signum, frame):
        logger.info(f"Received shutdown signal: {signum}")
        self.shutdown_event.set()

    async def wait(self):
        await self.shutdown_event.wait()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global lsh_cache_dict

    graceful_shutdown = GracefulShutdown()
    graceful_shutdown.__enter__()

    logger.info("Starting up...")
    lsh_cache_dict = {
        "english": get_lsh_from_redis(lsh_key="english:lsh_index"),
        # add more languages if necessary
    }
    logger.info("Initialized LSH cache with TTL for supported languages.")

    yield  # Control is returned to FastAPI here

    logger.info("Shutting down...")
    await graceful_shutdown.wait()  # Wait for the shutdown signal
    save_lsh_to_redis(lsh_cache_dict)
    logger.info("Saved LSH cache to Redis.")

    graceful_shutdown.__exit__(None, None, None)


app = FastAPI(lifespan=lifespan)


@app.post("/is_duplicate")
async def is_duplicate(request: Request):
    global lsh_cache_dict
    try:
        json_data = await request.json()
        language = json_data.get('language')
        lsh_cache = lsh_cache_dict.get(language)
        status = run_lsh_check(content=json_data.get('content'), language=language, lsh_cache=lsh_cache,
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
