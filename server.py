from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from utils import minhash_signature, get_lsh_from_redis, update_lsh_in_redis
import uvicorn
import logging


ADDRESS = "0.0.0.0"
PORT = 9037

app = FastAPI()
logger = logging.getLogger(__name__)


@app.post("/is_duplicate")
async def is_duplicate(request: Request):
    try:
        status = None

        # get parameters from request
        json_data = await request.json()
        content = json_data.get('content')
        article_domain = json_data.get('domain')
        language = json_data.get('language')
        article_id = json_data.get('article_id', None)
        lsh_key = f"{language}:lsh_index"

        # get lsh from redis
        lsh = get_lsh_from_redis(lsh_key=lsh_key)

        # calculate minhash for the new text
        minhash = await minhash_signature(content)

        candidate_pairs = lsh.query(minhash)
        for candidate_pair in candidate_pairs:
            _id = candidate_pair.split('|')[0]
            domain = candidate_pair.split('|')[1]
            if domain == article_domain:
                status = "duplicate"
            else:
                status = "similarity"

        # add new minhash to lsh and store in Redis.
        update_lsh_in_redis(lsh=lsh, lsh_key=lsh_key, minhash=minhash, article_id=article_id,
                            article_domain=article_domain)

        return JSONResponse(content={"status": status})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.get('/health_check')
async def health_endpoint():
    return {"message": "I'm OK"}


if __name__ == "__main__":
    uvicorn.run(app, host=ADDRESS, port=PORT)
