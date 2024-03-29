from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from utils import minhash_signature, get_lsh_from_redis, update_lsh_in_redis, update_candidates_duplicates_in_redis
import uvicorn
import logging
from consts import Consts
from datasketch import MinHashLSH

ADDRESS = "0.0.0.0"
PORT = 9039

app = FastAPI()

logging.basicConfig(filename="/home/omgili/log/duplicate_service_server.log",
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)

logger = logging.getLogger()


@app.post("/is_duplicate")
async def is_duplicate(request: Request):
    try:
        # get parameters from request
        json_data = await request.json()
        content = json_data.get('content')
        article_domain = json_data.get('domain')
        language = json_data.get('language')
        article_id = json_data.get('article_id', None)
        lsh_key = f"{language}:lsh_index"

        try:
            # get lsh from redis
            lsh = get_lsh_from_redis(lsh_key=lsh_key)
        except TypeError as e:
            # need to create new lsh
            logger.error(f"Error while getting LSH from Redis: {str(e)} Creating new LSH")
            lsh = MinHashLSH(threshold=0.9, num_perm=128)
        except Exception as e:
            logger.error(f"Error while getting LSH from Redis: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

        # calculate minhash for the new text
        minhash = await minhash_signature(content, language)

        candidate_pairs = lsh.query(minhash)
        if candidate_pairs:
            result = any(pair.split('|')[1] == article_domain for pair in candidate_pairs)
            if result:
                status = Consts.DUPLICATE
            else:
                status = Consts.SIMILARITY
        else:
            status = None

        logger.info(f"Candidate pairs for the query: {article_id} article: {candidate_pairs}")
        # add new minhash to lsh and store in Redis.
        update_lsh_in_redis(lsh=lsh, lsh_key=lsh_key, minhash=minhash, article_id=article_id,
                            article_domain=article_domain)
        if candidate_pairs:
            update_candidates_duplicates_in_redis(article_id=article_id, candidates=candidate_pairs)

        return JSONResponse(content={"status": status})
    except Exception as e:
        logger.critical(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.get('/health_check')
async def health_endpoint():
    return {"message": "I'm OK"}


if __name__ == "__main__":
    uvicorn.run(app, host=ADDRESS, port=PORT)
