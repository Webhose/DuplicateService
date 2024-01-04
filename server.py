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
        is_duplicate_document = False
        json_data = await request.json()
        content = json_data.get('content')
        article_domain = json_data.get('domain')
        language = json_data.get('language')
        article_id = json_data.get('article_id', None)
        lsh = get_lsh_from_redis(lsh_key=f"{language}:lsh_index")
        minhash = minhash_signature(content)
        candidate_pairs = lsh.query(minhash)
        for candidate_pair in candidate_pairs:
            _id = candidate_pair.split('|')[0]
            domain = candidate_pair.split('|')[1]
            if domain == article_domain:
                print(f"Found duplicate for {article_domain} and {domain}")
                is_duplicate_document = True
            else:
                print(f"Found Similarity {article_domain} and {domain}")
        # add new minhash to lsh and store in Redis.
        update_lsh_in_redis(lsh=lsh, lsh_key=f"{language}:lsh_index", minhash=minhash, article_id=article_id,
                            article_domain=article_domain)

        return JSONResponse(content={"is_duplicate_document": is_duplicate_document})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(app, host=ADDRESS, port=PORT)
