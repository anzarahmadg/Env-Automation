if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    from main import app

    app.root_path = None

import uvicorn
from fastapi import FastAPI
from scripts.config import Service

if __name__ == "__main__":
    app = FastAPI()
    uvicorn.run("main:app", host=Service.HOST, port=int(Service.PORT))
