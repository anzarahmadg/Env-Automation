from dataclasses import dataclass
from fastapi import FastAPI
from scripts.core import router


@dataclass
class FastAPIConfig:
    title: str = "Migration"


app = FastAPI(**FastAPIConfig().__dict__)
app.include_router(router)


