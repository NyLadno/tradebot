from idlelib import run

from fastapi import FastAPI
from models import setup_database
import asyncio
app = FastAPI()


@app.post('/data_base')
def start_db():
    setup_database()


if __name__ == '__main__':
    asyncio.run(start_db)