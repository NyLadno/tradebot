# from idlelib import run
#
# from fastapi import FastAPI
# import asyncio
# app = FastAPI()
#
#
# @app.post('/data_base')
# def start_db():
#     setup_database()
#
#
# if __name__ == '__main__':
#     asyncio.run(start_db())
#


s = "2006" * 298
print(s)
print(len(s))

while "200" in s or "666" in s:
    s = s.replace("200", "66", 1)
    s = s.replace("666", "66", 1)

print(s)