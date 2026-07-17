import uvicorn

if __name__ == '__main__':
    # Запуск сервера uvicorn
    # Приложение get_news:app будет запущено на порту 8000
    uvicorn.run("get_news:app", host="0.0.0.0", port=8000, reload=True)