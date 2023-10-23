import uvicorn
import logging
from fastapi import FastAPI
from service_ml import calculate_top_films
from service_ml import calculate_recommendations_one, calculate_recommendations_all
from config import config

# Set others loggers level
for _ in logging.root.manager.loggerDict:
    logging.getLogger(_).setLevel(logging.INFO)

# Loger configs
file_handler = logging.FileHandler("service.log", 'a', encoding='utf-8')
stream_handler = logging.StreamHandler()
logging.basicConfig(
    handlers=[stream_handler, file_handler],
    level=logging.DEBUG if config.LOGGIN_LEVEL == 'DEBUG' else logging.INFO,
    format='[%(asctime)s: %(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# API
api = FastAPI()

# Пересчитываем средний рейтинг фильмов
# Результат складываем в отдельную коллекцию
@api.post('/calculate/top/films')
def api_calculate_top_films():
    logging.info('Calculate top films')
    response = calculate_top_films()
    logging.debug(f'Response: {response}')
    return response

# Пересчитываем рекомендации для пользователей
# Результат складываем в отдельную коллекцию
@api.post('/calculate/user/recommendations')
def api_calculate_recommendations_all():
    logging.info('Calculate recommendations for all users')
    response = calculate_recommendations_all()
    logging.debug(f'Response: {response}')
    return response

# Получаем рекомендации для пользователей
@api.post('/calculate/user/recommendations/{user_id}')
def api_calculate_recommendations_one(user_id:str):
    logging.info(f'Calculate recommendations for one user: {user_id}')
    response = calculate_recommendations_one(user_id)
    logging.debug(f'Response: {response}')
    return response

if __name__ == "__main__":
    logging.info('Starting...')
    uvicorn.run("api:api", host=config.API_HOST, port=8080)
