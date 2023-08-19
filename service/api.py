import os
import json
import uvicorn
import logging
from fastapi import FastAPI
from datetime import datetime
from db import KMongoDb
from service_ml import calculate_top_films
from service_ml import calculate_recommendations_one, calculate_recommendations_all
from config import MONGO_DB, LOGGIN_LEVEL

# Set others loggers level
for _ in logging.root.manager.loggerDict:
    logging.getLogger(_).setLevel(logging.INFO)

# Loger configs
file_log = f"ml_service_log_{datetime.now().strftime('%Y-%m-%d')}.log"
file_handler = logging.FileHandler(file_log, 'a', encoding='utf-8')
stream_handler = logging.StreamHandler()
logging.basicConfig(
    handlers=[file_handler, stream_handler], 
    level=logging.DEBUG if LOGGIN_LEVEL == 'DEBUG' else logging.INFO,
    format='[%(asctime)s: %(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Deal with unicorn logging level
with open(os.path.abspath('uvicorn_log_config.json'), 'r') as config_json:
    log_config = uvicorn.config.LOGGING_CONFIG
    log_config = json.loads(config_json.read())
    log_config['handlers']['file']['filename'] = file_log


# API
api = FastAPI()
mongo_db = KMongoDb(MONGO_DB)


# Пересчитываем средний рейтинг фильмов
# Результат складываем в отдельную коллекцию
@api.post('/calculate/top/films')
def api_calculate_top_films():
    logging.info('Calculate top films')
    response = calculate_top_films(mongo_db)
    logging.debug(f'Response: {response}')
    return response

# Пересчитываем рекомендации для пользователей
# Результат складываем в отдельную коллекцию
@api.post('/calculate/user/recommendations')
def api_calculate_recommendations_all():
    logging.info('Calculate recommendations for all users')
    response = calculate_recommendations_all(mongo_db)
    logging.debug(f'Response: {response}')
    return response

# Получаем рекомендации для пользователей
@api.post('/calculate/user/recommendations/{user_id}')
def api_calculate_recommendations_one(user_id:str):
    logging.info(f'Calculate recommendations for one user: {user_id}')
    response = calculate_recommendations_one(mongo_db, user_id)
    logging.debug(f'Response: {response}')
    return response

if __name__ == "__main__":
    logging.info('Starting...')
    uvicorn.run("api:api", log_config=log_config)
