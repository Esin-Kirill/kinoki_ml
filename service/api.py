import uvicorn
from fastapi import FastAPI
from db import KMongoDb
from service import calculate_top_films, calculate_user_recommendations
from service import get_user_recommendations

api = FastAPI()
mongo_db = KMongoDb('test_db')
ml_model = 0

# Пересчитываем средний рейтинг фильмов
# Результат складываем в отдельную коллекцию
@api.post('/calculate/top/films')
def api_calculate_top_films():
    response = calculate_top_films(mongo_db)
    return response

# Пересчитываем рекомендации для пользователей
# Результат складываем в отдельную коллекцию
@api.post('/calculate/user/recommendations')
def api_calculate_top_films():
    response = calculate_user_recommendations(mongo_db)
    return response

# Получаем рекомендации для пользователей
@api.get('/recommend/user')
def api_recomend_for_user(user_id:str):
    response = get_user_recommendations(mongo_db, user_id)
    return response

if __name__ == "__main__":
    uvicorn.run("api:api")
