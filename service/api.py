from fastapi import FastAPI
import uvicorn
from db import KMongoDb
from service import calculate_top_films
from service import get_user_recommendations, get_film_recommendations

api = FastAPI()
mongo_db = KMongoDb('test_db')
ml_model = 0


@api.get('/recommend/user')
def recommend_for_user(user_id:str):
    films = get_user_recommendations(mongo_db, user_id)
    return films


@api.get('/recommend/film')
def recommend_for_film(film_id:str):
    films = get_film_recommendations(mongo_db, film_id)
    return film_id


# Пересчитываем средний рейтинг фильмов
# Результат складываем в отдельную коллекцию
@api.post('/calculate/top')
def calculate_film_rating():
    response = calculate_top_films(mongo_db)
    return response


if __name__ == "__main__":
    uvicorn.run("api:api")
