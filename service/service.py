from random import shuffle
from config import *
from calculations import *


def return_request_like_response(function):
    """
        Декоратор.
        Приводим формат ответа к request-like
    """

    def make_request_like_response(*args, **kwargs):
        response = {'status_code':200, 'text':'OK', 'data':None}

        try:
            response['data'] = function(*args, **kwargs)
        except Exception as err:
            response['status_code'] = 500
            response['text'] = err
        
        return response

    return make_request_like_response


# @return_request_like_response
def calculate_top_films(mongo_db):
    """
        Рассчитываем средний рейтинг фильма за всё время
        на основе рейтингов с разных площадок.

        Те фильмы, у которых рейтинг выше DEFAULT_TOP_RATING, кладём в отдельную коллекцию
        предварительно очистив её от старых записей.
    """

    select_query = {
            "rating":1, "ratingFilmCritics":1, 
            "ratingGoodReview":1, "ratingImdb":1, "ratingKinopoisk":1, 
        }
    films = mongo_db.get_records(MONGO_FILMS_TABLE, {}, select_query)
    films = prepare_top_films(films)

    mongo_db.create_collection(MONGO_FILMS_TOP_TABLE)
    mongo_db.insert_records(MONGO_FILMS_TOP_TABLE, films, delete_records=True)


# @return_request_like_response
def calculate_user_recommendations(mongo_db):
    """
        Подготавливаем рекомендации для пользователей.
    """

    user_ratings = mongo_db.get_records(MONGO_FILMS_RATINGS_TABLE)
    user_likes = mongo_db.get_records(MONGO_FILMS_LIKES_TABLE)
    user_recommendations = prepare_user_recommendations(user_ratings, user_likes)

    mongo_db.create_collection(MONGO_USER_RECOMS_TABLE)
    mongo_db.insert_records(MONGO_USER_RECOMS_TABLE, user_recommendations, delete_records=True)


# @return_request_like_response
def get_user_recommendations(mongo_db, user_id):
    """
        Получаем Id юзера, смотрим кол-во фильмов, кт он лайкнул или указал рейтинг.
        - Если это кол-во < DEFAULT_USER_ACTIVITY_LIMIT -> рекомендуем "топ" фильмы
        - Если это кол-во >= DEFAULT_USER_ACTIVITY_LIMIT -> подключаем модель и рекомендуем через неё.
    """

    find_query = {"userId":user_id}
    select_query = {"filmId":1, "_id":False}
    user_likes = mongo_db.count_records(MONGO_FILMS_LIKES_TABLE, find_query)
    user_recommendations = mongo_db.count_records(MONGO_USER_RECOMS_TABLE, find_query)

    if user_likes < DEFAULT_USER_ACTIVITY_LIMIT or user_recommendations <= 5:

        watched_films = mongo_db.get_records(MONGO_FILMS_LIKES_TABLE, find_query, select_query)
        watched_films = [doc.get("filmId") for doc in watched_films]

        films = mongo_db.get_records(MONGO_FILMS_TOP_TABLE, {}, select_query)
        films = [doc.get("filmId") for doc in films if doc.get("filmId") not in watched_films]
        shuffle(films)
        films = films[:DEFAULT_TOP_LIMIT]

        return films

    else:

        films = mongo_db.get_records(MONGO_USER_RECOMS_TABLE, find_query, select_query)
        shuffle(films)
        films = films[:DEFAULT_TOP_LIMIT]

        return films