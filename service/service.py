import pandas as pd
from random import shuffle
from config import USER_ACTIVITY_LIMIT, DEFAULT_TOP_LIMIT, DEFAULT_TOP_RATING


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


@return_request_like_response
def calculate_top_films(mongo_db):
    """
        Рассчитываем средний рейтинг фильма за всё время
        на основе рейтингов.

        Те фильмы, у которых рейтинг выше DEFAULT_TOP_RATING, кладём в отдельную коллекцию
        предварительно очистив её от старых записей.
    """

    # Выбираем только те "столбы", которые содержат инфу о рейтингах и Id фильма
    select_query = {
            "filmId":1, "rating":1, "ratingFilmCritics":1, 
            "ratingGoodReview":1, "ratingImdb":1, "ratingKinopoisk":1, 
            "_id":False
        }
    films = mongo_db.get_records('film', {}, select_query)

    # Находим средний рейтинг
    df = pd.DataFrame(films)
    df['meanRating'] = df['rating'] + df['ratingFilmCritics'] + df['ratingGoodReview']/10 + df['ratingImdb'] + df['ratingKinopoisk']
    df['meanRating'] /= 5
    df = df[df["meanRating"] >= DEFAULT_TOP_RATING][["filmId", "meanRating"]]
    records = df.to_dict('records') # Приводим датафрейм к виду списка словарей

    mongo_db.create_collection('film_top')
    mongo_db.insert_records('film_top', records, delete_records=True)


@return_request_like_response
def get_user_recommendations(mongo_db, user_id):
    """
        Получаем Id юзера, смотрим кол-во фильмов, кт он лайкнул или указал рейтинг.
        Если это кол-во < USER_ACTIVITY_LIMIT -> рекомендуем DEFAULT_TOP_LIMIT фильмов
        Если это кол-во >= USER_ACTIVITY_LIMIT -> подключаем модель и рекомендуем ей.
    """

    find_query = {"userId":user_id}
    user_likes = mongo_db.count_records('film_like_dislike', find_query)

    if user_likes < USER_ACTIVITY_LIMIT:

        find_query = {"userId":user_id}
        select_query = {"filmId":1, "_id":False}
        watched_films = mongo_db.get_records('film_like_dislike', find_query, select_query)
        watched_films = [doc.get("filmId") for doc in watched_films]

        select_query = {"filmId":1, "_id":False}
        films = mongo_db.get_records('film_top', {}, select_query)
        films = [doc.get("filmId") for doc in films if doc.get("filmId") not in watched_films]
        films = shuffle(films)

        return films[:DEFAULT_TOP_LIMIT]
    
    else:
    
        return "This user has too much likes"


def get_film_recommendations(mongo_db, film_id):
    find_query = {"filmId":film_id}
    film_doc = mongo_db.get_records('film_matches', find_query)
    films_matches = [film_id for film_id in film_doc if film_doc.get(film_id) > 80]
    return films_matches
