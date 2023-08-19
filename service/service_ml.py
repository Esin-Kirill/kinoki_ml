from random import shuffle
import logging
from config import *
from calculations import *

# Logger
logging.getLogger(__name__)


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
            err = f"ERROR IN {__import__(function.__module__).__name__}.py -> {function.__name__}: {str(err)}"
            response['text'] = err
        
        return response

    return make_request_like_response


@return_request_like_response
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
    films = mongo_db.get_records(MONGO_FILMS_TABLE, select_query=select_query)
    top_films = prepare_top_films(films)
    logging.info('Got top films')

    mongo_db.create_collection(MONGO_FILMS_TOP_TABLE)
    logging.info(f'{MONGO_FILMS_TOP_TABLE} collection created')

    mongo_db.insert_records(MONGO_FILMS_TOP_TABLE, top_films, delete_records=True)
    logging.info(f'{len(top_films)} top films inserted')


@return_request_like_response
def calculate_recommendations_all(mongo_db):
    """
        Подготавливаем рекомендации для всех пользователей.
    """

    ### ACTIVITY
    # Collect users activity
    users_ratings = mongo_db.get_records(MONGO_FILMS_RATINGS_TABLE)
    users_likes = mongo_db.get_records(MONGO_FILMS_LIKES_TABLE)
    df_users_activity = prepare_user_activity(users_ratings, users_likes)
    logging.info(f'Got users activity: {len(df_users_activity)}')

    # Insert users activity
    mongo_db.create_collection(MONGO_USER_ACTIVITY_TABLE)
    logging.info(f'{MONGO_USER_ACTIVITY_TABLE} collection created')

    users_activity_records = df_users_activity.to_dict('records')
    mongo_db.insert_records(MONGO_USER_ACTIVITY_TABLE, users_activity_records, delete_records=True)
    logging.info(f'{len(users_activity_records)} users activity inserted')

    ### RECOMMENDATIONS
    # Make user recommendations
    select_query = {"filmId": 1, "_id": 0}
    top_films = mongo_db.get_records(MONGO_USER_ACTIVITY_TABLE, select_query=select_query)
    top_films = [film.get('filmId') for film in top_films]
    dict_users_recommendations = prepare_user_recommendations(df_users_activity, top_films=top_films)
    logging.info('Got similar users and films')

    # Get films
    select_query = {"_id":1}
    films = mongo_db.get_records(MONGO_FILMS_TABLE, select_query=select_query)
    logging.info('Got films')

    # Merge films with recommendations
    films_with_recommendations = prepare_films_with_recommendations(dict_users_recommendations, films)
    logging.info('Got films with user recommend for')

    # Insert into Mongo
    mongo_db.create_collection(MONGO_USER_RECOMS_TABLE)
    logging.info(f'{MONGO_USER_RECOMS_TABLE} collection created')

    mongo_db.insert_records(MONGO_USER_RECOMS_TABLE, films_with_recommendations, delete_records=True)
    logging.info(f'{len(films_with_recommendations)} recommendations inserted')


@return_request_like_response
def calculate_recommendations_one(mongo_db, user_id):
    """
        Подготавливаем рекомендации для конкретного пользователя
    """

    # Find certain user likes & ratings
    # Ищем сначала по userId
    find_query = {"userId": user_id}
    user_ratings = mongo_db.get_records(MONGO_FILMS_RATINGS_TABLE, find_query=find_query)
    user_likes = mongo_db.get_sorted_limited_records(MONGO_FILMS_LIKES_TABLE, find_query=find_query, sort_field="updatedAt", limit=DEFAULT_ACTIVITY_TRIGGER_LIMIT)

    # Потом по anonymousId
    find_query = {"anonymousId": user_id}
    anonymus_ratings = mongo_db.get_records(MONGO_FILMS_RATINGS_TABLE, find_query=find_query)
    anonymus_likes = mongo_db.get_sorted_limited_records(MONGO_FILMS_LIKES_TABLE, find_query=find_query, sort_field="updatedAt", limit=DEFAULT_ACTIVITY_TRIGGER_LIMIT)

    # И эти данные объединяем
    df_user_activity = prepare_user_activity(user_ratings, user_likes)
    df_anonymus_activity = prepare_user_activity(anonymus_ratings, anonymus_likes)
    df_one_user_activity = pd.concat([df_user_activity, df_anonymus_activity])
    logging.info(f'Got user activity: {len(df_one_user_activity)}')

    # Get all users activity
    find_query = {"userId": {"$ne": user_id}}
    other_users_activity = mongo_db.get_records(MONGO_USER_ACTIVITY_TABLE, find_query=find_query)

    # Union data
    df_other_users_activity = pd.DataFrame(other_users_activity)
    df_users_activity = pd.concat([df_other_users_activity, df_one_user_activity], axis=0)
    logging.info(f'Got all users activity: {len(df_users_activity)}')

    # Make recommendations
    select_query = {"filmId": 1, "_id": 0}
    top_films = mongo_db.get_records(MONGO_USER_ACTIVITY_TABLE, select_query=select_query)
    top_films = [film.get('filmId') for film in top_films]
    user_recommendations = prepare_user_recommendations(df_users_activity, user_id, top_films)
    logging.info('Got similar users and films')

    # Get films
    select_query = {"_id":1}
    films = mongo_db.get_records(MONGO_FILMS_TABLE, select_query=select_query)
    logging.info('Got films')

    # Get recommended films
    films_with_recommendations = prepare_films_with_recommendations(user_recommendations, films)
    logging.info(f'Got {len(films_with_recommendations)} recommendations for user: {user_id}')

    # Insert new recommendations
    delete_query = {"userRecommendId": user_id}
    mongo_db.insert_records(MONGO_USER_RECOMS_TABLE, films_with_recommendations, delete_records=True, delete_query=delete_query)
    logging.info(f'{len(films_with_recommendations)} recommendations for {user_id} inserted')

    # Insert new user activity
    one_user_activity = df_one_user_activity.to_dict('records')
    delete_query = {"userId": user_id}
    mongo_db.insert_records(MONGO_USER_ACTIVITY_TABLE, one_user_activity, delete_records=True, delete_query=delete_query)
    logging.info(f'{len(one_user_activity)} user activity for {user_id} inserted')


# @return_request_like_response
# def get_user_recommendations(mongo_db, user_id):
    # """
    #     Получаем Id юзера, смотрим кол-во фильмов, кт он лайкнул или указал рейтинг.
    #     - Если это кол-во < DEFAULT_USER_ACTIVITY_LIMIT или нам особо нечего ему рекомендовать -> рекомендуем "топ" фильмы
    #     - Если это кол-во >= DEFAULT_USER_ACTIVITY_LIMIT -> подключаем модель и рекомендуем через неё.
    # """

    # find_query = {"userId": user_id}
    # select_query = {"filmId": 1, "_id": False}
    # user_likes = mongo_db.count_records(MONGO_FILMS_LIKES_TABLE, find_query)
    # logging.debug(f'User likes: {user_likes}')

    # user_ratings = mongo_db.count_records(MONGO_FILMS_RATINGS_TABLE, find_query)
    # logging.debug(f'User ratings: {user_ratings}')

    # user_activity = user_likes + user_ratings # Считаем пользовательскую активность
    # logging.info(f'User total activity: {user_activity}')

    # user_recommendations = mongo_db.count_records(MONGO_USER_RECOMS_TABLE, find_query) # Смотрим, а есть ли нам чё рекомендовать вообще
    # logging.info(f'Have recommendations: {user_recommendations}')

    # if user_activity < DEFAULT_USER_ACTIVITY_LIMIT or user_recommendations == 0:
    #     logging.info('Recommending top films...')
    #     watched_films = mongo_db.get_records(MONGO_FILMS_LIKES_TABLE, find_query, select_query)
    #     watched_films = [doc.get("filmId") for doc in watched_films]

    #     films = mongo_db.get_records(MONGO_FILMS_TOP_TABLE, {}, select_query)
    #     films = [doc.get("filmId") for doc in films if doc.get("filmId") not in watched_films]
    #     shuffle(films)
    #     films = films[:DEFAULT_TOP_LIMIT]

    #     return films

    # else:
    #     logging.info('Recommending films based on personal recommendations...')
    #     films = mongo_db.get_records(MONGO_USER_RECOMS_TABLE, find_query)[0].get("recommendedFilms")
    #     shuffle(films)
    #     films = films[:DEFAULT_TOP_LIMIT]

    #     return films
