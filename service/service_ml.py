import logging
import traceback
from config import config
from calculations_user import *
from helpers import update_user_profile, update_watched_films
from helpers import get_film_similarity, get_filter
from db import KMongoDb

# Logger
logging.getLogger(__name__)


def return_request_like_response(function):
    """Decorator to return request-like response"""

    def make_request_like_response(*args, **kwargs):
        response = {'status_code':200, 'text':'OK'}

        try:
            function(*args, **kwargs)
        except Exception as err:
            response['status_code'] = 500
            err = f"ERROR IN {__import__(function.__module__).__name__}.py -> {function.__name__}: {str(err)}"
            trb = traceback.format_exc()
            response['text'] = err + '\n' + trb
        
        return response

    return make_request_like_response


# @return_request_like_response
def calculate_top_films():
    """
        Рассчитываем средний рейтинг фильма за всё время
        на основе рейтингов с разных площадок.

        Те фильмы, у которых рейтинг выше DEFAULT_TOP_RATING, кладём в отдельную коллекцию
        предварительно очистив её от старых записей.
    """
    # Подключаемся к базе
    mongo_db = KMongoDb(config.MONGO_INITDB_DATABASE)

    # Получаем фильмы
    select_query = {
        "rating":1, "ratingFilmCritics":1, 
        "ratingGoodReview":1, "ratingImdb":1, "ratingKinopoisk":1, 
    }
    films = mongo_db.get_records(config.MONGO_FILMS_TABLE, select_query=select_query)
    top_films = prepare_top_films(films)
    logging.info('Got top films')

    mongo_db.create_collection(config.MONGO_FILMS_TOP_TABLE)
    logging.info(f'{config.MONGO_FILMS_TOP_TABLE} collection created')

    mongo_db.insert_records(config.MONGO_FILMS_TOP_TABLE, top_films, delete_records=True)
    logging.info(f'{len(top_films)} top films inserted')


# @return_request_like_response
# def calculate_recommendations_all():
#     """
#         Подготавливаем рекомендации для всех пользователей.
#     """

#     # Подключаемся к базе
#     mongo_db = KMongoDb(config.MONGO_INITDB_DATABASE)

#     ### ACTIVITY
#     # Collect users activity
#     user_likes = mongo_db.get_records(config.MONGO_FILMS_LIKES_TABLE)
#     df_user_activity = prepare_user_activity(user_likes)
#     logging.info(f'Got users activity: {len(df_user_activity)}')

#     # Insert users activity
#     mongo_db.create_collection(config.MONGO_USER_ACTIVITY_TABLE)
#     logging.info(f'{config.MONGO_USER_ACTIVITY_TABLE} collection created')

#     user_activity_records = df_user_activity.to_dict('records')
#     mongo_db.insert_records(config.MONGO_USER_ACTIVITY_TABLE, user_activity_records, delete_records=True)
#     logging.info(f'{len(user_activity_records)} users activity inserted')

#     ### RECOMMENDATIONS
#     # Make user recommendations
#     dict_users_recommendations = prepare_user_recommendations(df_user_activity)
#     logging.info('Got similar users and films')

#     # Merge films with recommendations
#     user_recommendations = process_user_recommendations(dict_users_recommendations)
#     logging.info('Got films with user recommend for')

#     # Insert into Mongo
#     mongo_db.create_collection(config.MONGO_USER_RECOMS_TABLE)
#     logging.info(f'{config.MONGO_USER_RECOMS_TABLE} collection created')

#     mongo_db.insert_records(config.MONGO_USER_RECOMS_TABLE, user_recommendations, delete_records=True)
#     logging.info(f'{len(user_recommendations)} recommendations inserted')


# @return_request_like_response
# def calculate_recommendations_one(user_id):
#     """
#         Подготавливаем рекомендации для конкретного пользователя
#     """
#     # Подключаемся к базе
#     mongo_db = KMongoDb(config.MONGO_INITDB_DATABASE)

#     # Find certain user likes & ratings
#     # Ищем по userId ИЛИ anonymousId
#     find_query = {
#         "$or": [
#             {"userId": ObjectId(user_id)}, 
#             {"anonymousId": ObjectId(user_id)}
#         ]
#     }
#     user_likes = mongo_db.get_sorted_limited_records(config.MONGO_FILMS_LIKES_TABLE, 
#                                                     find_query=find_query, 
#                                                     sort_field="updatedAt", 
#                                                     limit=config.DEFAULT_ACTIVITY_TRIGGER_LIMIT*3)
#     df_one_user_activity = prepare_user_activity(user_likes)
#     logging.info(f'Got user activity: {len(df_one_user_activity)}')

#     # Get all users activity
#     find_query = {
#         "$and": [
#             {"userId": {"$ne": ObjectId(user_id)}}, 
#             {"anonymousId": {"$ne": ObjectId(user_id)}}
#         ]
#     }
#     other_users_activity = mongo_db.get_records(config.MONGO_USER_ACTIVITY_TABLE, find_query=find_query, select_query={"_id":0})

#     # Union data
#     df_other_users_activity = pd.DataFrame(other_users_activity)
#     df_user_activity = pd.concat([df_one_user_activity, df_other_users_activity], axis=0)
#     logging.info(f'Got all users activity: {len(df_user_activity)}')

#     # Make recommendations
#     dict_user_recommendations = prepare_user_recommendations(df_user_activity, user_id)
#     logging.info('Got similar users and films')

#     # Get recommended films
#     user_recommendations = process_user_recommendations(dict_user_recommendations)
#     logging.info(f'Got {len(user_recommendations)} recommendations for user: {user_id}')

#     # Insert new recommendations
#     if bool(user_recommendations):
#         delete_query = {
#             "$or": [
#                 {"userId": ObjectId(user_id)}, 
#                 {"anonymousId": ObjectId(user_id)}
#             ]
#         }
#         mongo_db.insert_records(config.MONGO_USER_RECOMS_TABLE, user_recommendations, delete_records=True, delete_query=delete_query)
#         logging.info(f'{len(user_recommendations)} recommendations for {user_id} inserted')

#         # Insert new user activity
#         one_user_activity = df_one_user_activity.to_dict('records')
#         mongo_db.insert_records(config.MONGO_USER_ACTIVITY_TABLE, one_user_activity, delete_records=True, delete_query=delete_query)
#         logging.info(f'{len(one_user_activity)} user activity for {user_id} inserted')
#     else:
#         logging.info(f'No recommendations for: {user_id} found...')


# @return_request_like_response
def calculate_recommendations_all():
    """
        Подготавливаем рекомендации для всех пользователей.
    """

    # Подключаемся к базе
    mongo_db = KMongoDb(config.MONGO_INITDB_DATABASE)

    # Collect users likes
    user_likes = mongo_db.get_records(config.MONGO_FILMS_LIKES_TABLE)
    logging.info(f'Got user likes: {len(user_likes)}')

    # Create user profile based on likes
    users_profiles = {}
    logging.info('Start calculating user profiles')

    for like in user_likes:
        userid = like.get("userId")
        anonymousid = like.get("anonymousId")

        # Потому что юзеры и анонимы в разных таблицах и у них могу попасться одинаковые айдишники
        useruniqid = str(userid) if userid is not None else 'ANON_' + str(anonymousid)
        user_profile = users_profiles.get(useruniqid, {})

        if like["state"] == "LIKE":
            # Get liked film
            filmid = like.get("filmId")
            find_query = {"_id":filmid}
            select_query = {
                "nameRu":1, "nameOriginal":1,
                "type":1, "countries":1, "genres":1, "staff":1
            }
            film = mongo_db.get_one_record(config.MONGO_FILMS_TABLE, find_query, select_query)

            # Update user profile
            user_profile = update_user_profile(user_profile, film, like)

        # Update watched films
        user_profile = update_watched_films(user_profile, like)
        users_profiles[useruniqid] = user_profile

    logging.info('User profiles calculated')

    # Insert user profiles
    mongo_db.create_collection(config.MONGO_USER_PROFILES)
    logging.info(f'{config.MONGO_USER_PROFILES} collection created')

    records = list(users_profiles.values())
    mongo_db.insert_records(config.MONGO_USER_PROFILES, records, delete_records=True)
    logging.info('User profiles inserted')


# @return_request_like_response
def calculate_recommendations_one(user_id:str):
    """
        Подготавливаем рекомендации для конкретного пользователя
    """
    # Подключаемся к базе
    mongo_db = KMongoDb(config.MONGO_INITDB_DATABASE)

    # Get user profile
    find_query = {
        "$or": [
            {"userId": ObjectId(user_id)}, 
            {"anonymousId": ObjectId(user_id)}
        ]
    }
    user_profile = mongo_db.get_one_record(config.MONGO_USER_PROFILES, find_query=find_query)
    first_time_user = False

    # If new user
    if user_profile is None:
        # Костыыыль)
        first_time_user = True
        user_profile = {}

        # Create user profile
        user_likes = mongo_db.get_sorted_limited_records(
            config.MONGO_FILMS_LIKES_TABLE,
            sort_field='updatedAt',
            find_query=find_query,
            limit=config.DEFAULT_ACTIVITY_TRIGGER_LIMIT
        )
        logging.debug(f'Got user likes: {len(user_likes)} for {user_id}')

        user_profile['userId'] = user_likes[0].get("userId")
        user_profile['anonymousId'] = user_likes[0].get("anonymousId")

        # Iterate over likes
        for like in user_likes:
            if like["state"] == "LIKE":
                # Get liked film
                filmid = like.get("filmId")
                find_film_query = {"_id":filmid}
                select_query = {
                    "nameRu":1, "nameOriginal":1,
                    "type":1, "countries":1, "genres":1, "staff":1
                }
                film = mongo_db.get_one_record(config.MONGO_FILMS_TABLE, find_film_query, select_query)

                # Update user profile
                user_profile = update_user_profile(user_profile, film, like)

            # Update watched films
            user_profile = update_watched_films(user_profile, like)

    # Get user preferences
    filter = get_filter(user_profile)
    select_query = {
        "nameRu":1, "nameOriginal":1,
        "genres":1, "countries":1, "staff":1
    }
    films = mongo_db.get_records(config.MONGO_FILMS_TABLE, find_query=filter, select_query=select_query, limit=config.NUMBER_QUERY_FILMS)

    # Calculate similarity
    similarity = {}
    for film in films:
        filmid, sim = get_film_similarity(film, user_profile)
        similarity[filmid] = sim

    # Sort most recommended
    recommended_films = sorted(similarity.items(), key=lambda x: x[1], reverse=True)[:config.NUMBER_SIMILAR_FILMS]

    # Make values to insert
    userid = user_profile.get('userId')
    anonymusid = user_profile.get('anonymusId')
    recommendations = [{'userId': userid, 'anonymusId': anonymusid, 'filmId': film[0]} for film in recommended_films]

    # Insert into collection
    delete_query = {
        "$or": [
            {"userId": ObjectId(user_id)}, 
            {"anonymousId": ObjectId(user_id)}
        ]
    }
    mongo_db.insert_records(config.MONGO_USER_RECOMS_TABLE, recommendations, delete_records=True, delete_query=delete_query)
    logging.info(f'{len(recommendations)} recommendations for {user_id} inserted')

    # Update user profile
    user_profile = user_profile if not first_time_user else {} #Костыыыыль)

    user_likes = mongo_db.get_sorted_limited_records(
        config.MONGO_FILMS_LIKES_TABLE,
        sort_field='updatedAt',
        find_query=find_query,
        limit=config.DEFAULT_ACTIVITY_TRIGGER_LIMIT
    )
    logging.debug(f'Got user likes: {len(user_likes)} for {user_id}')

    user_profile['userId'] = user_likes[0].get("userId")
    user_profile['anonymousId'] = user_likes[0].get("anonymousId")

    # Iterate over likes
    for like in user_likes:
        if like["state"] == "LIKE":
            # Get liked film
            filmid = like.get("filmId")
            find_film_query = {"_id":filmid}
            select_query = {
                "nameRu":1, "nameOriginal":1,
                "type":1, "countries":1, "genres":1, "staff":1
            }
            film = mongo_db.get_one_record(config.MONGO_FILMS_TABLE, find_film_query, select_query)

            # Update user profile
            user_profile = update_user_profile(user_profile, film, like)

        # Update watched films
        user_profile = update_watched_films(user_profile, like)

    mongo_db.insert_one_record(config.MONGO_USER_PROFILES, user_profile, delete_record=True, delete_query=delete_query)
    logging.info(f'User profile for {user_id} inserted')
