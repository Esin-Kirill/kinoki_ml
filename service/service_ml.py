import logging
import traceback
from config import *
from calculations import *
from db import KMongoDb

# Logger
logging.getLogger(__name__)


def return_request_like_response(function):
    """
        Декоратор.
        Приводим формат ответа к request-like
    """

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


@return_request_like_response
def calculate_top_films():
    """
        Рассчитываем средний рейтинг фильма за всё время
        на основе рейтингов с разных площадок.

        Те фильмы, у которых рейтинг выше DEFAULT_TOP_RATING, кладём в отдельную коллекцию
        предварительно очистив её от старых записей.
    """
    # Подключаемся к базе
    mongo_db = KMongoDb(MONGO_DB)

    # Получаем фильмы
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
def calculate_recommendations_all():
    """
        Подготавливаем рекомендации для всех пользователей.
    """

    # Подключаемся к базе
    mongo_db = KMongoDb(MONGO_DB)

    ### ACTIVITY
    # Collect users activity
    user_likes = mongo_db.get_records(MONGO_FILMS_LIKES_TABLE)
    df_user_activity = prepare_user_activity(user_likes)
    logging.info(f'Got users activity: {len(df_user_activity)}')

    # Insert users activity
    mongo_db.create_collection(MONGO_USER_ACTIVITY_TABLE)
    logging.info(f'{MONGO_USER_ACTIVITY_TABLE} collection created')

    user_activity_records = df_user_activity.to_dict('records')
    mongo_db.insert_records(MONGO_USER_ACTIVITY_TABLE, user_activity_records, delete_records=True)
    logging.info(f'{len(user_activity_records)} users activity inserted')

    ### RECOMMENDATIONS
    # Make user recommendations
    dict_users_recommendations = prepare_user_recommendations(df_user_activity)
    logging.info('Got similar users and films')

    # Merge films with recommendations
    user_recommendations = process_user_recommendations(dict_users_recommendations)
    logging.info('Got films with user recommend for')

    # Insert into Mongo
    mongo_db.create_collection(MONGO_USER_RECOMS_TABLE)
    logging.info(f'{MONGO_USER_RECOMS_TABLE} collection created')

    mongo_db.insert_records(MONGO_USER_RECOMS_TABLE, user_recommendations, delete_records=True)
    logging.info(f'{len(user_recommendations)} recommendations inserted')


@return_request_like_response
def calculate_recommendations_one(user_id):
    """
        Подготавливаем рекомендации для конкретного пользователя
    """
    # Подключаемся к базе
    mongo_db = KMongoDb(MONGO_DB)

    # Find certain user likes & ratings
    # Ищем по userId ИЛИ anonymousId
    find_query = {
        "$or": [
            {"userId": ObjectId(user_id)}, 
            {"anonymousId": ObjectId(user_id)}
        ]
    }
    user_likes = mongo_db.get_sorted_limited_records(MONGO_FILMS_LIKES_TABLE, 
                                                    find_query=find_query, 
                                                    sort_field="updatedAt", 
                                                    limit=DEFAULT_ACTIVITY_TRIGGER_LIMIT*3)
    df_one_user_activity = prepare_user_activity(user_likes)
    logging.info(f'Got user activity: {len(df_one_user_activity)}')

    # Get all users activity
    find_query = {
        "$and": [
            {"userId": {"$ne": ObjectId(user_id)}}, 
            {"anonymousId": {"$ne": ObjectId(user_id)}}
        ]
    }
    other_users_activity = mongo_db.get_records(MONGO_USER_ACTIVITY_TABLE, find_query=find_query, select_query={"_id":0})

    # Union data
    df_other_users_activity = pd.DataFrame(other_users_activity)
    df_user_activity = pd.concat([df_one_user_activity, df_other_users_activity], axis=0)
    logging.info(f'Got all users activity: {len(df_user_activity)}')

    # Make recommendations
    dict_user_recommendations = prepare_user_recommendations(df_user_activity, user_id)
    logging.info('Got similar users and films')

    # Get recommended films
    user_recommendations = process_user_recommendations(dict_user_recommendations)
    logging.info(f'Got {len(user_recommendations)} recommendations for user: {user_id}')

    # Insert new recommendations
    if bool(user_recommendations):
        delete_query = {
            "$or": [
                {"userId": ObjectId(user_id)}, 
                {"anonymousId": ObjectId(user_id)}
            ]
        }
        mongo_db.insert_records(MONGO_USER_RECOMS_TABLE, user_recommendations, delete_records=True, delete_query=delete_query)
        logging.info(f'{len(user_recommendations)} recommendations for {user_id} inserted')

        # Insert new user activity
        one_user_activity = df_one_user_activity.to_dict('records')
        mongo_db.insert_records(MONGO_USER_ACTIVITY_TABLE, one_user_activity, delete_records=True, delete_query=delete_query)
        logging.info(f'{len(one_user_activity)} user activity for {user_id} inserted')
    else:
        logging.info(f'No recommendations for: {user_id} found...')
