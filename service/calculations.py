import pandas as pd
import logging
import math
from bson import ObjectId
from bson.decimal128 import Decimal128
from sklearn.metrics.pairwise import pairwise_distances
from config import DEFAULT_TOP_RATING, DEFAULT_COSINE_LIMIT, DEFAULT_ACTIVITY_TRIGGER_LIMIT
from config import DAFAULT_FILM_MISSED_RATING, DEFAULT_ACTIVITY_TRIGGER_LIMIT

# Logger
logging.getLogger(__name__)


def get_film_rating(film, key):
    rating = float(film.get(key, Decimal128('0.0')).to_decimal())
    rating = rating/10 if key == 'ratingGoodReview' else rating
    rating = DAFAULT_FILM_MISSED_RATING if rating == 0 or math.isnan(rating) else rating
    return rating


def prepare_top_films(films):
    top_films = []
    rating_keys = ['rating', 'ratingFilmCritics', 'ratingGoodReview', 'ratingImdb', 'ratingKinopoisk']
    rating_keys_length = len(rating_keys)
    logging.info(f'Preparing top films based on: {rating_keys}')

    # Решил делать не через пандас, а через словари, 
    # т.к. по времени это в 2 раза быстрее выходит
    for film in films:
        film['meanRating'] = 0.0
        for key in rating_keys:
            film['meanRating'] += get_film_rating(film, key)

        film['meanRating'] /= rating_keys_length
        if film['meanRating'] >= DEFAULT_TOP_RATING:
            top_film = {'filmId': str(film.get('_id')), 'meanRating': film.get('meanRating')}
            top_films.append(top_film)

    return top_films


def map_user_likes(row):
    if row['state'] == 'LIKE' and row.get('listCode') == 'WATCHED':
        state = 2
    elif row['state'] == 'LIKE':
        state = 1
    elif row['state'] == 'DISLIKE' and row.get('listCode') == 'WATCHED':
        state = -2
    elif row['state'] == 'DISLIKE':
        state = -1
    return state


def prepare_user_activity(user_likes):
    logging.info('Preparing user activity')

    # Тут через пандас, потому что через словари будет намного больше кода :)))
    # Собираем данные по лайкам
    df_like = pd.DataFrame(user_likes)
    if len(df_like) > 0:
        no_anonym = False
        no_userid = False

        if 'anonymousId' not in df_like.columns:
            df_like['anonymousId'], no_anonym = None, True

        if 'userId' not in df_like.columns:
            df_like['userId'], no_userid = None, True

        if no_userid:
            df_like['uniqueId'] = df_like['anonymousId']
        elif no_anonym:
            df_like['uniqueId'] = df_like['userId']
        else:
            df_like['uniqueId'] = df_like['userId'].fillna(df_like['anonymousId'])

        df_like['state'] = df_like.apply(map_user_likes, axis=1)
    logging.info(f'Collected user_likes: {len(df_like)}')

    # Объединяем данные вместе
    if len(df_like) > 0:
        df_like = df_like[['uniqueId', 'userId', 'anonymousId', 'filmId', 'state']]

        # Исключаем пользователей, у которых меньше DEFAULT_ACTIVITY_TRIGGER_LIMIT оценок\лайков фильмов
        less_active_users = df_like.groupby('uniqueId')['filmId'].count()
        less_active_users = [user for user, likes in less_active_users.items() if likes < DEFAULT_ACTIVITY_TRIGGER_LIMIT]
        df_user_activity = df_like[~df_like['uniqueId'].isin(less_active_users)]
        df_user_activity = df_user_activity.drop('uniqueId', axis=1)

        logging.info(f'After filtering less active: {len(df_user_activity)}')
        return df_user_activity
    else:
        logging.info('No user_activity')
        return pd.DataFrame()


def prepare_user_recommendations(df_user_activity, user_id=None):
    logging.info('Start preparing user recommendations')

    # From ObjectId to String
    # Объединяем userId & anonymousId в одно поле -> так удобнее считать
    df_user_activity['userId'] = df_user_activity['userId'].fillna('ANON_' + df_user_activity['anonymousId'].astype('string'))
    df_user_activity['userId'] = df_user_activity['userId'].astype('string')

    # Вычисляем разряженную матрицу: в строках userId, в столбцах filmId, в значениях 1\0 (в зависимости от оценки)
    df_matrix = pd.pivot_table(df_user_activity, index='userId', columns='filmId', values='state', aggfunc='sum')
    df_matrix = df_matrix.fillna(0).reset_index()

    # Вычисляем расстояние между пользователями
    logging.info('Start calculating pairwise_distances between users')

    # Если передали userId, то вычисляем расстояние только м\у этим юзером и остальными, 
    # А не для всех пар юзеров
    # braycurtis, hamming, cosine
    metric = 'braycurtis'
    if user_id:
        user_key_id = 'ANON_' + user_id if len(df_matrix[df_matrix['userId']==user_id]) == 0 else user_id
    
        one_user = df_matrix[df_matrix['userId']==user_key_id]
        other_users = df_matrix[df_matrix['userId']!=user_key_id]
        
        user_similarity_values = pairwise_distances(one_user[one_user.columns[1:]], other_users[other_users.columns[1:]], metric=metric, n_jobs=-1)
        user_similarity = dict(zip(other_users['userId'], user_similarity_values.tolist()[0]))
        
        # Если одним методом ничё не нашли -> идём искать другим
        similar_users = [key for key, value in user_similarity.items() if 0.01 <= value <= DEFAULT_COSINE_LIMIT]
        if len(similar_users) == 0:
            df_matrix = pd.pivot_table(df_user_activity, index='filmId', columns='userId', values='state', aggfunc='sum')
            df_matrix = df_matrix.fillna(0).reset_index()

            one_user = df_matrix[user_key_id]
            other_users = df_matrix.drop(user_key_id, axis=1)

            user_similarity = other_users.corrwith(one_user).to_dict()
            user_similarity = dict(sorted(user_similarity.items(), reverse=True, key=lambda x: x[1]))
            user_similarity = {key:DEFAULT_COSINE_LIMIT for key in list(user_similarity.keys())[:10]}
            logging.debug(f'{user_similarity}')

        user_similarity = {user_key_id: user_similarity}
    else:
        user_similarity = pairwise_distances(df_matrix[df_matrix.columns[1:]], metric=metric, n_jobs=-1)
        user_similarity = pd.DataFrame(user_similarity, columns=df_matrix['userId'], index=df_matrix['userId'])
        user_similarity = user_similarity.to_dict('dict')

    logging.info('Done calculating pairwise_distances between users')

    # Группируем фильмы по пользователям, которые их лайкнули
    liked_films = df_user_activity[df_user_activity['state']>=1].groupby('userId')['filmId'].agg(lambda x: set(x)).to_dict()
    disliked_films = df_user_activity[df_user_activity['state']<0].groupby('userId')['filmId'].agg(lambda x: set(x)).to_dict()  

    # Для текущего юзера определяем похожих юзеров
    # И выбираем их фильмы, чтобы порекомендовать текущему
    logging.info('Start finding similar users and films')
    dict_user_recommendations = {}

    for current_user_id, value_dct in user_similarity.items():
        similar_users = [key for key, value in value_dct.items() if 0.01 <= value <= DEFAULT_COSINE_LIMIT]

        if bool(similar_users):
            # Берём фильмы текущего пользователя
            user_liked_films = liked_films.get(current_user_id, set())
            user_disliked_films = disliked_films.get(current_user_id, set())

            # Вытаскиевам фильмы похожих пользователей и убираем фильмы, кт юзер уже смотрел или лайкнул
            user_recommended_films = [film_id for user_id in similar_users for film_id in liked_films.get(user_id, [])]
            user_recommended_films = set(user_recommended_films) - user_liked_films - user_disliked_films
            dict_user_recommendations[current_user_id] = [user_recommended_films]

            if user_id:
                logging.info(f'Got {len(user_recommended_films)} recommendations for {current_user_id}')

    logging.info('Done finding similar users and films')
    return dict_user_recommendations


def process_user_recommendations(dict_user_recommendations):
    if bool(dict_user_recommendations):
        # Make user recommendations DataFrame
        df_users_films = pd.DataFrame.from_dict(dict_user_recommendations, orient='index').reset_index()
        df_users_films.columns = ['userId', 'filmId']
        df_users_films = df_users_films.explode('filmId')
        df_users_films = df_users_films[df_users_films['filmId'].isna()==False]
        logging.info('Put users recommendations in DataFrame')

        # Devide users and anonymus
        recs = df_users_films
        recs['anonymousId'] = recs['userId'].apply(lambda x: x.replace('ANON_', '') if 'ANON_' in x else None)
        recs['userId'] = recs['userId'].apply(lambda x: x if 'ANON_' not in x else None)
        logging.info('Devide users and anonymus - done')

        # From String To ObjectId
        recs['anonymousId'] = recs['anonymousId'].apply(lambda x: ObjectId(x) if x else None)
        recs['userId'] = recs['userId'].apply(lambda x: ObjectId(x) if x else None)
        recs = recs.to_dict('records')
        logging.info('Recommendations to records')
    
    else:
        recs = {}

    return recs
