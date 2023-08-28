import pandas as pd
import logging
import math
from random import shuffle
from bson.decimal128 import Decimal128
from sklearn.metrics.pairwise import pairwise_distances
from config import DEFAULT_TOP_RATING, DEFAULT_COSINE_LIMIT, DEFAULT_ACTIVITY_TRIGGER_LIMIT
from config import DAFAULT_FILM_MISSED_RATING, DEFAULT_USER_ACTIVITY_LIMIT

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


def map_user_rating(rating):
    rating = float(rating.to_decimal())
    if rating >= 8:
        state = 2
    elif rating >= 6:
        state = 1
    elif rating >= 4:
        state = 0
    else:
        state = -1
    return state


def map_user_likes(row):
    if row['state'] == 'LIKE' and row.get('listCode') == 'WATCHED':
        state = 2
    elif row['state'] == 'LIKE':
        state = 1
    elif row['state'] == 'DISLIKE' and row.get('listCode') == 'WATCHED':
        state = -1
    elif row['state'] == 'DISLIKE':
        state = 0
    return state


def prepare_user_activity(user_ratings, user_likes):
    logging.info('Preparing user activity')

    # Тут через пандас, потому что через словари будет намного больше кода :)))
    # Собираем данные по рейтингам
    df_rating = pd.DataFrame(user_ratings)
    if len(df_rating) > 0:
        df_rating = df_rating.rename(columns={'rating':'state'})
        df_rating['state'] = df_rating['state'].apply(map_user_rating)
    logging.info(f'Collected user_ratings: {len(df_rating)}')

    # Собираем данные по лайкам
    df_like = pd.DataFrame(user_likes)
    if len(df_like) > 0:
        if 'anonymousId' in df_like.columns and 'userId' in df_like.columns:
            df_like['userId'] = df_like['userId'].fillna(df_like['anonymousId']) 
            df_like = df_like.drop('anonymousId', axis=1)
        elif 'anonymousId' in df_like.columns and 'userId' not in df_like.columns:
            df_like['userId'] = df_like['anonymousId']
            df_like = df_like.drop('anonymousId', axis=1)
        df_like['state'] = df_like.apply(map_user_likes, axis=1)
    logging.info(f'Collected user_likes: {len(df_like)}')

    # Объединяем данные вместе
    if len(df_like) > 0 or len(df_rating) > 0:
        df_all = pd.concat([df_like, df_rating], axis=0)
        df_all = df_all[['userId', 'filmId', 'state']]
        logging.info(f'Unioned data: {len(df_all)}')

        # Исключаем пользователей, у которых меньше DEFAULT_USER_ACTIVITY_LIMIT оценок\лайков фильмов
        less_active_users = df_all.groupby('userId')['filmId'].count()
        less_active_users = [user for user, likes in less_active_users.items() if likes < DEFAULT_USER_ACTIVITY_LIMIT]
        df_user_activity = df_all[~df_all['userId'].isin(less_active_users)]

        logging.info(f'After filtering less active: {len(df_user_activity)}')
        return df_user_activity
    else:
        logging.info('No user_activity')
        return pd.DataFrame()


def prepare_user_recommendations(df_user_activity, user_id=None, top_films=None):
    logging.info('Start preparing user recommendations')

    # Вычисляем разряженную матрицу: в строках userId, в столбцах filmId, в значениях 1\0 (в зависимости от оценки)
    df_matrix = pd.pivot_table(df_user_activity, index='userId', columns='filmId', values='state', aggfunc=lambda x: 1 if sum(x) >= 1 else 0)
    df_matrix = df_matrix.fillna(0).reset_index()

    # Вычисляем расстояние между пользователями
    logging.info('Start calculating pairwise_distances between users')

    # Если передали userId, то вычисляем расстояние только м\у этим юзером и остальными, 
    # А не для всех пар юзеров
    if user_id:
        first_user = df_matrix[df_matrix['userId']==user_id]
        other_users = df_matrix[df_matrix['userId']!=user_id]
        user_similarity_values = pairwise_distances(first_user[first_user.columns[1:]], other_users[other_users.columns[1:]], metric='cosine', n_jobs=-1)
        user_similarity = {user_id: dict(zip(other_users['userId'], user_similarity_values.tolist()[0]))}
    else:
        user_similarity = pairwise_distances(df_matrix[df_matrix.columns[1:]], metric='cosine', n_jobs=-1)
        user_similarity = pd.DataFrame(user_similarity, columns=df_matrix['userId'], index=df_matrix['userId'])
        user_similarity = user_similarity.to_dict('dict')

    logging.info('Done calculating pairwise_distances between users')

    # Группируем фильмы по пользователям, которые их лайкнули
    liked_films = df_user_activity[df_user_activity['state']>=1].groupby('userId')['filmId'].agg(lambda x: set(x)).to_dict()
    disliked_films = df_user_activity[df_user_activity['state']<=0].groupby('userId')['filmId'].agg(lambda x: set(x)).to_dict()  

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
            if user_id:
                logging.info(f'Got {len(user_recommended_films)} recommendations for {current_user_id}')

            recoms_len = len(user_recommended_films)
            if top_films and recoms_len < DEFAULT_ACTIVITY_TRIGGER_LIMIT:
                shuffle(top_films)
                user_top_films = list(set(top_films) - user_liked_films - user_disliked_films)
                user_top_films = set(user_top_films[:50-recoms_len])
                user_recommended_films = user_recommended_films ^ user_top_films

            dict_user_recommendations[current_user_id] = [user_recommended_films]

    logging.info('Done finding similar users and films')
    return dict_user_recommendations


def prepare_films_with_recommendations(dict_user_recommendations, films):
    # Make user recommendations DataFrame
    df_users_films = pd.DataFrame.from_dict(dict_user_recommendations, orient='index').reset_index()
    df_users_films.columns = ['userRecommendId', 'filmStringId']
    df_users_films = df_users_films.explode('filmStringId')
    df_users_films['filmStringId'] = df_users_films['filmStringId'].astype('string')
    df_users_films = df_users_films[df_users_films['filmStringId'].isna()==False]
    logging.info('Put users recommendations in DataFrame')

    # Make films DataFrame
    df_films = pd.DataFrame(films).rename(columns={'_id':'filmId'})
    df_films['filmStringId'] = df_films['filmId'].astype('string')
    # columns_to_drop = ['createdAt', 'updateAt', 'updatedAt']
    # df_films = df_films.drop(columns_to_drop, axis=1)
    logging.info('Put films in DataFrame')

    # Merge together
    film_recommendations = df_users_films.merge(df_films, how='inner', on='filmStringId')
    film_recommendations = film_recommendations.drop('filmStringId', axis=1)
    film_recommendations = film_recommendations.to_dict('records')
    logging.info('Merged user recommendations and films')

    return film_recommendations
