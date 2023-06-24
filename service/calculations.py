import pandas as pd
from sklearn.metrics.pairwise import pairwise_distances
from config import DEFAULT_TOP_RATING, DEFAULT_COSINE_LIMIT


def prepare_top_films(films):
     # Находим средний рейтинг
    df = pd.DataFrame(films)
    df['filmId'] = df['_id']
    df['meanRating'] = df['rating'] + df['ratingFilmCritics'] + df['ratingGoodReview']/10 + df['ratingImdb'] + df['ratingKinopoisk']
    df['meanRating'] /= 5

    df = df[df["meanRating"] >= DEFAULT_TOP_RATING][["filmId", "meanRating"]]
    films = df.to_dict('records') # Приводим датафрейм к виду списка словарей

    return films


def prepare_user_recommendations(user_ratings, user_likes):
    # Собираем данные по рейтингам
    df_rating = pd.DataFrame(user_ratings)
    df_rating = df_rating.rename(columns={'rating':'state'})
    df_rating['state'] = df_rating['state'].apply(lambda x: 1 if x >= 6 else 0)

    # Собираем данные по лайкам
    df_like = pd.DataFrame(user_likes)
    df_like['userId'] = df_like['userId'].fillna(df_like['anonymousId']) 
    df_like = df_like.drop('anonymousId', axis=1) #Можно триггерить юзеров зарегаться, чтобы получать рекомендации :)
    df_like['state'] = df_like['state'].map({'LIKE':1, 'DISLIKE':0})

    # Объединяем данные вместе
    df_all = pd.concat([df_like, df_rating], axis=0)

    # Исключаем пользователей, у которых меньше 10 оценок\лайков фильмов
    users_less_10_likes = df_all.groupby('userId')['filmId'].count()
    users_less_10_likes = [user for user, likes in users_less_10_likes.items() if likes < 10]
    df_all = df_all[~df_all['userId'].isin(users_less_10_likes)]

    # Вычисляем разряженную матрицу: в строках userId, в столбцах filmId, в значениях 1\0 (в зависимости от оценки)
    df_matrix = pd.pivot_table(df_all, index='userId', columns='filmId', values='state', aggfunc=lambda x: 1 if sum(x) >= 1 else 0)
    df_matrix = df_matrix.fillna(0).reset_index()

    # Вычисляем расстояние между пользователями
    user_similarity = pairwise_distances(df_matrix[df_matrix.columns[1:]], metric='cosine', n_jobs=-1)
    user_similarity = pd.DataFrame(user_similarity, columns=df_matrix['userId'])
    user_similarity = pd.concat([df_matrix['userId'], user_similarity], axis=1)
    user_similarity = user_similarity.to_dict('records')

    # Группируем фильмы по пользователям, которые их лайкнули
    liked_films = df_all[df_all['state']==1].groupby('userId')['filmId'].agg(lambda x: list(set(x))).to_dict()
    disliked_films = df_all[df_all['state']==0].groupby('userId')['filmId'].agg(lambda x: list(set(x))).to_dict()    

    # Находим похожих друг на друга юзеров
    list_user_matches = []
    for user in user_similarity:
    
        user_id = user.get('userId')
        user_match = {'userId':user_id}
        user_match['users'] = [key for key, value in user.items() if key != 'userId' and 0.05 < value <= DEFAULT_COSINE_LIMIT]
        
        user_liked_films = liked_films.get(user_id, [])
        user_disliked_films = disliked_films.get(user_id, [])
        user_match['films'] = [film for user in user_match['users'] for film in liked_films.get(user)]
        user_match['recommended_films'] = list(set(user_match['films'])-set(user_liked_films)-set(user_disliked_films))

        del user_match['users']
        del user_match['films']
        list_user_matches.append(user_match)

    return list_user_matches
