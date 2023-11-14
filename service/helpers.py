import re
from collections import Counter
from bson import ObjectId


def update_user_profile(profile, film, like):
    # Profile
    prf_genres = profile.get("genres", {})
    prf_countries = profile.get("countries", {})
    prf_director = profile.get("directors", {})
    prf_actors = profile.get("actors", {})
    prf_texts = profile.get("texts", [])

    # Ger film features
    film_genres = {genre:1 for genre in film.get("genres", [])[:3]}
    film_countries = {country:1 for country in film.get("countries", [])[:2]}

    # Не могут бы ключи типа ObjectId, поэтому приводим к строке
    film_director = {str(person["personId"]): 1 for person in film.get("staff", [])[:1] if person["proffession"]=="DIRECTOR"}
    film_actors = {str(person["personId"]): 1 for person in film.get("staff", [])[:4] if person["proffession"]=="ACTOR"}
    film_text = clear_text(film.get("nameRu", '') + ' ' + film.get("nameOriginal", ''))

    # Union film & profile
    new_genres = dict(Counter(prf_genres) + Counter(film_genres))
    new_countries = dict(Counter(prf_countries) + Counter(film_countries))
    new_director = dict(Counter(prf_director) + Counter(film_director))
    new_actors = dict(Counter(prf_actors) + Counter(film_actors))
    new_texts = prf_texts + film_text

    # Update profile
    profile['userId'] = like.get("userId")
    profile['anonymousId'] = like.get("anonymousId")
    profile['genres'] = new_genres
    profile['countries'] = new_countries
    profile['directors'] = new_director
    profile['actors'] = new_actors
    profile['texts'] = new_texts

    return profile


def update_watched_films(profile, like):
    profile['watchedFilms'] = profile.get('watchedFilms', []) + [like.get('filmId')]
    return profile


def clear_text(text):
    text = text.strip()    
    text = re.sub(r"[^а-яa-zё0-9]", ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub('\xa0', ' ', text)
    text = text.lower()
    text = [word for word in text.split(' ') if len(word)>3]
    return text


def get_filter(user_profile):
    # Get filters for Mongo
    genres = sorted(user_profile.get("genres", {}).items(), key=lambda x: x[1], reverse=True)[:5]
    countries = sorted(user_profile.get("countries", {}).items(), key=lambda x: x[1], reverse=True)[:5]
    directors = sorted(user_profile.get("directors", {}).items(), key=lambda x: x[1], reverse=True)[:10]
    actors = sorted(user_profile.get("actors", {}).items(), key=lambda x: x[1], reverse=True)[:10]
    
    # Get
    genres = [genre[0] for genre in genres]
    countries = [country[0] for country in countries]
    directors = [ObjectId(director[0]) for director in directors]
    actors = [ObjectId(actor[0]) for actor in actors]

    # Make find query
    find_query = {
        "_id": {"$nin": user_profile.get("watchedFilms", [])},
        "$or": [
            {"genres": {"$in": genres}}, 
            {"countries": {"$in": countries}},
            {"staff.personId": {"$in": directors+actors}},
        ]
    }
    return find_query


def get_film_similarity(film, user_profile):
    # Film
    film_genres = {genre:1 for genre in film.get("genres", [])[:3]}
    film_countries = {country:1 for country in film.get("countries", [])[:2]}

    # Не могут бы ключи типа ObjectId, поэтому приводим к строке
    film_directors = {str(person["personId"]): 1 for person in film.get("staff", [])[:1] if person["proffession"]=="DIRECTOR"}
    film_actors = {str(person["personId"]): 1 for person in film.get("staff", [])[:10] if person["proffession"]=="ACTOR"}
    film_text = clear_text(film.get("nameRu", '') + ' ' + film.get("nameOriginal", ''))

    # User
    user_genres = user_profile.get("genres")
    user_countries = user_profile.get("countries")
    user_directors = user_profile.get("directors")
    user_actors = user_profile.get("actors")
    user_texts = user_profile.get("texts")

    # Similarity
    similarity = len(set(film_genres)&set(user_genres))
    similarity += len(set(film_countries)&set(user_countries))
    similarity += len(set(film_directors)&set(user_directors))
    similarity += len(set(film_actors)&set(user_actors))
    similarity += len(set(film_text)&set(user_texts))
    
    return film.get('_id'), round(similarity, 2)
