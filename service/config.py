import os


def replace_if_none(value, default):
    if value is None:
        return default
    else:
        return value


### MONGO
MONGO_DB = replace_if_none(os.environ.get("MONGO_INITDB_DATABASE"), 'movie-gram')
MONGO_CONNECTION_LOGIN = replace_if_none(os.environ.get("MONGO_INITDB_ROOT_USERNAME"), "root")
MONGO_CONNECTION_PASS = replace_if_none(os.environ.get("MONGO_INITDB_ROOT_PASSWORD"), "pass")
MONGO_CONNECTION_HOST = replace_if_none(os.environ.get("MONGO_HOST"), "localhost")
MONGO_CONNECTION_PORT = replace_if_none(os.environ.get("MONGO_PORT"), "27017")
MONGO_CONNECTION_URI = f"mongodb://{MONGO_CONNECTION_LOGIN}" \
                       f":{MONGO_CONNECTION_PASS}@{MONGO_CONNECTION_HOST}:{MONGO_CONNECTION_PORT}"
MONGO_FILMS_TABLE = "film"
MONGO_FILMS_LIKES_TABLE = "like_dislike"
MONGO_USER_RECOMS_TABLE = "user_recommendations"
MONGO_FILMS_TOP_TABLE = "film_top"
MONGO_USER_ACTIVITY_TABLE = "user_activity"

### SERVICE
DEFAULT_TOP_LIMIT = 20
DEFAULT_TOP_RATING = 7
DEFAULT_COSINE_LIMIT = 0.4
DAFAULT_FILM_MISSED_RATING = 6
DEFAULT_ACTIVITY_TRIGGER_LIMIT = 20
LOGGIN_LEVEL = 'INFO'
