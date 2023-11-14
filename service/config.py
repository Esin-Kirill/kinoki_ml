import os
from typing import Optional
from dotenv import load_dotenv


class GlobalConfig():
    """Global configurations."""

    # This variable will be loaded from the .env file. However, if there is a
    # shell environment variable having the same name, that will take precedence.

    # the class Field is necessary while defining the global variables
    load_dotenv()
    ENV_STATE: Optional[str] = os.environ.get('ENV_STATE', 'dev')

    # environment specific configs
    API_HOST: Optional[str] = "0.0.0.0"
    MONGO_INITDB_DATABASE: Optional[str] = os.environ.get('MONGO_INITDB_DATABASE')
    MONGO_INITDB_ROOT_USERNAME: Optional[str] = os.environ.get('MONGO_INITDB_ROOT_USERNAME')
    MONGO_INITDB_ROOT_PASSWORD: Optional[str] = os.environ.get('MONGO_INITDB_ROOT_PASSWORD')
    MONGO_CONNECTION_URI: Optional[str] = f"mongodb://{MONGO_INITDB_ROOT_USERNAME}:{MONGO_INITDB_ROOT_PASSWORD}@mongodb:27017" \
                                            if ENV_STATE == 'prod' else "localhost:27017"
    MONGO_FILMS_TABLE: Optional[str] = "film"
    MONGO_FILMS_LIKES_TABLE: Optional[str] = "like_dislike"
    MONGO_USER_RECOMS_TABLE: Optional[str] = "ml_user_recommendations"
    MONGO_FILMS_TOP_TABLE: Optional[str] = "ml_film_top"
    MONGO_USER_ACTIVITY_TABLE: Optional[str] = "ml_user_activity"
    MONGO_FILM_RECOMS_TABLE: Optional[str] = "ml_film_recommendations"
    MONGO_USER_PROFILES: Optional[str] = "ml_user_profiles"

    ### SERVICE
    DEFAULT_TOP_LIMIT: Optional[int] = int(os.environ.get('DEFAULT_TOP_LIMIT'))
    DEFAULT_TOP_RATING: Optional[int] = int(os.environ.get('DEFAULT_TOP_RATING'))
    DEFAULT_COSINE_LIMIT: Optional[float] = float(os.environ.get('DEFAULT_COSINE_LIMIT'))
    DEFAULT_FILM_MISSED_RATING: Optional[float] = float(os.environ.get('DEFAULT_FILM_MISSED_RATING'))
    DEFAULT_ACTIVITY_TRIGGER_LIMIT: Optional[int] = int(os.environ.get('DEFAULT_ACTIVITY_TRIGGER_LIMIT'))
    NUMBER_SIMILAR_FILMS: Optional[int] = int(os.environ.get('NUMBER_SIMILAR_FILMS'))
    LOGGIN_LEVEL: Optional[str] = os.environ.get('LOGGIN_LEVEL')
    NUMBER_QUERY_FILMS: Optional[int] = int(os.environ.get('NUMBER_QUERY_FILMS'))


config = GlobalConfig()
