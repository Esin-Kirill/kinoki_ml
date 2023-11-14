from typing import Optional
from pydantic import BaseSettings, Field


class GlobalConfig(BaseSettings):
    """Global configurations."""

    # This variable will be loaded from the .env file. However, if there is a
    # shell environment variable having the same name, that will take precedence.

    # the class Field is necessary while defining the global variables
    ENV_STATE: Optional[str] = Field(..., env="ENV_STATE")

    # environment specific configs
    API_HOST: Optional[str] = None
    MONGO_DB: Optional[str] = None
    MONGO_CONNECTION_LOGIN: Optional[str] = None
    MONGO_CONNECTION_PASS: Optional[str] = None
    MONGO_CONNECTION_URI: Optional[str] = f"mongodb://{MONGO_CONNECTION_LOGIN}:{MONGO_CONNECTION_PASS}@mongodb:27017" \
                                            if ENV_STATE == 'prod' else "localhost:27017"
    MONGO_FILMS_TABLE: Optional[str] = None
    MONGO_FILMS_LIKES_TABLE: Optional[str] = None
    MONGO_USER_RECOMS_TABLE: Optional[str] = None
    MONGO_FILMS_TOP_TABLE: Optional[str] = None
    MONGO_USER_ACTIVITY_TABLE: Optional[str] = None
    MONGO_FILM_RECOMS_TABLE: Optional[str] = None

    ### SERVICE
    DEFAULT_TOP_LIMIT: Optional[int] = None
    DEFAULT_TOP_RATING: Optional[int] = None
    DEFAULT_COSINE_LIMIT: Optional[float] = None
    DEFAULT_FILM_MISSED_RATING: Optional[float] = None
    DEFAULT_ACTIVITY_TRIGGER_LIMIT: Optional[int] = None
    NUMBER_SIMILAR_FILMS: Optional[int] = None
    LOGGIN_LEVEL: Optional[str] = None

    class Config:
        """Loads the dotenv file."""

        env_file: str = ".env"


class DevConfig(GlobalConfig):
    """Development configurations."""

    class Config:
        env_prefix: str = "DEV_"


class ProdConfig(GlobalConfig):
    """Production configurations."""

    class Config:
        env_prefix: str = ""


class FactoryConfig:
    """Returns a config instance depending on the ENV_STATE variable."""

    def __init__(self, env_state: Optional[str]):
        self.env_state = env_state

    def __call__(self):
        if self.env_state == "dev":
            return DevConfig()

        elif self.env_state == "prod":
            return ProdConfig()

config = FactoryConfig(GlobalConfig().ENV_STATE)()
