from pydantic_settings import BaseSettings


class Settings(BaseSettings):

    url:str
    user:str

    class Config:
        env_file = ".env"


settings = Settings()