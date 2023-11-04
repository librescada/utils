from typing import Dict, Any

from librescada_utils.logger import logger
# import pprint
# from typing import Dict, Any

from passlib.context import CryptContext
# from pydantic import BaseModel

import crud, schemas
# from database import SessionLocal, get_db


"""
From [FastAPI documentation](https://fastapi.tiangolo.com/tutorial/security/get-current-user/)
"""

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Function that a hashed password stored in the db matches the input plain password.
    """
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(plain_password: str) -> str:
    """
    Function that hashes a password using bcrypt.
    """
    return pwd_context.hash(plain_password)


def get_user(username: str):

    user = crud.get_user(username)
    if user:
        return schemas.User(**user.__dict__)


def authenticate_user(username: str, password: str):

    user = crud.get_user(username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


def create_users_dict(credentials_content) -> Dict[str, Any]:

    def check_and_complete(key, value, suffix, property):
        if key.endswith(suffix):
            # If the key ends with suffix, store the username
            base_key = key.replace(suffix, "")

            if base_key not in credentials_temp:
                credentials_temp[base_key] = {}

            credentials_temp[base_key][property] = value

    # Initialize an empty dictionary to store username-password pairs
    credentials_temp = {}

    for line in credentials_content:
        # Split each line by "=" to separate the key and value
        parts = line.strip().split("=")

        if len(parts) == 2:
            key, value = parts[0], parts[1]

            if key.endswith("_USERNAME"):
                # If the key ends with "_USERNAME", store the username
                check_and_complete(key, value, "_USERNAME", "username")

            elif key.endswith("_PASSWORD"):
                # If the key ends with "_PASSWORD", store the password
                check_and_complete(key, value, "_PASSWORD", "password")

            elif key.endswith("_ROLE"):
                # If the key ends with "_ROLE", store the role
                check_and_complete(key, value, "_ROLE", "role")

    # For each key that has both username and password, store the pair in the credentials dictionary
    user_credentials = {}
    for key, data in credentials_temp.items():
        # Incluir role
        if "username" in data and "password" in data:
            user_credentials[data["username"]] = data["password"]
        else:
            logger.error(f"Key {key} does not have both username and password")

    # print(user_credentials)

    return user_credentials


def create_usersdb_from_credentials(credentials) -> Dict[str, Any]:
    user_credentials = create_users_dict(credentials)

    # At this point, user_credentials will contain the pairs of usernames and passwords
    # Incluir role!
    for username, password in user_credentials.items():
        # Check if user already exists in the database
        db_user = crud.get_user_by_username(username=username)
        if db_user:
            if authenticate_user(username, password):
                logger.info(f"User {username} already exists in the database")
            else:
                logger.warning(f"User {username} already exists in the database but the password is different to the one provided")

        else:
            user = schemas.UserCreate(username=username, password=password)
            crud.create_user(user)
            logger.info(f"User {username} created in the database")

    return user_credentials

# def password_generator():
#     return ''.join(random.choice(string.ascii_letters + string.digits) for i in range(20))