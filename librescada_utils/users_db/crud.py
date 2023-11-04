from sqlalchemy.orm import Session
from passlib.context import CryptContext
from librescada_utils.logger import logger
import models, schemas
from database import get_db
from authentication import get_password_hash, verify_password

"""
CRUD (Create, Read, Update, and Delete), it contains reusable functions to interact with the data in the database. 
"""

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def get_password_hash(password):
    # utility function to hash a password coming from the user
    return pwd_context.hash(password)


def get_user(username: str):
    #
    with get_db() as db:
        return db.query(models.User).filter(models.User.username == username).first()


def get_user_by_username(username: str):
    #
    with get_db() as db:
        return db.query(models.User).filter(models.User.username == username).first()


def get_user_by_email(email: str):
    #
    with get_db() as db:
        return db.query(models.User).filter(models.User.email == email).first()


def create_user(user: schemas.UserCreate):
    db_user = models.User(email=user.email,
                          hashed_password=get_password_hash(user.password),
                          username=user.username,
                          )

    with get_db() as db:
        db.add(db_user)
        db.commit()
        db.refresh(db_user)

    return db_user


def update_user(username: str, updated_user_data: schemas.UserUpdate):

    with get_db() as db:
        db_user = db.query(models.User).filter(models.User.username == username).first()
        if db_user:
            # Password change
            if updated_user_data.new_password:
                # First verify the provided "old_password" matches the current password

                if not verify_password(updated_user_data.old_password, db_user.hashed_password):
                    logger.error(f"Provided old password does not match the current password for user {username}")
                    return None
                else:
                    db_user.hashed_password = get_password_hash(updated_user_data.new_password)
                    # Remove the password from the updated_user_data
                    del updated_user_data.new_password
                    del updated_user_data.old_password

            for key, value in updated_user_data.model_dump().items():
                setattr(db_user, key, value)
            db.commit()
            db.refresh(db_user)

            logger.info(f"User {username} updated successfully")

            return db_user

        return None

# def user_reset_active(db, username):
#
#     db_user = get_user(db, username)
#     db_user.is_active = False
#     db.commit()
#
#     logger.info(f"User {username} state set to inactive")
#
#
# def user_set_active(db, username):
#     db_user = get_user(db, username)
#     db_user.is_active = True
#     db_user.n_of_logins += 1
#     db.commit()
#
#     logger.info(f"User {username} state set active")

# def get_items(db: Session, skip: int = 0, limit: int = 100):
#     return db.query(models.Item).offset(skip).limit(limit).all()


# def create_user_item(db: Session, item: schemas.ItemCreate, user_id: int):
#     db_item = models.Item(**item.dict(), owner_id=user_id)
#     db.add(db_item)
#     db.commit()
#     db.refresh(db_item)
#     return db_ite