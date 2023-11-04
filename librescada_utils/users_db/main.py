from librescada_utils.logger import logger
import argparse
import os
from pydantic import ValidationError

import crud, schemas, authentication
# from database import engine
# import  # import create_users_from_credentials

"""
    This modules contains the functions that interact with the database. Each module that requires
    to interact with the users database should perform equivalent operations to the ones in this
    module.
"""

default_credentials_path = '/run/secrets/credentials.txt'

argparser = argparse.ArgumentParser()
argparser.add_argument('--credentials_file', help='Path to the credentials .txt file that holds user '
                       'information, optionally set in the environment variable CREDENTIALS_FILE', required=False,
                       default=os.getenv("CREDENTIALS_FILE", default=default_credentials_path))
# argparser.add_argument('--db_url', help='URL to the database, optionally set in the environment variable '
#                                         'USERSDB_DATABASE_URL', required=False,
#                        default=os.getenv("USERSDB_DATABASE_URL", f"sqlite:///{default_db_path}"))
args = argparser.parse_args()


"""
 A note about the database initialization:
 The behavior of create_all with respect to an existing database can be summarized as follows:

    1. If the database does not exist, create_all will create it and then create the necessary tables based on your 
    model definitions.
    2. If the database already exists but the tables defined in your models do not exist, create_all will create the 
    tables in the existing database.
    3. If the database and the tables already exist, create_all will not make any changes to the database structure. 
    It will not recreate the tables or modify them in any way. It will leave the existing tables intact.

If the database specified in engine already exists and the tables defined in your models are also present, running 
models.Base.metadata.create_all(bind=engine) will not affect the existing database or tables. It will simply do nothing 
in this case. This behavior is useful during development when you may frequently update your data models and want to 
apply those changes to the database structure without manually managing the schema. However, in a production environment, 
you would typically use database migration tools (e.g., Alembic for SQLAlchemy) to manage database schema changes more 
robustly and safely, as create_all does not handle schema migrations.
"""

def main():

    with open(args.credentials_file) as f:
        credentials = f.readlines()

    authentication.create_usersdb_from_credentials(credentials)

    # Update an existing user
    test_user = 'webscada_user'
    old_password = 'o4ufN1trnAr0J40Ure69'

    # Try to authenticate with the old password
    user = authentication.authenticate_user(username=test_user, password=old_password)
    if user:
        logger.info(f"User {test_user} authenticated successfully")
    else:
        logger.info(f"User {test_user} not authenticated")

    # Should raise an error because old_password is not provided
    try:
        updated_user = schemas.UserUpdate(username=test_user, new_password='new_super_duper_secure_password', email='atxuiii@asd.com')
    except ValidationError as e:
        logger.error(e)

    updated_user = schemas.UserUpdate(username=test_user, old_password='new_super_duper_secure_password', new_password='new_super_duper_secure_password', email='laviiin@asd.com')
    crud.update_user(username=test_user, updated_user_data=updated_user)



if __name__=="__main__":
    main()
