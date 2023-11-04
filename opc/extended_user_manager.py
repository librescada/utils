from pathlib import Path
from typing import Union, Literal

from asyncua.crypto import uacrypto
from asyncua.server.users import User, UserRole
from librescada_utils.logger import logger
from librescada_utils.users_db import authentication, crud, schemas

# class AuthenticationUserManager:
#     """
#     Authentication user manager, takes an username and passwords, stores is in a user db and provides those users.
#     """
#
#     def __init__(self):
#         pass
#
#     async def add_role(self, user_role: UserRole, name: str, format: Union[str, None] = None):
#         pass
#
#     def get_user(self, iserver, username=None, password=None, certificate=None):
#         """
#         Default user_manager, does nothing much but check for admin
#         """
#         if username and iserver.allow_remote_admin and username in ("admin", "Admin"):
#             return User(role=UserRole.Admin)
#         else:
#             return User(role=UserRole.User)
#
#     async def add_user(self, certificate_path: Path, name: str, format: Union[str, None] = None):
#         await self.add_role(certificate_path=certificate_path, user_role=UserRole.User, name=name, format=format)
#
#     async def add_admin(self, certificate_path: Path, name: str, format: Union[str, None] = None):
#         await self.add_role(certificate_path=certificate_path, user_role=UserRole.Admin, name=name, format=format)
#

class CertificateUserManager:
    """
    Certificate user manager, takes a certificate handler with its associated users and provides those users.
    """

    def __init__(self, use_passwords: bool = False):
        self._trusted_certificates = {}
        self.use_passwords = use_passwords

    async def add_role(self, certificate_path: Path, user_role: UserRole, name: str, format: Union[str, None] = None):
        certificate = await uacrypto.load_certificate(certificate_path, format)
        if name is None:
            raise KeyError

        user = User(role=user_role, name=name, )

        if name in self._trusted_certificates:
            logger.warning(f"certificate with name {name} "
                           f"attempted to be added multiple times, only the last version will be kept.")
        self._trusted_certificates[name] = {'certificate': uacrypto.der_from_x509(certificate), 'user': user}

    def get_user(self, iserver, username=None, password=None, certificate=None):
        if certificate is None:
            return None
        correct_users = [prospective_certificate['user'] for prospective_certificate in
                         self._trusted_certificates.values()
                         if certificate == prospective_certificate['certificate']]
        if len(correct_users) == 0:
            return None

        if self.use_passwords:
            if username is None or password is None:
                return None

            user = authentication.authenticate_user(username=username, password=password)
            if not user:
                return None

            if user.name != correct_users[0].name:
                logger.warning(f"User {username} tried to login with certificate of user {correct_users[0].name}")
                return None
            else:
                return correct_users[0]

        else:
            return correct_users[0]

    async def add_user(self, certificate_path: Path, name: str, password: str | None = None,
                       role: Literal["user", "admin"] = 'user', format: str | None = None):
        if self.use_passwords and password is None:
            raise ValueError("Password must be provided if use_passwords is True")

        # Add user to the database
        db_user = crud.get_user_by_username(username=name)
        if db_user:
            if authentication.authenticate_user(name, password):
                logger.info(f"User {name} already exists in the database")
            else:
                logger.warning(
                    f"User {name} already exists in the database but the password is different to the one provided")

        else:
            user = schemas.UserCreate(username=name, password=password)
            crud.create_user(user)
            logger.info(f"User {name} created in the users database")

        # Add user to the server
        if role == 'user':
            await self.add_role(certificate_path=certificate_path, user_role=UserRole.User, name=name, format=format)
        elif role == 'admin':
            await self.add_role(certificate_path=certificate_path, user_role=UserRole.Admin, name=name, format=format)
        else:
            raise ValueError(f"Invalid role {role}, supported roles are 'user' and 'admin'")