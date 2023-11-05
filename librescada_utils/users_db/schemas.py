from pydantic import BaseModel, model_validator

"""
Here data models for the programs that need to interact with the database are defined. The schemas are classes
equivalent to the `models`.

UserBase Pydantic models that have common attributes while creating or reading data.

and UserCreate that inherit from UserBase (so they will have the same attributes), 
plus any additional data (attributes) needed for creation (password field).

Pydantic's orm_mode will tell the Pydantic model to read the data even if it is not a dict, but an ORM model 
(or any other arbitrary object with attributes).

This way, instead of only trying to get the id value from a dict, as in:

id = data["id"]

it will also try to get it from an attribute, as in:

id = data.id

"""


class UserBase(BaseModel):
    """
    Base user model that contains basic information about the user. Common between creation and reading.
    """
    username: str
    email: str = "" # Not required
    role: str
    # organization: str = ""
    # ip_range_allowed: str = "0.0.0.*"

    class Config:
        from_attributes = True
    #     json_schema_extra = {
    #         "example": {
    #             "username": "test_user",
    #             "email": "test_user@psa.es",
    #         }
    #     }


class User(UserBase):
    """
    User model that contains any additional information about the user. This is the one used to read data from the db.
    """
    id: int
    # is_active: bool = False
    # disabled: bool = False
    # n_of_logins: int = None

    class Config:
        from_attributes = True


class UserCreate(UserBase):
    """
    User model that contains the hashed password. Used to store the user in the database.
    """
    password: str


class UserUpdate(UserBase):
    """
    User model that contains the user update attributes. Used to update the user in the database.
    """
    old_password: str = None
    new_password: str = None

    @model_validator(mode='after')
    def check_old_passwords_included(self) -> 'UserUpdate':
        if self.new_password is not None and self.old_password is None:
            raise ValueError('To update the password, you must provide the old password.')
        return self
