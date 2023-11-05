from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
# from sqlalchemy.orm import relationship

from .database import Base

"""
Here the database models (tables) for the users are defined. The models are classes that inherit from database.Base.

"""

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, default="")
    hashed_password = Column(String)
    role = Column(String, default="")
    # is_active = Column(Boolean, default=True)
    # n_of_logins = Column(Integer, default=0)
    # disabled = Column(Boolean, default=False)
    # organization = Column(String, default="")
    # items = relationship("Item", back_populates="owner")


# class Item(Base):
#     __tablename__ = "items"
#
#     id = Column(Integer, primary_key=True, index=True)
#     title = Column(String, index=True)
#     description = Column(String, index=True)
#     owner_id = Column(Integer, ForeignKey("users.id"))
#
#     owner = relationship("User", back_populates="items")