from typing import Any
from asyncua import ua
from asyncua.common.structures104 import new_struct, new_struct_field

from librescada_utils.logger import logger
import models, schemas

"""
CRUD (Create, Read, Update, and Delete), it contains reusable functions to interact with the data in the opc server. 
"""


