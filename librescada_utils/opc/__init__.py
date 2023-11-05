from asyncua import Server
from asyncua.common.structures104 import new_struct
from typing import Any, Literal
from .models import Signal, Measurement, Input
from asyncua import ua
# from asyncua.common.ua_utils import val_to_string,
from .utils import convert_to_ua_type
from asyncua.common.structures104 import new_struct_field



async def create_signal_type(server: Server, idx: ua.NodeId, signal: Literal[Signal, Measurement, Input]):
    fields = []
    for field_name, field_value in signal.model_fields.items():
        fields.append(
            new_struct_field(name=field_name,
                             dtype=convert_to_ua_type(field_value.annotation),
                             array=True if isinstance(field_value, list) else False,
                             optional=not field_value.is_required(),
                             description=field_value.description),
        )

    signal_node, _ = await new_struct(server, idx, signal.__name__, fields=fields)

    return signal_node