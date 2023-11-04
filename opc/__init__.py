from asyncua import Server
from asyncua.common.structures104 import new_struct
from typing import Any
from models import Signal, Measurement, Input
from asyncua import ua

async def create_signal_type(server: Server, idx: ua.NodeId, signal: Any[Signal, Measurement, Input]):

    signal_fields = signal.to_ua_struct_fields()
    signal_node, _ = await new_struct(server, idx, signal.__name__, fields=signal_fields)

    return signal_node