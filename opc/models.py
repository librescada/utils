from typing import Any, List
from pydantic import field_validator, model_validator, BaseModel, Field, ValidationInfo
from asyncua import ua
from asyncua.common.structures104 import new_struct_field

class Signal(BaseModel):
    """
    Signal model that contains the common information about any signal
    """
    """
    Signal model that contains the common information about any signal
    """
    signal_id: str = Field(..., description="Signal ID. Primary key, used to identify the signal across modules")
    var_id: str = Field(description="Variable ID. Unique, natural name used to refer to the signal (md, Ts_in, Qs, etc)")
    unit: str = Field(description="used to specify the unit of the magnitude being measured (kg/s, ºC, m³/h, etc)")
    description: str = Field(default=None, description="Description of the signal")
    num_type: Any['float', 'int', 'bool', 'str'] = Field(default=None, description="The type of the value of the signal", examples=['float', 'int', 'bool', 'str'])
    value: Any[float, int, bool, str, ua.DataValue] = Field(default=None, description="The value of the signal as a datavalue to avoid having to specify the specifc type", validate_default=False)

    @field_validator('signal_id', 'var_id')
    @classmethod
    def check_alphanumeric_and_no_whitespaces(cls, v: str, info: ValidationInfo) -> str:
        if isinstance(v, str):
            # info.field_name is the name of the field being validated
            is_alphanumeric = v.replace(' ', '').isalnum()
            assert is_alphanumeric, f'{info.field_name} must be alphanumeric'

            has_whitespaces = ' ' in v
            assert not has_whitespaces, f'{info.field_name} must not contain whitespaces'
        return v

    @field_validator('num_type')
    @classmethod
    def check_supported_type(cls, value):
        supported_num_types = ['float', 'int', 'bool', 'str']
        if value not in supported_num_types:
            raise ValueError(f"Unsupported numeric type {value}, supported types are: {supported_num_types}")
        return value

    @model_validator(mode='after')
    def if_value_provided_set_num_type(self) -> 'Signal':
        if self.value and not self.num_type:
            self.num_type = type(self.value).__name__
        return self

    @field_validator('num_type')
    @classmethod
    def convert_to_ua_type(cls, value):
        if value == 'float':
            return ua.VariantType.Double
        elif value == 'int':
            return ua.VariantType.Int64
        elif value == 'bool':
            return ua.VariantType.Boolean
        elif value == 'str':
            return ua.VariantType.String

    @model_validator(mode='after')
    def datavalue_from_value(self) -> 'Signal':
        if self.value:
            self.value = ua.DataValue(ua.Variant(self.value, self.num_type))
        return self

    # def __post_init__(self, **kwargs):
    #     # Set any attribute passed in kwargs, since it's coming from validated schemas it should be safe
    #     for key, value in kwargs.items():
    #         setattr(self, key, value)

    def to_ua_struct_fields(self) -> List[ua.StructureField]:
        fields = []
        for field_name, field_value in self.__dict__.items():
            field_info = self.model_fields.get(field_name)

            fields.append(
                new_struct_field(name=field_name,
                                 dtype=self.convert_to_ua_type(field_info.annotation),
                                 array=True if isinstance(field_value, list) else False,
                                 optional=not field_info.required,
                                 description=field_info.description),
            )

        return fields

class Measurement(Signal):
    sensor_id: str = Field(description="Used to identify the sensor that generated the measurement (flow_meter_EH_Proline_Promag_P300, level_meter_WIKA_LH10, etc).")
    sensor_type: str = Field(description="Used to identify the type of sensor that generated the measurement (vortex flow sensor, magnetic level meter, etc).")


class Input(Signal):
    """
    Input model that contains the common information about any input
    """
    input_id: str = Field(description="Used to identify the actuator/object type that generated the input (vfd_distillate, autom_valve_brine, controller_distillate_level, etc).")
    input_type: str = Field(description="Used to identify the type of input: frequency, valve aperture, controller setpoint, etc.")
    range: List[Any[float, int]] = Field(default=None, description="Operating range of the input")

    @field_validator('range')
    @classmethod
    def check_range(cls, value):
        if value and len(value) != 2:
            raise ValueError("Range must be a list of 2 values: [min, max]")
        return value

    @model_validator(mode='after')
    def check_range(self) -> 'Input':
        if self.range and self.value:
            if not (self.range[0] <= self.value <= self.range[1]):
                raise ValueError(f"Value {self.value} is not within range {self.range}")
        return self
