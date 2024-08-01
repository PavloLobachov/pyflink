from pyflink.common.typeinfo import (
    TypeInformation,
    BasicTypeInfo,
    RowTypeInfo,
    MapTypeInfo,
    ListTypeInfo,
    SqlTimeTypeInfo,
    PrimitiveArrayTypeInfo,
    BasicArrayTypeInfo,
    ObjectArrayTypeInfo,
    LocalTimeTypeInfo,
    InstantTypeInfo, Types
)


class UnsupportedTypeError(Exception):
    pass


def type_info_to_sql(type_info: TypeInformation) -> str:
    if isinstance(type_info, BasicTypeInfo):
        if type_info == BasicTypeInfo.STRING_TYPE_INFO():
            return "STRING"
        elif type_info == BasicTypeInfo.BYTE_TYPE_INFO():
            return "TINYINT"
        elif type_info == BasicTypeInfo.BOOLEAN_TYPE_INFO():
            return "BOOLEAN"
        elif type_info == BasicTypeInfo.SHORT_TYPE_INFO():
            return "SMALLINT"
        elif type_info == BasicTypeInfo.INT_TYPE_INFO():
            return "INT"
        elif type_info == BasicTypeInfo.LONG_TYPE_INFO():
            return "BIGINT"
        elif type_info == BasicTypeInfo.FLOAT_TYPE_INFO():
            return "FLOAT"
        elif type_info == BasicTypeInfo.DOUBLE_TYPE_INFO():
            return "DOUBLE"
        elif type_info == BasicTypeInfo.CHAR_TYPE_INFO():
            return "CHAR"
        elif type_info == BasicTypeInfo.BIG_INT_TYPE_INFO():
            return "DECIMAL"
        elif type_info == BasicTypeInfo.BIG_DEC_TYPE_INFO():
            return "DECIMAL"
        elif type_info == BasicTypeInfo.INSTANT_TYPE_INFO():
            return "TIMESTAMP"

    elif isinstance(type_info, SqlTimeTypeInfo):
        if type_info == SqlTimeTypeInfo.DATE():
            return "DATE"
        elif type_info == SqlTimeTypeInfo.TIME():
            return "TIME"
        elif type_info == SqlTimeTypeInfo.TIMESTAMP():
            return "TIMESTAMP"

    elif isinstance(type_info, LocalTimeTypeInfo):
        if type_info == LocalTimeTypeInfo(LocalTimeTypeInfo.TimeType.LOCAL_DATE):
            return "DATE"
        elif type_info == LocalTimeTypeInfo(LocalTimeTypeInfo.TimeType.LOCAL_TIME):
            return "TIME"
        elif type_info == LocalTimeTypeInfo(LocalTimeTypeInfo.TimeType.LOCAL_DATE_TIME):
            return "TIMESTAMP"

    elif isinstance(type_info, RowTypeInfo):
        fields = ', '.join(
            f"{name} {type_info_to_sql(field)}"
            for name, field in zip(type_info.get_field_names(), type_info.get_field_types())
        )
        return f"ROW<{fields}>"

    elif isinstance(type_info, MapTypeInfo):
        key_type = type_info_to_sql(type_info._key_type_info)
        value_type = type_info_to_sql(type_info._value_type_info)
        return f"MAP<{key_type}, {value_type}>"

    elif isinstance(type_info, ListTypeInfo):
        element_type = type_info_to_sql(type_info.elem_type)
        return f"ARRAY<{element_type}>"

    elif isinstance(type_info, PrimitiveArrayTypeInfo):
        element_type = type_info_to_sql(type_info._element_type)
        return f"ARRAY<{element_type}>"

    elif isinstance(type_info, BasicArrayTypeInfo):
        element_type = type_info_to_sql(type_info._element_type)
        return f"ARRAY<{element_type}>"

    elif isinstance(type_info, ObjectArrayTypeInfo):
        element_type = type_info_to_sql(type_info._element_type)
        return f"ARRAY<{element_type}>"

    elif isinstance(type_info, InstantTypeInfo):
        return "TIMESTAMP"

    raise UnsupportedTypeError(f"Unsupported TypeInformation: {type_info}")


def flink_schema_to_row(flink_schema: dict[str, TypeInformation]) -> RowTypeInfo:
    field_names = list(flink_schema.keys())
    field_types = list(flink_schema.values())
    return Types.ROW_NAMED(field_names, field_types)