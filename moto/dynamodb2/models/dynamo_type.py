from moto.dynamodb2.comparisons import get_comparison_func
from moto.dynamodb2.exceptions import IncorrectDataType
from moto.dynamodb2.models.utilities import bytesize


class DDBType(object):
    """
    Official documentation at https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html
    """

    BINARY_SET = "BS"
    NUMBER_SET = "NS"
    STRING_SET = "SS"
    STRING = "S"
    NUMBER = "N"
    MAP = "M"
    LIST = "L"
    BOOLEAN = "BOOL"
    BINARY = "B"
    NULL = "NULL"


class DDBTypeConversion(object):
    _human_type_mapping = {
        val: key.replace("_", " ")
        for key, val in DDBType.__dict__.items()
        if key.upper() == key
    }

    @classmethod
    def get_human_type(cls, abbreviated_type):
        """
        Args:
            abbreviated_type(str): An attribute of DDBType

        Returns:
            str: The human readable form of the DDBType.
        """
        return cls._human_type_mapping.get(abbreviated_type, abbreviated_type)


class DynamoType(object):
    """
    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModelDataTypes
    """

    def __init__(self, type_as_dict):
        if type(type_as_dict) == DynamoType:
            self.type = type_as_dict.type
            self.value = type_as_dict.value
        else:
            self.type = list(type_as_dict)[0]
            self.value = list(type_as_dict.values())[0]
        if self.is_list():
            self.value = [DynamoType(val) for val in self.value]
        elif self.is_map():
            self.value = {k: DynamoType(v) for k, v in self.value.items()}

    def filter(self, projection_expressions):
        nested_projections = [
            expr[: expr.index(".")]
            for expr in projection_expressions
            if "." in expr
        ]

        if self.is_map():
            expressions_to_delete = []
            for attr in self.value:
                if (
                    attr not in projection_expressions
                    and attr not in nested_projections
                ):
                    expressions_to_delete.append(attr)
                elif attr in nested_projections:
                    relevant_expressions = [
                        expr[len(f"{attr}.") :]
                        for expr in projection_expressions
                        if expr.startswith(f"{attr}.")
                    ]

                    self.value[attr].filter(relevant_expressions)
            for expr in expressions_to_delete:
                self.value.pop(expr)

    def __hash__(self):
        return hash((self.type, self.value))

    def __eq__(self, other):
        return self.type == other.type and self.value == other.value

    def __ne__(self, other):
        return self.type != other.type or self.value != other.value

    def __lt__(self, other):
        return self.cast_value < other.cast_value

    def __le__(self, other):
        return self.cast_value <= other.cast_value

    def __gt__(self, other):
        return self.cast_value > other.cast_value

    def __ge__(self, other):
        return self.cast_value >= other.cast_value

    def __repr__(self):
        return "DynamoType: {0}".format(self.to_json())

    def __add__(self, other):
        if self.type != other.type:
            raise TypeError("Different types of operandi is not allowed.")
        if not self.is_number():
            raise IncorrectDataType()
        self_value = float(self.value) if "." in self.value else int(self.value)
        other_value = float(other.value) if "." in other.value else int(other.value)
        return DynamoType(
            {DDBType.NUMBER: "{v}".format(v=self_value + other_value)}
        )

    def __sub__(self, other):
        if self.type != other.type:
            raise TypeError("Different types of operandi is not allowed.")
        if self.type != DDBType.NUMBER:
            raise TypeError("Sum only supported for Numbers.")
        self_value = float(self.value) if "." in self.value else int(self.value)
        other_value = float(other.value) if "." in other.value else int(other.value)
        return DynamoType(
            {DDBType.NUMBER: "{v}".format(v=self_value - other_value)}
        )

    def __getitem__(self, item):
        if (
            isinstance(item, str)
            and self.type == DDBType.MAP
            or not isinstance(item, str)
            and isinstance(item, int)
            and self.type == DDBType.LIST
        ):
            return self.value[item]
        raise TypeError(
            "This DynamoType {dt} is not subscriptable by a {it}".format(
                dt=self.type, it=type(item)
            )
        )

    def __setitem__(self, key, value):
        if isinstance(key, int):
            if self.is_list():
                if key >= len(self.value):
                    # DynamoDB doesn't care you are out of box just add it to the end.
                    self.value.append(value)
                else:
                    self.value[key] = value
        elif isinstance(key, str):
            if self.is_map():
                self.value[key] = value
        else:
            raise NotImplementedError("No set_item for {t}".format(t=type(key)))

    @property
    def cast_value(self):
        if self.is_number():
            try:
                return int(self.value)
            except ValueError:
                return float(self.value)
        elif self.is_set():
            sub_type = self.type[0]
            return {DynamoType({sub_type: v}).cast_value for v in self.value}
        elif self.is_list():
            return [DynamoType(v).cast_value for v in self.value]
        elif self.is_map():
            return dict([(k, DynamoType(v).cast_value) for k, v in self.value.items()])
        else:
            return self.value

    def child_attr(self, key):
        """
        Get Map or List children by key. str for Map, int for List.

        Returns DynamoType or None.
        """
        if isinstance(key, str) and self.is_map() and key in self.value:
            return DynamoType(self.value[key])

        if isinstance(key, int) and self.is_list():
            idx = key
            if 0 <= idx < len(self.value):
                return DynamoType(self.value[idx])

        return None

    def size(self):
        if self.is_number():
            return len(str(self.value))
        elif self.is_set():
            sub_type = self.type[0]
            return sum(DynamoType({sub_type: v}).size() for v in self.value)
        elif self.is_list():
            return sum(v.size() for v in self.value)
        elif self.is_map():
            return sum(bytesize(k) + DynamoType(v).size() for k, v in self.value.items())
        elif type(self.value) == bool:
            return 1
        else:
            return bytesize(self.value)

    def to_json(self):
        return {self.type: self.value}

    def compare(self, range_comparison, range_objs):
        """
        Compares this type against comparison filters
        """
        range_values = [obj.cast_value for obj in range_objs]
        comparison_func = get_comparison_func(range_comparison)
        return comparison_func(self.cast_value, *range_values)

    def is_number(self):
        return self.type == DDBType.NUMBER

    def is_set(self):
        return self.type in (DDBType.STRING_SET, DDBType.NUMBER_SET, DDBType.BINARY_SET)

    def is_list(self):
        return self.type == DDBType.LIST

    def is_map(self):
        return self.type == DDBType.MAP

    def same_type(self, other):
        return self.type == other.type

    def pop(self, key, *args, **kwargs):
        if self.is_map() or self.is_list():
            self.value.pop(key, *args, **kwargs)
        else:
            raise TypeError("pop not supported for DynamoType {t}".format(t=self.type))
