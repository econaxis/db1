import copy
from dataclasses import dataclass

from llvmlite import ir


@dataclass(init = False)
class Type:
    type: list['Type'] | str

    def into_ir_type(self) -> ir.Type:
        match self.type:
            case "int":
                return ir.IntType(64)
            case "float":
                return ir.DoubleType()
            case _:
                raise RuntimeError


    def __init__(self, *args):
        match args:
            case [Type() as t]:
                self.type = t.type
            case [str() as s]:
                self.type = s
            case [[*params]] | [*params]:
                self.type = [Type(p) if isinstance(p, str) else p for p in params ]

    def is_base_type(self) -> bool:
        match self.type:
            case "float" | "int" | "void":
                return True
            case [*t]:
                return all((u.is_base_type() for u in t if isinstance(u, Type)))
            case _:
                return False

    def __repr__(self):
        match self.type:
            case str() as s:
                return s
            case [*types]:
                return f"({', '.join(map(str, types))})"

    def __hash__(self):
        return 1
        # match self.type:
        #     case str() as s:
        #         return s.__hash__()
        #     case (T1, T2):
        #         return T1.__hash__() + T2.__hash__()
        #     case _:
        #         raise RuntimeError("what")

    def clone(self) -> 'Type':
        return copy.deepcopy(self)

    def set(self, other: 'Type'):
        self.type = other.type


@dataclass
class TypeConstraint:
    lhs: Type
    rhs: Type

    def __hash__(self):
        l = self.lhs.__hash__()
        r = self.rhs.__hash__()
        return l + r


