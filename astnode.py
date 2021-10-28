from dataclasses import dataclass, field
from typing import Union, Tuple, Any

from llvmlite import ir

from instruction import Instruction
from type import Type


class BaseAst:
    # text_indices: [int, int]

    def __init__(self):
        raise RuntimeError("Cannot instantiate BaseAst")


@dataclass()
class ProgN(BaseAst):
    programs: list[BaseAst]


@dataclass(init=False)
class VariableDef(BaseAst):
    name: str
    type: Type

    def __init__(self, name: str, type: Type):
        self.name = name
        self.type = type


@dataclass
class VariableLoad(BaseAst):
    variable: VariableDef


@dataclass
class UnresolvedVariableLoad(BaseAst):
    variable: str


@dataclass()
class ConstantLoad(BaseAst):
    type: Type
    constant: Any


@dataclass(slots=True)
class UntypedFnDef(BaseAst):
    name: str
    params: list[VariableDef]
    body: list[BaseAst]


@dataclass
class TypedFnDef(BaseAst):
    name: str
    params: list[VariableDef]
    body: BaseAst
    return_type: Type


@dataclass(slots=True)
class FunctionApplication(BaseAst):
    fn: TypedFnDef
    params: list[BaseAst]


@dataclass(slots=True)
class UnresolvedFnApp(BaseAst):
    fn: str
    params: list[BaseAst]


@dataclass
class AstNode:
    instr: Instruction
    operands: list[Union['AstNode', int, str]] = field(default_factory=list)
    bound_variables: dict = field(default_factory=dict)
    text_indices: Tuple[int, int] = field(default=(0, 0))

    def set(self, node: 'AstNode'):
        self.instr = node.instr
        self.operands = node.operands
        self.bound_variables = node.bound_variables
        self.text_indices = node.text_indices

    def set_bound_vars(self, new_var: str, value: ir.Value):
        self.bound_variables[new_var] = value
        for k in self.operands:
            match k:
                case AstNode():
                    k.set_bound_vars(new_var, value)

    def get_bound_vars(self, name: str) -> ir.Value:
        try:
            return self.bound_variables[name]
        except KeyError:
            raise KeyError(f"{name} doesn't exist in the variable namespace")

    def __repr__(self):
        return self.curry_type_repr(0)

    def repr_operands(self, depth: int) -> str:
        tail = ""
        for o in self.operands:
            match o:
                case AstNode() as o:
                    tail += f"{' ' * depth}{o.curry_type_repr(depth + 1)}\n"
                case _ as o:
                    tail += f"{' ' * depth}{o}"
        return tail

    def curry_type_repr(self, depth: int) -> str:
        match self.instr:
            case Instruction.NumberConstant:
                return f"{' ' * depth}C-{self.operands[0]}"
            case Instruction.String1:
                return f"{' ' * depth}String({self.operands[0]})"
            case Instruction.Unknown:
                match self.operands:
                    case [x] if not isinstance(x, AstNode):
                        return f"{self.repr_operands(depth)}"
                    case _:
                        return f"{' ' * depth}UK-(\n{self.repr_operands(depth + 1)}"
            case _:
                head = f"{self.instr}"
                return f"{head}\n{self.repr_operands(depth + 1)}"
