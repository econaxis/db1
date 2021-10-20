from enum import Enum, auto


class Instruction(Enum):
    Add = auto()
    Negate = auto()
    LambdaParams = auto()
    Defun = auto()
    Unknown = auto()
    Module = auto()
    Mult = auto()
    String1 = auto()
    Setf = auto()
    Lambda = auto()
    NumberConstant = auto()