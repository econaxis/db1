import copy
from typing import Callable

from astnode import AstNode
from instruction import Instruction
from parser import is_number
from type import TypeConstraint, Type

type_var_counter = 0


def get_unknown_type_var(var_name: str = None) -> str:
    global type_var_counter
    if not var_name:
        var_name = type_var_counter + 1
        type_var_counter += 1
    return f"Unknown-{var_name}"


def type_annotator(fn: AstNode, constraints: list[TypeConstraint]) -> Type:
    match fn:
        case AstNode(Instruction.NumberConstant, [x]) if x.isnumeric() or isinstance(x, int):
            return Type('int')
        case AstNode(Instruction.NumberConstant, [x]) if is_number(x):
            return Type("float")
        case AstNode(Instruction.String1, [str() as var_name]) | (str() as var_name):
            return Type(get_unknown_type_var(var_name))
        case AstNode(Instruction.Add | Instruction.Mult, [lhs, rhs]):
            lhs_type = type_annotator(lhs, constraints)
            rhs_type = type_annotator(rhs, constraints)
            result_type = Type(get_unknown_type_var())
            param_types = Type([lhs_type, rhs_type, result_type])
            constraints.append(TypeConstraint(Type("Operator+"), param_types))
            return result_type
        case AstNode(Instruction.Negate, [val]):
            val_type = type_annotator(val, constraints)
            constraints.append(TypeConstraint(val_type, Type('int')))
            return val_type

        case AstNode(Instruction.Unknown, [AstNode(Instruction.String1, [str() as fn_name]), *params_list]):
            return_type = Type(get_unknown_type_var())
            fn_name_type = Type(fn_name)
            params_type = list(map(lambda k: type_annotator(k, constraints), params_list))
            params_type = Type([*params_type, return_type])

            constraints.append(TypeConstraint(fn_name_type, params_type))
            return return_type
        case AstNode(Instruction.Unknown, [*operands]):
            operands_type = [type_annotator(p, constraints) for p in operands]
            return operands_type[-1]


def unify_vars(one: Type, two: Type, solutions: dict[Type, Type]) -> dict[Type, Type]:
    match one, two:
        case Type([*t1]), Type([*t2]):
            [unify_vars(x1, x2, solutions) for x1, x2 in zip(t1, t2)]
            return solutions
        case Type(x), Type(x1):
            if one == two:
                return solutions
            if one in solutions:
                return unify_vars(solutions[one], two, solutions)
            if two in solutions:
                return unify_vars(one, solutions[two], solutions)
            match one.is_base_type(), two.is_base_type():
                # Primitive types
                case True, True:
                    assert one == two, f"Primitive types don't match {one} != {two}"
                case ((True, False) as order) | ( (False, True) as order):
                    print(f"Inserting() {two}/{one}")

                    # If first is base type
                    if order[0]:
                        solutions[two.clone()] = one.clone()
                    # If second is base type
                    else:
                        solutions[one.clone()] = two.clone()

                case False, False:
                    print(f"Inserting {one}/{two}")
                    solutions[one.clone()] = two.clone()
                case _:
                    raise RuntimeError

            return solutions

        case _:
            raise RuntimeError("Invalid types")


def unify_type_dicts(one: dict[Type, Type], two: dict[Type, Type]) -> dict[Type, Type]:
    ret_dict = copy.copy(one)

    for o in one:
        ret_dict[o] = ret_dict[o]
        if o in two:
            unify_vars(ret_dict[o], two[o], ret_dict)
        else:
            ret_dict[o] = one[o]
    for o in two:
        if o not in one:
            ret_dict[o] = two[o]
    return ret_dict


def unify_constraints(constraints: list[TypeConstraint]) -> dict[Type, Type]:
    solutions = {}
    for c in constraints:
        unify_vars(c.lhs, c.rhs, solutions)

    simplify_unification(solutions)
    return solutions


def convert_type(base_t: Type, t: Type, converter: Callable[[Type], bool]) -> bool:
    if t == base_t:
        return False
    match t.type:
        case str():
            return converter(t)
        case [*types]:
            return any([convert_type(base_t, T1, converter) for T1 in types])


def simplify_unification(unified: dict[Type, Type]):
    def demote_constraint(t: Type) -> bool:
        if t in unified and t != unified[t]:
            t.set(unified[t])
            return True
        return False

    for _ in range(0, 10):
        for u in unified:
            if not unified[u].is_base_type():
                convert_type(u, unified[u], demote_constraint)