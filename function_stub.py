import copy
from dataclasses import dataclass, field

from astnode import AstNode
from type import Type, TypeConstraint
from type_inference import type_annotator, unify_vars, unify_type_dicts, unify_constraints, simplify_unification


@dataclass
class FunctionStub:
    node: AstNode
    name: str
    params_list: list[str]
    body: AstNode
    intermediate_types: dict[Type, Type] = field(default_factory=dict)
    my_type: Type | None = field(default=None)

    def apply_with_type(self, args: Type) -> Type:
        # assert args.is_base_type()
        match args.type:
            case [*types]:
                assert len(types) == len(self.intermediate_types[self.my_type].type)
            case _:
                raise RuntimeError("Compile failed")
        solution = unify_vars(self.my_type, args, copy.deepcopy(self.intermediate_types))
        simplify_unification(solution)

        print(solution)
        assert all((solution[t].is_base_type() for t in solution)), "Type must be fully resolved at this point"
        return solution[self.my_type]

    def annotate_type(self):
        constraints = []
        self.my_type = fn_type_annotator(self, constraints)
        self.intermediate_types = unify_constraints(constraints)

    def unify_with_other(self, other: dict[Type, Type]):
        if other == self.intermediate_types:
            return

        self.intermediate_types = unify_type_dicts(self.intermediate_types, other)


def fn_type_annotator(fn: FunctionStub, constraints: list[TypeConstraint]) -> Type:
    my_type = Type(fn.name)
    params_type = list(map(lambda k: type_annotator(k, constraints), fn.params_list))
    function_body_type = type_annotator(fn.body, constraints)
    params_type = Type([*params_type, function_body_type])
    constraints.append(TypeConstraint(my_type, params_type))
    return my_type
