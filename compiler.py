# Compile (+ (arg1) (arg2))
import copy
from pprint import pformat
from typing import Optional, Union

from astnode import AstNode, BaseAst, UnresolvedFnApp, TypedFnDef, VariableDef, ProgN, UnresolvedVariableLoad, \
    ConstantLoad, FunctionApplication
from function_stub import FunctionStub
from instruction import Instruction
from parser import flatten_tree, lexer, parse_all
from type import Type
from type_inference import type_annotator


def pprint(x):
    string = pformat(x).replace('       ', ' ')
    print(string)


global_namespace = {}


def convert_string_to_str(*args: AstNode) -> list[str]:
    def mapper(a: AstNode) -> str:
        match a:
            case AstNode(Instruction.String1, [str() as s]):
                return s
            case _:
                raise RuntimeError

    return list(map(mapper, args))


def separate_defuns(node: AstNode) -> tuple[list[FunctionStub], Union[AstNode, None]]:
    match node:
        case AstNode(Instruction.Mult | Instruction.Add, [_, _]):
            pass
        case AstNode(Instruction.Negate | Instruction.String1 | Instruction.NumberConstant, [_]):
            pass
        case AstNode(Instruction.Defun,
                     [AstNode(Instruction.String1, [name]), AstNode(Instruction.LambdaParams, [*params]), fn_body]):
            params = convert_string_to_str(*params)

            stub = FunctionStub(node, name=name, params_list=params, body=fn_body)
            return [stub], None
        case AstNode(Instruction.LambdaParams | Instruction.Unknown | Instruction.Module, _):
            pass
        case _:
            raise RuntimeError("Compile error at node ", node)

    functions = []
    new_operands = []
    for j in node.operands:
        if isinstance(j, AstNode):
            functions_list, new_node = separate_defuns(j)
            functions.extend(functions_list)
            if new_node:
                new_operands.append(new_node)
        else:
            new_operands.append(j)
    node.operands = new_operands

    return functions, node


def insert_not_exists(dict, key, value):
    assert key not in dict
    dict[key] = value


def type_lookup(d: dict[Type, Type], k) -> Type:
    match k:
        case str() as s:
            type_string = type_annotator(s, [])
            return d[type_string]
        case Type() as t:
            return d[t]
        case _:
            raise RuntimeError


def resolve_all_function_calls(functions: list[TypedFnDef]) -> list[TypedFnDef]:
    function_name_map = {x.name: x for x in functions}

    def recursive_resolver(a: BaseAst) -> BaseAst:
        match a:
            case TypedFnDef():
                a.body = recursive_resolver(a.body)
                return a
            case UnresolvedFnApp(fn, params):
                resolved_params = [recursive_resolver(p) for p in params]
                return FunctionApplication(function_name_map[fn], resolved_params)
            case ProgN(programs):
                return ProgN([recursive_resolver(x) for x in programs])
            case _:
                return a

    return list(map(recursive_resolver, functions))

def convert_to_baseast(a: FunctionStub, type_solutions: dict[Type, Type]) -> TypedFnDef:
    def create_variable_defs(params: list[str]) -> list[VariableDef]:
        ret = []
        for p in params:
            p_type = type_lookup(type_solutions, p)
            ret.append(VariableDef(p, p_type))
        return ret

    def convert_function(n: FunctionStub) -> TypedFnDef:
        params = create_variable_defs(n.params_list)
        body = iter_node(n.body)
        return_type = type_lookup(type_solutions, n.my_type)
        return TypedFnDef(n.name, params, body, return_type)

    def iter_node(n: AstNode) -> BaseAst:
        match n.instr:
            case Instruction.String1:
                return UnresolvedVariableLoad(n.operands[0])
            case Instruction.NumberConstant:
                return ConstantLoad(Type("int"), n.operands[0])
            case Instruction.Add | Instruction.Mult as operation:
                lhs, rhs = n.operands
                name = "Operator+" if operation == Instruction.Add else "Operator*"
                lhs = iter_node(lhs)
                rhs = iter_node(rhs)
                return UnresolvedFnApp(name, [lhs, rhs])
            case Instruction.Unknown if n.operands[0].instr == Instruction.String1:
                name, *args = n.operands
                [name] = name.operands
                assert isinstance(name, str)
                args = [iter_node(x) for x in args]
                return UnresolvedFnApp(name, args)
            case Instruction.Unknown:
                if all(map(lambda k: k.instr == Instruction.Unknown, n.operands)):
                    programs = [iter_node(n1) for n1 in n.operands]
                    return ProgN(programs)
                else:
                    raise RuntimeError
            case _:
                raise RuntimeError

    return convert_function(a)


a = lexer("(defun norm(x y z) (sqrt (+ (*  x x) (*  y y) (*  z z))))"
          "(defun Operator*(x y) (compiler-intrinsic))"
          "(defun Operator+(x y) (compiler-intrinsic))"
          "(defun pow (x exp) (compiler-intrinsic))"
          "(defun print (a b c) (compiler-intrinsic))"
          "(defun sqrt(x) (pow x 0.5))"
          "(defun main() ((norm 30.0 20.0 50.0) (print 1 2 3)))"
          "")
a = AstNode(Instruction.Module, parse_all(a))
a = flatten_tree(a)
functions, script = separate_defuns(a)

main_fn = next(x for x in functions if x.name == "main")

std_types = {Type("sqrt"): Type("float", "float"),
             Type("pow"): Type("float", "float"),
             Type("sqrti"): Type("int", "float"),
             Type("normiEE"): Type("int", "int", "int", "float")}
[f.annotate_type() for f in functions]
[main_fn.unify_with_other(f.intermediate_types) for f in functions]

main_fn.unify_with_other(std_types)
application = Type(["void"])
resulting_type = main_fn.apply_with_type(application)

type_solutions = main_fn.intermediate_types
fn_defs = [convert_to_baseast(f, type_solutions) for f in functions]

fn_defs = resolve_all_function_calls(fn_defs)
pprint(fn_defs)

# application.type[2] = Type("float")
# f.apply_with_type(application)

# function = FunctionStub(node=f, intermediate_types=unified, )

u64 = ir.IntType(64)

module = ir.Module(name="test")


def compile1(builder: Optional[ir.IRBuilder], ast: AstNode):
    match ast.instr:
        case Instruction.Lambda:
            params_list = ast.operands[0].operands
            params_type_llvm = [u64] * len(params_list)
            fn_body = ast.operands[1]

            func = ir.Function(module, ir.FunctionType(u64, params_type_llvm), name="lambda")
            funcblock = func.append_basic_block()
            builder = ir.IRBuilder(funcblock)

            args = func.args

            _ = [ast.set_bound_vars(name.operands[0], value) for name, value in zip(params_list, args)]
            result = compile1(builder, fn_body)
            builder.ret(result)
        case Instruction.Add | Instruction.Mult as instr:
            assert len(ast.operands) == 2
            lhs = compile1(builder, ast.operands[0])
            rhs = compile1(builder, ast.operands[1])

            if instr == Instruction.Add:
                result = builder.add(lhs, rhs)
            elif instr == Instruction.Mult:
                result = builder.mul(lhs, rhs)
            else:
                raise RuntimeError
            return result
        case Instruction.Negate:
            assert len(ast.operands) == 1
            normal = compile1(builder, ast.operands[0])
            result = builder.neg(normal)
            return result

        case Instruction.NumberConstant:
            return ir.NumberConstant(u64, ast.operands[0])
        case Instruction.String1:
            # Do variable lookup
            name = ast.operands[0]
            ir_value = ast.get_bound_vars(name)
            return ir_value


compile1(None, a)

# compile(builder, ast)

# print(module)
