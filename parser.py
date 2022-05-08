from functools import reduce


from astnode import AstNode
from instruction import Instruction


def is_number(a: str) -> bool:
    return all((c.isdigit() or c == '.' for c in a))


def take_two(tokens: list[str]) -> tuple[AstNode, AstNode, list[str]]:
    LHS, next = parser(tokens)
    RHS, next = parser(next)
    return LHS, RHS, next


def take_until(next: list[str], until: str) -> tuple[list[AstNode], list[str]]:
    total_result = []
    while len(next) > 0 and next[0] != until:
        result, next = parser(next)
        total_result.append(result)
    return total_result, next


def parser(tokens: list[str]) -> tuple[AstNode, list[str]]:
    match tokens:
        case ['(', *next]:
            total_result, next = take_until(next, ')')
            assert next[0] == ')'
            return AstNode(Instruction.Unknown, total_result), next[1:]
        case ['+', *rest]:
            token_result, next = take_until(rest, ')')
            assert next[0] == ')'
            return reduce(lambda k1, k2: AstNode(Instruction.Add, [k1, k2]), token_result), next
        case ['-', *rest]:
            token_result, next = take_until(rest, ')')
            assert next[0] == ')'
            token_result[1:] = list(map(lambda k: AstNode(Instruction.Negate, k), token_result[1:]))

            return reduce(lambda k1, k2: AstNode(Instruction.Add, [k1, k2]), token_result), next
        case ['*', *rest]:
            token_result, next = take_until(rest, ')')
            assert next[0] == ')'
            return reduce(lambda k1, k2: AstNode(Instruction.Mult, [k1, k2]), token_result), next
        case [x, *rest] if is_number(x):
            return AstNode(Instruction.NumberConstant, [x]), rest
        case [x, *rest]:
            return AstNode(Instruction.String1, [x]), rest


def flatten_tree(node: AstNode) -> AstNode:
    match node:
        case AstNode(Instruction.NumberConstant) | AstNode(Instruction.String1):
            return node
        case AstNode(Instruction.Unknown, [AstNode() as curry_type_node]):
            node = flatten_tree(curry_type_node)
            return node
        case AstNode(Instruction.Unknown, _) as uk_node:
            match uk_node.operands:
                case [AstNode(Instruction.String1, ["lambda"]), AstNode(Instruction.Unknown, [*lambda_params]), fn_body]:
                    assert all((param.instr == Instruction.String1 for param in lambda_params))

                    node.instr = Instruction.Defun
                    node.operands = [gen_random_name(), AstNode(Instruction.LambdaParams, lambda_params), fn_body]
                case [AstNode(Instruction.String1, ["defun"]), AstNode(Instruction.String1, [_]) as fn_name,
                      AstNode(Instruction.Unknown, [*lambda_params]),
                      fn_body]:
                    node.instr = Instruction.Defun
                    node.operands = [fn_name, AstNode(Instruction.LambdaParams, lambda_params), fn_body]
    node.operands = list(map(lambda k: flatten_tree(k), node.operands))

    return node


def lexer(str: str) -> list[str]:
    words = []
    current_word = ""
    for c in str:
        match c:
            case ' ':
                words.append(current_word)
                current_word = ""
            case '(' | ')' as x:
                words.append(current_word)
                current_word = ""
                words.append(x)
            case c:
                current_word += c

    return list(filter(lambda k: k != "", words))


def parse_all(a: [str]) -> [AstNode]:
    if a == []:
        return []
    a, next = parser(a)
    return [a, *parse_all(next)]


name_counter = 1


def gen_random_name():
    global name_counter
    name_counter += 1
    return AstNode(Instruction.String1, [f"Lambda-{name_counter}"])
