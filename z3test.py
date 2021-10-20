# from z3 import *
#
#
# Type = Datatype("Type")
# Str = DeclareSort("Str")
# A, B, C, D = Consts("A B C D", Str)
# Type.declare('cons', ('car', Str), ('cdr', Type))
# Type.declare('nil')
# Type = Type.create()
#
# DU = Const("DU", Type)
#
# cons = Type.cons
# l1 = cons(A, cons(B, Type.nil))
# l2 = cons(D, DU)
# print(solve(l1 == l2, A != B,))

import ast

sources = ["compiler.py", "astnode.py", "type.py", "function_stub.py"]
out_path = "bearer"

bear_import = ast.Import(names = [ast.alias(name = "beartype")])
tg_import = ast.Import(names = [ast.alias(name = "typeguard")])
bear_type = ast.Name("beartype", ast.Load())
attrib = ast.Attribute(bear_type, "beartype", ast.Load())

tg_type = ast.Name("typeguard", ast.Load())
tg_attrib = ast.Attribute(tg_type, "typechecked", ast.Load())

class Transformer(ast.NodeTransformer):
    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
        node.decorator_list.extend([attrib, tg_attrib])
        return node

def bearify(filename: str):
    with open(filename) as source:
        tree = ast.parse(source.read())


        tree.body = [bear_import, tg_import, *tree.body]
        Transformer().visit(tree)

        ast.fix_missing_locations(tree)

    with open(f"{out_path}/{filename}", "w") as source1:
        source1.write(ast.unparse(tree))

[bearify(s) for s in sources]