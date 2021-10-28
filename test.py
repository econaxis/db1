import sympy
from sympy import *
from sympy.calculus.util import continuous_domain
from sympy.solvers import solve

def eql(x, y):
    return x - y

def f(x):
    return 1/(x-1)
def g(x):
    return abs(x)
x = Symbol('x')

ff = f(f(x))
gg = g(g(x))
fg = f(g(x))
gf = g(f(x))
v = [print(continuous_domain(a0,  x, Reals)) for a0 in [ff, gg, fg, gf]]