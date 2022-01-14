# Generates random tuples and tests that the get_range method is valid
# For example, get_range(a, b) returns all tuples such that a <= tuple <= b


import sys

sys.path.append("../target/debug/")

import random
import time

seed = time.time()
random.seed(seed)
print("seed ", seed)


from libpythonlib import *

prev_pkeys = set()
data = []
for i in range(0, 1000):
    value = (
        i * 10,
        random.randint(1, 100000),
        "12345678",
        "12345",
        1,
        1,
        1,
        1,
        1,
        True,
    )
    if value[0] in prev_pkeys:
        continue
    else:
        prev_pkeys.add(value[0])
        data.append(value)

data = sorted(data)

for value in data:
    store(*value)


def expand_range(list, idx, direction):
    match_value = list[idx][0]

    while 0 <= idx + direction < len(list) and list[idx + direction][0] == match_value:
        idx += direction
    return idx


for index, elem in enumerate(data):
    for _ in range(0, 100):
        index1 = random.randint(index, len(data) - 1)

        index_ = expand_range(data, index, -1)
        index1_ = expand_range(data, index1, 1)

        assert index_ <= index1_

        li = get_range(data[index_][0], data[index1_][0])
        try:
            one = list(map(lambda k: k["timestamp"], li))
            two = list(map(lambda k: k[0], data[index_ : index1_ + 1]))
            assert one == two
            assert len(li) == index1_ - index_ + 1
        except Exception as e:
            print(one)
            print(two)
            raise e

    if index % 5 == 0:
        print(f"Passed {index}/{len(data)}")
