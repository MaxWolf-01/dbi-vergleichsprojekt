import typing
from statistics import mean, stdev
from timeit import Timer
from typing import Callable

import psycopg2.extensions
import pymongo.database
from tqdm import tqdm


def measure_performance(
        db: pymongo.database.Database | psycopg2.extensions.connection,
        test_func: Callable,
        n_tests: int,
        init_func: typing.Optional[Callable] = None,
        *args,
        **kwargs
) -> tuple[float, float]:
    if init_func:
        init_func_n = kwargs.pop('init_func_n', kwargs.pop('n', 1000))
        print(f"Applying init function: {init_func.__name__}(n={init_func_n})")
        init_func(db, n=init_func_n)

    print(f"Running test function: {test_func.__name__}(*{args}, **{kwargs})")

    timings = []
    for _ in tqdm(range(n_tests)):
        timer = Timer(lambda: test_func(db, *args, **kwargs))
        timings.append(timer.timeit(number=1))

    avg_time = mean(timings)
    std_dev = stdev(timings) if len(timings) > 1 else 0

    print(f"'{test_func.__name__}' - Average time: {avg_time:.4f} s, Std Dev: {std_dev:.4f} s")
    return avg_time, std_dev
