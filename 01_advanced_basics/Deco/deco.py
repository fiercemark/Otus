#!/usr/bin/env python
# -*- coding: utf-8 -*-

from functools import wraps

def disable(func):
    '''
    Disable a decorator by re-assigning the decorator's name
    to this function. For example, to turn off memoization:

    >>> memo = disable

    '''

    return func


def decorator(func):
    '''
    Decorate a decorator so that it inherits the docstrings
    and stuff from the function it's decorating.
    '''
    @wraps(func)
    def inner(*args, **kwargs):
        return func(*args, **kwargs)
    return inner


def countcalls(func):
    '''Decorator that counts calls made to the function decorated.'''
    @wraps(func)
    def inner(*args, **kwargs):
        inner.calls += 1
        return func(*args, **kwargs)
    inner.calls = 0
    return inner


def memo(func):
    '''
    Memoize a function so that it caches all return values for
    faster future lookups.
    '''
    cache = {}
    @wraps(func)
    def inner(*args, **kwargs):
        key = (args, frozenset(kwargs.items()))
        if key not in cache:
            ret = func(*args, **kwargs)
            cache[key] = ret
        return cache[key]
    return inner


def n_ary(func):
    '''
    Given binary function f(x, y), return an n_ary function such
    that f(x, y, z) = f(x, f(y,z)), etc. Also allow f(x) = x.
    '''
    @wraps(func)
    def inner(*args, **kwargs):
        call = ', '.join(
            [str(a) for a in args] + [f"{k}={v}" for k, v in kwargs]
        )
        if len(args) > 2:
            ret = func(args[0], inner(*args[1:]), **kwargs)
            return ret
        else:
            ret = func(*args, **kwargs)
            return ret
    return inner


def trace(separator):
    '''Trace calls made to function decorated.

    @trace("____")
    def fib(n):
        ....

    >>> fib(3)
     --> fib(3)
    ____ --> fib(2)
    ________ --> fib(1)
    ________ <-- fib(1) == 1
    ________ --> fib(0)
    ________ <-- fib(0) == 1
    ____ <-- fib(2) == 2
    ____ --> fib(1)
    ____ <-- fib(1) == 1
     <-- fib(3) == 3

    '''
    def decorator(func):
        @wraps(func)
        def inner(*args, **kwargs):
            print(separator * inner.state, '--> ' + func.__name__ + '({})'.format(args[0]))
            inner.state += 1
            result = func(*args, **kwargs)
            if result:
                inner.state -= 1
                print(separator * inner.state, '<-- ' + func.__name__ + '({})'.format(args[0]) + '==', result)
            return result
        inner.state = 0
        return inner
    return decorator


@countcalls
@memo
@n_ary
def bar(a, b):
    return a * b

@memo
@countcalls
@n_ary
def foo(a, b):
    return a + b

@countcalls
@trace("####")
@memo
def fib(n):
    """Some doc"""
    return 1 if n <= 1 else fib(n-1) + fib(n-2)


def main():
    print(foo(4, 3))
    print(foo(4, 3, 2))
    print(foo(4, 3))
    print("foo was called", foo.calls, "times")

    print(bar(4, 3))
    print(bar(4, 3, 2))
    print(bar(4, 3, 2, 1))
    print("bar was called", bar.calls, "times")

    print(fib.__doc__)
    fib(3)
    print(fib.calls, 'calls made')


if __name__ == '__main__':
    main()