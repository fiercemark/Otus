from functools import wraps

def cases(cases):
    def decorator(f):
        @wraps(f)
        def wrapper(*args):
            for case in cases:
                new_args = args + (case if isinstance(case, tuple) else (case,))
                try:
                    f(*new_args)
                except Exception as e:
                    print('\n')
                    print('FAIL case:', case)
                    raise e
        return wrapper
    return decorator