from typing import Callable

# 함수에 적용하여 호출 전, 후에 로그 메시지 출력
# 그냥 테스트 용이어서 구현은 크게 신경 안 쓰셔도 될 것 같아요
def log(name: str) -> Callable:
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kw):
            print(f'call {name} {func.__name__}():')
            res = func(*args, **kw)
            print(f'end {name} {func.__name__}():')
            return res
        return wrapper
    return decorator

class A:
    @log('A')
    def __new__(cls, *args, other_cls=None, **kwargs):
        if other_cls:
            inst = other_cls(*args, **kwargs)
        else:
            inst = super().__new__(cls)
        return inst
    
    @log('A')
    def __init__(self, *args, **kwargs):
        pass


class B(A):
    @log('B')
    def __new__(cls, *args, other_cls=None, **kwargs):
        if other_cls:
            inst = other_cls(*args, **kwargs)
        else:
            inst = super().__new__(cls)
        return inst
    
    @log('B')
    def __init__(self, *args, **kwargs):
        pass

class C:
    @log('C')
    def __new__(cls, *args, other_cls=None, **kwargs):
        if other_cls:
            inst = other_cls(*args, **kwargs)
        else:
            inst = super().__new__(cls)
        return inst
    
    @log('C')
    def __init__(self, *args, **kwargs):
        pass


A()

print("=====================================")

B()

print("=====================================")

C()

print("=====================================")

# 자신의 자식 타입 클래스를 new 메서드에서 생성할 때 B의 init이 2번 호출됨...
A(other_cls=B)

print("=====================================")

# C를 인자로 받으면, A의 new 메서드가 끝나도 C의 init이 다시 호출되지 않음.
A(other_cls=C)