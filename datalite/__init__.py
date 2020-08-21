__all__ = ['commons', 'datalite_decorator', 'fetch', 'migrations', 'datalite', 'constraints']

from dataclasses import dataclass


from .datalite_decorator import datalite


@dataclass
class Student:
    id_: int
    name: str
