"""
datalite.constraints module introduces constraint
    types that can be used to hint field variables,
    that can be used to signal datalite decorator
    constraints in the database.
"""
from typing import TypeVar, Union, Tuple

T = TypeVar('T')


class ConstraintFailedError(Exception):
    """
    This exception is raised when a Constraint fails.
    """
    pass


"""
Dataclass fields hinted with this type signals
    datalite that the bound column of this
    field in the table is NOT NULL and UNIQUE.
"""
Unique = Union[Tuple[T], T]


