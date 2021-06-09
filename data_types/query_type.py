from enum import Enum


class QueryType(Enum):
    SELECT = 'select'
    INSERT = 'insert'
    UPDATE = 'update'
    DELETE = 'delete'
    CREATE = 'create'
    COUNT = 'count'
