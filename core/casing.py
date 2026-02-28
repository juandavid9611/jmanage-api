import re

def to_camel(s: str) -> str:
    parts = s.split('_')
    return parts[0] + ''.join(p.capitalize() for p in parts[1:])

# Para Pydantic v2
def camel_alias(field_name: str) -> str:
    return to_camel(field_name)
