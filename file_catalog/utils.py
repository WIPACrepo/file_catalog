"""Generic utilities. Things that should be library functions."""


# NOTE: no relative imports please, let's keep this generic
from typing import Any, Dict


def get_val_in_dict_dotted(field: str, dicto: Dict[str, Any]) -> bool:
    """Return True if the `field` is a key or sub-key (dot syntax) in `dicto`.

    Raises:
        KeyError - if field not found
        TypeError - if parent-field is not `dict`-like
    """
    if "." not in field:  # simple field; ex: "logical_name", "sha512"
        return field in dicto

    # compound field; ex: "checksum.sha512"
    parent, child = field.split(".", maxsplit=1)  # ex: "checksum" & "sha512"
    # ex: is "sha512" in "checksum"'s dict?
    return get_val_in_dict_dotted(child, dicto[parent])
