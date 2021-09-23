"""Generic utilities. Things that should be library functions."""


# NOTE: no relative imports please, let's keep this generic
from typing import Any, Dict


class DottedKeyError(KeyError):
    """Raised when a value cannot be retrieved using a dot-syntax key."""


def get_val_in_dict_dotted(field: str, dicto: Dict[str, Any]) -> Any:
    """Return value of the `field` at the key/sub-key (dot syntax) from `dicto`.

    Raises:
        DottedKeyError - if field not found / parent-field is not `dict`-like
    """
    try:
        if "." not in field:  # simple field; ex: "logical_name", "sha512"
            return dicto[field]  # possible KeyError/TypeError

        # compound field; ex: "checksum.sha512"
        parent, child = field.split(".", maxsplit=1)  # ex: "checksum" & "sha512"

        # ex: is "sha512" in "checksum"'s dict?
        # possible KeyError/TypeError
        return get_val_in_dict_dotted(child, dicto[parent])

    except (KeyError, TypeError) as e:
        raise DottedKeyError() from e
