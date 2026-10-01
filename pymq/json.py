"""
JSON encoding and decoding helpers that transparently (de-)serialize arbitrary Python objects.

The encoder walks object graphs using :func:`pymq.typing.deep_to_dict` and annotates the
result with type information (``__type``, ``__obj``, ``__list``) so that the decoder can
reconstruct the original types using :func:`pymq.typing.deep_from_dict`.
"""

import json
from typing import Any, Callable, List

from pymq.typing import deep_from_dict, deep_to_dict, fullname, load_class

dumps = json.dumps
loads = json.loads


class DeepDictEncoder(json.JSONEncoder):
    """
    JSON encoder that serializes arbitrary Python objects into dictionaries annotated with type information.

    Primitive types and containers of primitives are encoded as-is. Everything else is converted using
    :func:`pymq.typing.deep_to_dict` and tagged so that :class:`DeepDictDecoder` can restore it.
    """

    def encode(self, obj: Any) -> str:
        """
        Encode the given object as a JSON string.

        :param obj: the object to encode
        :return: the JSON representation of the object
        """
        if isinstance(obj, (bool, int, float, str, bytes, dict)):
            return super().encode(obj)

        if isinstance(obj, list):
            if not obj:
                return super().encode(obj)
            elem = obj[0]
            # check if list is primitive
            if isinstance(elem, (bool, int, float, str, bytes)):
                return super().encode(obj)
            doc = deep_to_dict(obj)
            return super().encode({"__list": doc, "__type": fullname(obj[0])})

        doc = deep_to_dict(obj)

        if isinstance(doc, dict):
            doc["__type"] = fullname(obj)
            return super().encode(doc)
        else:
            return super().encode({"__obj": doc, "__type": fullname(obj)})


class DeepDictDecoder(json.JSONDecoder):
    """
    JSON decoder that restores objects encoded by :class:`DeepDictEncoder`.

    It reads the type annotations added by the encoder and uses :func:`pymq.typing.deep_from_dict`
    to reconstruct the appropriate Python types. A target class can be fixed via :meth:`for_type`.
    """

    target_class: type | None = None

    def decode(self, s: str, _w: Callable[..., Any] = json.decoder.WHITESPACE.match) -> Any:
        """
        Decode a JSON string into a Python object.

        :param s: the JSON string to decode
        :param _w: internal whitespace matcher used by the underlying JSON decoder
        :return: the decoded object
        """
        doc = super().decode(s, _w)

        cls = None
        if self.target_class:
            cls = self.target_class

        if not isinstance(doc, (dict, list)):
            if not cls:
                return doc
            else:
                return deep_from_dict(doc, cls)

        if cls is None:
            if "__type" in doc:
                cls = doc["__type"]
                cls = self._load_class(cls)
            if "__list" in doc:
                cls = List[cls]

        if "__type" in doc:
            del doc["__type"]

        if "__obj" in doc:
            doc = doc["__obj"]
        elif "__list" in doc:
            doc = doc["__list"]

        if cls:
            return deep_from_dict(doc, cls)
        else:
            return doc

    @classmethod
    def for_type(cls, target_class: type) -> Callable[..., "DeepDictDecoder"]:
        """
        Create a decoder factory that always decodes into the given target class.

        :param target_class: the class to decode into
        :return: a callable that creates a :class:`DeepDictDecoder` bound to ``target_class``
        """

        def init(*args: Any, **kwargs: Any) -> "DeepDictDecoder":
            """
            Create a decoder bound to the enclosing ``target_class``.
            """
            decoder = cls(*args, **kwargs)
            decoder.target_class = target_class
            return decoder

        return init

    def _load_class(self, class_name: str) -> type:
        """
        Load the class for the given fully qualified name. Override to customize class resolution.

        :param class_name: the fully qualified class name
        :return: the resolved class
        """
        return load_class(class_name)
