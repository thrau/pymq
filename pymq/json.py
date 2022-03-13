import json

from pymq.typing import deep_from_dict, deep_to_dict, fullname, load_class

dumps = json.dumps
loads = json.loads


class DeepDictEncoder(json.JSONEncoder):
    def encode(self, obj):
        doc = deep_to_dict(obj)

        if isinstance(obj, (bool, int, float, str, list, dict)):
            return super().encode(obj)

        if isinstance(doc, dict):
            doc["__type"] = fullname(obj)
            return super().encode(doc)
        else:
            return super().encode({"__obj": doc, "__type": fullname(obj)})


class DeepDictDecoder(json.JSONDecoder):
    target_class: type = None

    def decode(self, s, _w=json.decoder.WHITESPACE.match):
        doc = super().decode(s, _w)

        cls = None
        if self.target_class:
            cls = self.target_class

        if not isinstance(doc, dict):
            if not cls:
                return doc
            else:
                return deep_from_dict(doc, cls)

        if cls is None:
            if "__type" in doc:
                cls = doc["__type"]
                cls = self._load_class(cls)

        if "__type" in doc:
            del doc["__type"]

        if "__obj" in doc:
            doc = doc["__obj"]

        if cls:
            return deep_from_dict(doc, cls)
        else:
            return doc

    @classmethod
    def for_type(cls, target_class: type):
        def init(*args, **kwargs):
            decoder = cls(*args, **kwargs)
            decoder.target_class = target_class
            return decoder

        return init

    def _load_class(self, class_name):
        return load_class(class_name)
