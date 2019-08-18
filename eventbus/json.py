import json

from eventbus.typing import deep_to_dict, deep_from_dict, load_class


class DeepDictEncoder(json.JSONEncoder):

    def encode(self, o):
        d = deep_to_dict(o)

        if isinstance(o, (bool, int, float, str, list, dict)):
            return super().encode(o)

        if isinstance(d, dict):
            d['__type'] = str(o.__class__)[8:-2]
            return super().encode(d)
        else:
            return super().encode({
                '__obj': d,
                '__type': str(o.__class__)[8:-2]
            })


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
            if '__type' in doc:
                cls = doc['__type']
                cls = self._load_class(cls)

        if '__type' in doc:
            del doc['__type']

        if '__obj' in doc:
            doc = doc['__obj']

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
