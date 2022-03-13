import json
from typing import Any, Dict, List, Set, Tuple

import pytest

import pymq
from pymq.json import DeepDictDecoder, DeepDictEncoder
from pymq.typing import deep_from_dict, deep_to_dict, fullname


class SimpleNested:
    name: str
    value: int

    def __init__(self, name=None, value=None) -> None:
        super().__init__()
        self.name = name
        self.value = value


class ComplexNested:
    nested_list: List[SimpleNested]
    nested_tuple: Tuple[int, SimpleNested]


class RootClass:
    some_int: int
    some_str: str

    simple: SimpleNested

    simple_dict: Dict[str, int]
    complex_dict: Dict[int, SimpleNested]

    complex_list: List[ComplexNested]


class DataClass:
    req_a: str
    req_b: int
    opt_c: bool = False
    opt_d: SimpleNested = None

    def __init__(self, req_a: str, req_b: int) -> None:
        super().__init__()
        self.req_a = req_a
        self.req_b = req_b


class ClassWithAny:
    arg: Any

    def __init__(self, arg) -> None:
        super().__init__()
        self.arg = arg


class ClassWithSet:
    simple_set: Set[str]

    def __init__(self, simple_set) -> None:
        super().__init__()
        self.simple_set = simple_set


class ModifiedDeepDictDecoder(DeepDictDecoder):
    def _load_class(self, class_name):
        if class_name.endswith("RootClass"):
            return RootClass
        if class_name.endswith("ClassWithSet"):
            return ClassWithSet

        return super()._load_class(class_name)


def sample_object() -> RootClass:
    s1 = SimpleNested("a", 1)
    s2 = SimpleNested("b", 2)
    s3 = SimpleNested("c", 3)

    cn1 = ComplexNested()
    cn1.nested_list = [s1, s2]
    cn1.nested_tuple = (1, s1)

    cn2 = ComplexNested()
    cn2.nested_list = [s2, s3]
    cn2.nested_tuple = (2, s2)

    root = RootClass()
    root.some_int = 42
    root.some_str = "jaffa kree"

    root.simple_dict = {"a": 1, "b": 2}
    root.complex_dict = {1: s1, 2: s2}
    root.complex_list = [cn1, cn2]

    return root


class TestMarhsalling:

    # TODO: better/finer-grained tests

    def test_to_dict(self):
        doc = deep_to_dict(sample_object())

        expected = {
            "some_int": 42,
            "some_str": "jaffa kree",
            "simple_dict": {"a": 1, "b": 2},
            "complex_dict": {1: {"name": "a", "value": 1}, 2: {"name": "b", "value": 2}},
            "complex_list": [
                {
                    "nested_list": [{"name": "a", "value": 1}, {"name": "b", "value": 2}],
                    "nested_tuple": (1, {"name": "a", "value": 1}),
                },
                {
                    "nested_list": [{"name": "b", "value": 2}, {"name": "c", "value": 3}],
                    "nested_tuple": (2, {"name": "b", "value": 2}),
                },
            ],
        }

        assert expected == doc

    def test_to_dict_any(self):
        t_int = ClassWithAny(1)
        assert {"arg": 1} == deep_to_dict(t_int)

        t_dict = ClassWithAny({"a": 1, "b": 2})
        assert {"arg": {"a": 1, "b": 2}} == deep_to_dict(t_dict)
        assert {"arg": {"a": 1, "b": 2}} == deep_to_dict(t_dict)

        t_nested = ClassWithAny(SimpleNested("foo", 42))
        assert {"arg": {"name": "foo", "value": 42}} == deep_to_dict(t_nested)

    def test_to_dict_set(self):
        obj = ClassWithSet({"foo", "bar"})
        doc = deep_to_dict(obj)

        assert "foo" in doc["simple_set"]
        assert "bar" in doc["simple_set"]
        assert len(doc["simple_set"]) == 2

    def test_from_dict_any(self):
        t_int = deep_from_dict({"arg": 1}, ClassWithAny)
        assert 1 == t_int.arg

        t_dict = deep_from_dict({"arg": {"a": 1, "b": 2}}, ClassWithAny)
        assert {"a": 1, "b": 2} == t_dict.arg

    def test_from_dict_set(self):
        def do_assert(_obj: ClassWithSet):
            assert "foo" in _obj.simple_set
            assert "bar" in _obj.simple_set
            assert type(_obj.simple_set) == set
            assert len(_obj.simple_set) == 2

        do_assert(deep_from_dict({"simple_set": {"foo", "bar"}}, ClassWithSet))
        do_assert(deep_from_dict({"simple_set": ["foo", "bar"]}, ClassWithSet))
        do_assert(deep_from_dict({"simple_set": ("foo", "bar")}, ClassWithSet))

    def test_encode_set(self):
        json_string = json.dumps(ClassWithSet({"foo", "bar"}), cls=DeepDictEncoder)
        doc = json.loads(json_string)

        assert doc["__type"] == fullname(ClassWithSet)
        assert "foo" in doc["simple_set"]
        assert "bar" in doc["simple_set"]
        assert len(doc["simple_set"]) == 2

    def test_decode_set(self):
        json_string = '{"simple_set": ["foo", "bar"], "__type": "%s"}' % fullname(ClassWithSet)

        obj = json.loads(json_string, cls=DeepDictDecoder)

        assert type(obj) == ClassWithSet
        assert "foo" in obj.simple_set
        assert "bar" in obj.simple_set
        assert type(obj.simple_set) == set
        assert len(obj.simple_set) == 2

    def test_from_dict_base_cases(self):
        assert 1 == deep_from_dict(1, int)
        assert 1 == deep_from_dict("1", int)
        assert "a" == deep_from_dict("a", str)
        assert (1, 2) == deep_from_dict((1, 2), tuple)
        assert [1, 2] == deep_from_dict([1, 2], list)
        assert (1, 2) == deep_from_dict([1, 2], tuple)
        assert {1, 2} == deep_from_dict({1, 2}, set)
        assert {1, 2} == deep_from_dict([1, 2], set)
        assert {"1", "2"} == deep_from_dict(["1", "2"], set)

    def test_from_dict_generics(self):
        assert {1, 2} == deep_from_dict(["1", "2"], Set[int])
        assert {1, 2} == deep_from_dict(["1", "2", "1"], Set[int])
        assert [(1, "a"), (2, "b")] == deep_from_dict(
            [["1", "a"], ["2", "b"]], List[Tuple[int, str]]
        )

    def test_from_dict_simple_type(self):
        doc = {"name": "a", "value": "1"}
        obj = deep_from_dict(doc, SimpleNested)
        assert isinstance(obj, SimpleNested)
        assert "a" == obj.name
        assert 1 == obj.value

    def test_from_dict_constructor(self):
        doc = {
            "req_a": "a",
            "req_b": 1,
            "opt_c": True,
        }
        obj = deep_from_dict(doc, DataClass)
        assert isinstance(obj, DataClass)
        assert "a" == obj.req_a
        assert 1 == obj.req_b
        assert obj.opt_c is True
        assert obj.opt_d is None

    def test_from_dict(self):
        doc = {
            "some_int": 42,
            "some_str": "jaffa kree",
            "simple_dict": {"a": 1, "b": 2},
            "complex_dict": {1: {"name": "a", "value": 1}, 2: {"name": "b", "value": 2}},
            "complex_list": [
                {
                    "nested_list": [{"name": "a", "value": 1}, {"name": "b", "value": 2}],
                    "nested_tuple": (1, {"name": "a", "value": 1}),
                },
                {
                    "nested_list": [{"name": "b", "value": 2}, {"name": "c", "value": 3}],
                    "nested_tuple": (2, {"name": "b", "value": 2}),
                },
            ],
        }

        root: RootClass = deep_from_dict(doc, RootClass)

        self.assertEqualsRoot(root)

    def test_encoding(self):
        doc = json.dumps(sample_object(), cls=DeepDictEncoder)
        expected = (
            '{"some_int": 42, "some_str": "jaffa kree", "simple_dict": {"a": 1, "b": 2}, "complex_dict": {"1": {"name": "a", "value": 1}, "2": {"name": "b", "value": 2}}, "complex_list": [{"nested_list": [{"name": "a", "value": 1}, {"name": "b", "value": 2}], "nested_tuple": [1, {"name": "a", "value": 1}]}, {"nested_list": [{"name": "b", "value": 2}, {"name": "c", "value": 3}], "nested_tuple": [2, {"name": "b", "value": 2}]}], "__type": "%s"}'
            % fullname(RootClass)
        )
        assert expected == doc

    def test_decoding(self):
        doc = (
            '{"some_int": 42, "some_str": "jaffa kree", "simple_dict": {"a": 1, "b": 2}, "complex_dict": {"1": {"name": "a", "value": 1}, "2": {"name": "b", "value": 2}}, "complex_list": [{"nested_list": [{"name": "a", "value": 1}, {"name": "b", "value": 2}], "nested_tuple": [1, {"name": "a", "value": 1}]}, {"nested_list": [{"name": "b", "value": 2}, {"name": "c", "value": 3}], "nested_tuple": [2, {"name": "b", "value": 2}]}], "__type": "%s"}'
            % fullname(RootClass)
        )
        root = json.loads(doc, cls=ModifiedDeepDictDecoder)

        self.assertEqualsRoot(root)

    def assertEqualsRoot(self, root):
        assert 42 == root.some_int
        assert "jaffa kree" == root.some_str
        assert 1 == root.complex_dict[1].value  # dict has int as keys
        assert 2 == root.complex_dict[2].value
        assert "a" == root.complex_dict[1].name
        assert "b" == root.complex_dict[2].name
        assert "a" == root.complex_list[0].nested_list[0].name
        assert "b" == root.complex_list[0].nested_list[1].name
        assert "b" == root.complex_list[1].nested_list[0].name
        assert "c" == root.complex_list[1].nested_list[1].name

    def test_normalize_type(self):
        assert "pymq.core.EventBus" == deep_to_dict(pymq.EventBus)
        assert "pymq.core.EventBus.run" == deep_to_dict(pymq.EventBus.run)
        assert "TimeoutError" == deep_to_dict(TimeoutError)

    def test_cast_type(self):
        with pytest.raises(TypeError):
            deep_from_dict("pymq.core.EventBus", type)

    def test_normalize_exception(self):
        assert ("failed",) == deep_to_dict(TimeoutError("failed"))

    def test_cast_exception(self):
        err = deep_from_dict(("failed",), TimeoutError)
        assert isinstance(err, TimeoutError)
        assert ("failed",) == err.args

        err = deep_from_dict("failed", TimeoutError)
        assert isinstance(err, TimeoutError)
        assert ("failed",) == err.args
