import json
import unittest
from typing import Any, Dict, List, Set, Tuple

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


class ModifiedDeepDictDecoder(DeepDictDecoder):
    def _load_class(self, class_name):
        if class_name.endswith("RootClass"):
            return RootClass

        return super()._load_class(class_name)


class TestMarhsalling(unittest.TestCase):
    root: RootClass

    def setUp(self) -> None:
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

        self.root = root

    # TODO: better/finer-grained tests

    def test_to_dict(self):
        doc = deep_to_dict(self.root)

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

        self.assertEqual(expected, doc)

    def test_to_dict_any(self):
        t_int = ClassWithAny(1)
        self.assertEqual({"arg": 1}, deep_to_dict(t_int))

        t_dict = ClassWithAny({"a": 1, "b": 2})
        self.assertEqual({"arg": {"a": 1, "b": 2}}, deep_to_dict(t_dict))

        self.assertEqual({"arg": {"a": 1, "b": 2}}, deep_to_dict(t_dict))

        t_nested = ClassWithAny(SimpleNested("foo", 42))
        self.assertEqual({"arg": {"name": "foo", "value": 42}}, deep_to_dict(t_nested))

    def test_from_dict_any(self):
        t_int = deep_from_dict({"arg": 1}, ClassWithAny)
        self.assertEqual(1, t_int.arg)

        t_dict = deep_from_dict({"arg": {"a": 1, "b": 2}}, ClassWithAny)
        self.assertEqual({"a": 1, "b": 2}, t_dict.arg)

    def test_from_dict_base_cases(self):
        self.assertEqual(1, deep_from_dict(1, int))
        self.assertEqual(1, deep_from_dict("1", int))
        self.assertEqual("a", deep_from_dict("a", str))
        self.assertEqual((1, 2), deep_from_dict((1, 2), tuple))
        self.assertEqual([1, 2], deep_from_dict([1, 2], list))
        self.assertEqual((1, 2), deep_from_dict([1, 2], tuple))
        self.assertEqual({1, 2}, deep_from_dict([1, 2], set))
        self.assertEqual({"1", "2"}, deep_from_dict(["1", "2"], set))

    def test_from_dict_generics(self):
        self.assertEqual({1, 2}, deep_from_dict(["1", "2"], Set[int]))
        self.assertEqual({1, 2}, deep_from_dict(["1", "2", "1"], Set[int]))
        self.assertEqual(
            [(1, "a"), (2, "b")], deep_from_dict([["1", "a"], ["2", "b"]], List[Tuple[int, str]])
        )

    def test_from_dict_simple_type(self):
        doc = {"name": "a", "value": "1"}
        obj = deep_from_dict(doc, SimpleNested)
        self.assertIsInstance(obj, SimpleNested)
        self.assertEqual("a", obj.name)
        self.assertEqual(1, obj.value)

    def test_from_dict_constructor(self):
        doc = {
            "req_a": "a",
            "req_b": 1,
            "opt_c": True,
        }
        obj = deep_from_dict(doc, DataClass)
        self.assertIsInstance(obj, DataClass)
        self.assertEqual("a", obj.req_a)
        self.assertEqual(1, obj.req_b)
        self.assertEqual(True, obj.opt_c)
        self.assertIsNone(obj.opt_d)

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
        doc = json.dumps(self.root, cls=DeepDictEncoder)
        expected = (
            '{"some_int": 42, "some_str": "jaffa kree", "simple_dict": {"a": 1, "b": 2}, "complex_dict": {"1": {"name": "a", "value": 1}, "2": {"name": "b", "value": 2}}, "complex_list": [{"nested_list": [{"name": "a", "value": 1}, {"name": "b", "value": 2}], "nested_tuple": [1, {"name": "a", "value": 1}]}, {"nested_list": [{"name": "b", "value": 2}, {"name": "c", "value": 3}], "nested_tuple": [2, {"name": "b", "value": 2}]}], "__type": "%s"}'
            % fullname(RootClass)
        )
        self.assertEqual(expected, doc)

    def test_decoding(self):
        doc = (
            '{"some_int": 42, "some_str": "jaffa kree", "simple_dict": {"a": 1, "b": 2}, "complex_dict": {"1": {"name": "a", "value": 1}, "2": {"name": "b", "value": 2}}, "complex_list": [{"nested_list": [{"name": "a", "value": 1}, {"name": "b", "value": 2}], "nested_tuple": [1, {"name": "a", "value": 1}]}, {"nested_list": [{"name": "b", "value": 2}, {"name": "c", "value": 3}], "nested_tuple": [2, {"name": "b", "value": 2}]}], "__type": "%s"}'
            % fullname(RootClass)
        )
        root = json.loads(doc, cls=ModifiedDeepDictDecoder)

        self.assertEqualsRoot(root)

    def assertEqualsRoot(self, root):
        self.assertEqual(42, root.some_int)
        self.assertEqual("jaffa kree", root.some_str)
        self.assertEqual(1, root.complex_dict[1].value)  # dict has int as keys!
        self.assertEqual(2, root.complex_dict[2].value)
        self.assertEqual("a", root.complex_dict[1].name)
        self.assertEqual("b", root.complex_dict[2].name)
        self.assertEqual("a", root.complex_list[0].nested_list[0].name)
        self.assertEqual("b", root.complex_list[0].nested_list[1].name)
        self.assertEqual("b", root.complex_list[1].nested_list[0].name)
        self.assertEqual("c", root.complex_list[1].nested_list[1].name)

    def test_normalize_type(self):
        self.assertEqual("unittest.case.TestCase", deep_to_dict(unittest.TestCase))
        self.assertEqual("unittest.case.TestCase.debug", deep_to_dict(unittest.TestCase.debug))
        self.assertEqual("TimeoutError", deep_to_dict(TimeoutError))

    def test_cast_type(self):
        self.assertRaises(TypeError, deep_from_dict, "unittest.case.TestCase", type)

    def test_normalize_exception(self):
        self.assertEqual(("failed",), deep_to_dict(TimeoutError("failed")))
        self.assertEqual(("failed",), deep_to_dict(TimeoutError("failed")))

    def test_cast_exception(self):
        err = deep_from_dict(("failed",), TimeoutError)
        self.assertIsInstance(err, TimeoutError)
        self.assertEqual(("failed",), err.args)

        err = deep_from_dict("failed", TimeoutError)
        self.assertIsInstance(err, TimeoutError)
        self.assertEqual(("failed",), err.args)


if __name__ == "__main__":
    unittest.main()
