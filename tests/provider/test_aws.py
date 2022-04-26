import pytest

from pymq.provider.aws import decode_topic_name, encode_topic_name, validate_topic_name


def test_validate_topic_name():
    with pytest.raises(ValueError):
        validate_topic_name("")

    with pytest.raises(ValueError):
        validate_topic_name("foo.bar")

    with pytest.raises(ValueError):
        validate_topic_name("#!$")

    with pytest.raises(ValueError):
        validate_topic_name("foo.<locals>")

    with pytest.raises(ValueError):
        validate_topic_name("a" * 257)

    validate_topic_name("foobar")
    validate_topic_name("123_foo-bar_")
    validate_topic_name("a" * 256)


def test_encode_topic_name():
    validate_topic_name(encode_topic_name("foo"))
    validate_topic_name(encode_topic_name("foo/bar"))
    validate_topic_name(encode_topic_name("foo/*"))
    validate_topic_name(encode_topic_name("__main__.foobar"))
    validate_topic_name(encode_topic_name("__main__.foobar:Event"))
    validate_topic_name(encode_topic_name("tests.remote.<locals>.remote_test_fn"))


def test_encode_decode_topic_name():
    def test(name):
        assert decode_topic_name(encode_topic_name(name)) == name

    test("foo")
    test("foo/bar")
    test("foo/*")
    test("__main__.foobar")
    test("__main__.foobar:Event")
    test("tests.remote.<locals>.remote_test_fn")

    with pytest.raises(AssertionError):
        test("._DOT_.")  # limitation of the encoding
