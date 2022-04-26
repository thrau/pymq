import pytest

from pymq.provider.aws import encode_topic_name, validate_topic_name


def test_validate_topic_name():
    with pytest.raises(ValueError):
        validate_topic_name("")

    with pytest.raises(ValueError):
        validate_topic_name("foo.bar")

    with pytest.raises(ValueError):
        validate_topic_name("#!$")

    with pytest.raises(ValueError):
        validate_topic_name("a" * 257)

    validate_topic_name("foobar")
    validate_topic_name("123_foo-bar_")
    validate_topic_name("a" * 256)


def test_encode_topic_name():
    validate_topic_name(encode_topic_name("__main__.foobar"))
    validate_topic_name(encode_topic_name("__main__.foobar:Event"))
    validate_topic_name(encode_topic_name("foo/bar"))
