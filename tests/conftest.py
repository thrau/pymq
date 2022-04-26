import logging
import os
import shutil
import sys

import pytest
from localstack.utils.bootstrap import LocalstackContainer, LocalstackContainerServer

import pymq as pymq

LOG = logging.getLogger(__name__)


def pytest_runtest_setup(item):
    # handle xfail_provider marker

    try:
        init_fn = item.callspec.params.get("pymq_init")
    except AttributeError:
        # functions without arguments don't have callspecs
        return

    if not init_fn:
        return

    for marker in item.iter_markers():
        if marker.name == "xfail_provider":
            if init_fn in marker.args:
                pytest.xfail("text expected to fail for %s" % init_fn)


@pytest.fixture(params=["init_simple", "init_redis", "init_ipc", "init_aws"])
def pymq_init(request):
    """parameterized pymq_init fixture (a fixture that, when called, initializes pymq)"""

    if request.param == "init_ipc" and sys.platform != "linux":
        pytest.skip("IPC provider only works for linux")
        return None

    if request.param == "init_aws" and not shutil.which("docker"):
        pytest.skip("Cannot test AWS provider without docker (needed to run localstack)")
        return None

    return request.getfixturevalue(request.param)


@pytest.fixture()
def bus(pymq_init) -> pymq.EventBus:
    """initialized pymq bus as fixture"""
    bus = pymq_init()
    setattr(pymq, "type", type(bus))
    yield pymq


# simple provider


@pytest.fixture()
def init_simple():
    from pymq.provider.simple import SimpleEventBus

    config = SimpleEventBus

    _bus_container = []
    _invalid = False

    def _init() -> SimpleEventBus:
        if _bus_container:
            raise ValueError("already called")
        if _invalid:
            raise ValueError("expired init function")

        bus = pymq.init(config)
        _bus_container.append(bus)
        return bus

    yield _init

    _invalid = True

    if _bus_container:
        pymq.shutdown()
        _bus_container[0].close()


@pytest.fixture()
def pymq_simple(init_simple):
    init_simple()
    yield pymq


# ipc provider


@pytest.fixture(scope="class")
def _ipc_cleanup():
    from pymq.provider.ipc import IpcQueue

    yield
    IpcQueue("pymq_global_test_queue").free()
    IpcQueue("pymq_global_test_queue_1").free()
    IpcQueue("pymq_global_test_queue_2").free()


@pytest.fixture()
def init_ipc(_ipc_cleanup):
    from pymq.provider.ipc import IpcConfig, IpcEventBus

    config = IpcConfig()

    _bus_container = []
    _invalid = False

    def _init() -> IpcEventBus:
        if _bus_container:
            raise ValueError("already called")
        if _invalid:
            raise ValueError("expired init function")

        bus = pymq.init(config)
        _bus_container.append(bus)
        return bus

    yield _init

    _invalid = True

    if _bus_container:
        pymq.shutdown()
        _bus_container[0].close()


@pytest.fixture()
def pymq_ipc(init_ipc):
    init_ipc()
    yield pymq


# redis provider


@pytest.fixture(scope="class")
def redislite(tmp_path_factory):
    import redislite

    rds: redislite.Redis
    tmp = tmp_path_factory.mktemp("redislite", numbered=True)
    print(tmp)
    tmpfile = tmp / "pymq_test.db"
    rds = redislite.Redis(str(tmpfile), decode_responses=True)
    rds.get("dummykey")  # run a first command to initiate

    yield rds

    rds.shutdown()
    os.remove(rds.redis_configuration_filename)
    os.remove(rds.settingregistryfile)
    shutil.rmtree(rds.redis_dir)


@pytest.fixture()
def init_redis(redislite):
    from pymq.provider.redis import RedisConfig, RedisEventBus

    config = RedisConfig(redislite)

    _bus_container = []
    _invalid = False

    def _init() -> RedisEventBus:
        if _bus_container:
            raise ValueError("already called")
        if _invalid:
            raise ValueError("expired init function")

        bus = pymq.init(config)
        _bus_container.append(bus)
        return bus

    yield _init

    _invalid = True

    if _bus_container:
        pymq.shutdown()
        _bus_container[0].close()

    redislite.flushall()


@pytest.fixture()
def pymq_redis(init_redis):
    init_redis()
    yield pymq


# aws provider through localstack


@pytest.fixture()
def pymq_aws(init_aws):
    init_aws()
    yield pymq


@pytest.fixture()
def init_aws(localstack):
    from pymq.provider.aws import AwsEventBus, LocalstackConfig

    config = LocalstackConfig(endpoint_url=localstack.url)

    _bus_container = []
    _invalid = False

    def _init() -> AwsEventBus:
        if _bus_container:
            raise ValueError("already called")
        if _invalid:
            raise ValueError("expired init function")

        bus = pymq.init(config)
        _bus_container.append(bus)
        return bus

    yield _init

    _invalid = True

    if _bus_container:
        pymq.shutdown()
        _bus_container[0].close()


@pytest.fixture(scope="class")
def localstack():
    container = LocalstackContainer()
    container.ports.add(4566, 4566)
    ls = LocalstackContainerServer(container)
    ls.start()
    ls.wait_is_up()
    yield ls
    ls.shutdown()
