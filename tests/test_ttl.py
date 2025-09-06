import time
from conftest import send_cmd


def test_expire_and_ttl_basic(run_server):
    host, port, stop = run_server()
    try:
        assert send_cmd(host, port, "SET", "t", "v") == 1
        assert send_cmd(host, port, "EXPIRE", "t", "1") == 1
        ttl = send_cmd(host, port, "TTL", "t")
        assert ttl in (1, 0)
        time.sleep(1.2)
        assert send_cmd(host, port, "GET", "t") is None
        assert send_cmd(host, port, "TTL", "t") == -2
    finally:
        stop()


def test_ttl_no_expire_is_minus_one(run_server):
    host, port, stop = run_server()
    try:
        assert send_cmd(host, port, "SET", "a", "1") == 1
        assert send_cmd(host, port, "TTL", "a") == -1
    finally:
        stop()


def test_default_ttl_via_env(run_server):
    host, port, stop = run_server(default_ttl_env=1)
    try:
        assert send_cmd(host, port, "SET", "b", "2") == 1
        ttl = send_cmd(host, port, "TTL", "b")
        assert ttl in (1, 0)
        time.sleep(1.2)
        assert send_cmd(host, port, "GET", "b") is None
    finally:
        stop()
