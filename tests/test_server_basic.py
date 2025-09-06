import time
import pytest
from conftest import send_cmd


def test_set_get(run_server):
    host, port, stop = run_server()
    try:
        assert send_cmd(host, port, "SET", "a", "1") == 1
        assert send_cmd(host, port, "GET", "a") == "1"
    finally:
        stop()


def test_mset_mget_and_missing(run_server):
    host, port, stop = run_server()
    try:
        assert send_cmd(host, port, "MSET", "k1", "v1", "k2", "v2") == 2
        assert send_cmd(host, port, "MGET", "k1", "k2", "oops") == ["v1", "v2", None]
    finally:
        stop()


def test_delete_and_flush(run_server):
    host, port, stop = run_server()
    try:
        assert send_cmd(host, port, "SET", "x", "1") == 1
        assert send_cmd(host, port, "DELETE", "x") == 1
        assert send_cmd(host, port, "GET", "x") is None
        assert send_cmd(host, port, "MSET", "a", "1", "b", "2", "c", "3") == 3
        removed = send_cmd(host, port, "FLUSH")
        assert removed >= 1
        assert send_cmd(host, port, "GET", "a") is None
    finally:
        stop()


def test_bad_command_returns_error(run_server):
    host, port, stop = run_server()
    try:
        with pytest.raises(Exception):
            send_cmd(host, port, "BOGUS")
    finally:
        stop()
