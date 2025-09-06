import pytest
from conftest import send_cmd


def test_mset_with_odd_number_of_args_errors(run_server):
    host, port, stop = run_server()
    try:
        with pytest.raises(Exception):
            send_cmd(host, port, "MSET", "k1", "v1", "k2")  # нечётное число аргументов
    finally:
        stop()


def test_expire_wrong_seconds_errors(run_server):
    host, port, stop = run_server()
    try:
        with pytest.raises(Exception):
            send_cmd(host, port, "EXPIRE", "x", "not_a_number")
    finally:
        stop()
