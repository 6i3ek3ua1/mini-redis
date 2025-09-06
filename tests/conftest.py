import socket
import threading
import time

import pytest

from mini_redis import server as server_mod
from mini_redis.protocol import ProtocolHandler
from mini_redis.errors import Error, CommandError
from io import TextIOWrapper


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture
def run_server(monkeypatch):
    def _start(default_ttl_env=None):
        if default_ttl_env is not None:
            monkeypatch.setenv("MINIREDIS_DEFAULT_TTL", str(default_ttl_env))
        host = "127.0.0.1"
        port = _free_port()
        srv = server_mod.KVServer(host=host, port=port)
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()

        time.sleep(0.05)

        def _stop():
            try:
                srv._server.shutdown()
            except Exception:
                pass
            t.join(timeout=2)

        return (host, port, _stop)

    return _start


def send_cmd(host, port, *args):
    proto = ProtocolHandler()
    with socket.create_connection((host, port)) as sock:
        writer = sock.makefile("wb")
        reader = TextIOWrapper(sock.makefile("rb"), encoding="utf-8", newline="")
        proto.write_response(writer, list(args))
        resp = proto.handle_request(reader)
        if isinstance(resp, Error):
            raise CommandError(resp.message)
        return resp
