from io import BytesIO
from mini_redis.protocol import ProtocolHandler
from mini_redis.errors import Error


def _roundtrip(data):
    p = ProtocolHandler()
    buf = BytesIO()
    p._write(buf, data)
    buf.seek(0)

    class R:
        def __init__(self, b): self._b = b

        def read(self, n):
            s = self._b.read(n).decode("utf-8")
            return s

        def readline(self):
            chunk = b""
            while True:
                c = self._b.read(1)
                if not c: break
                chunk += c
                if c == b"\n": break
            return chunk.decode("utf-8")

    r = R(BytesIO(buf.getvalue()))
    return p.handle_request(r)


def test_simple_types_roundtrip_string():
    assert _roundtrip("hello") == "hello"


def test_simple_types_roundtrip_int():
    assert _roundtrip(42) == 42


def test_array_and_nil_roundtrip():
    data = ["a", None, "b"]
    got = _roundtrip(data)
    assert got == data


def test_error_serialization():
    p = ProtocolHandler()
    out = BytesIO()
    p._write(out, Error("boom"))
    assert out.getvalue().startswith(b"-boom")
