import shlex
import socket
from io import TextIOWrapper

from errors import Error, CommandError
from protocol import ProtocolHandler


class Client:
    def __init__(self, host='127.0.0.1', port=31337, timeout=None):
        self._protocol = ProtocolHandler()
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._writer = self._sock.makefile('wb')
        self._reader = TextIOWrapper(self._sock.makefile('rb'), encoding='utf-8', newline='')

    def close(self):
        try: self._writer.close()
        finally:
            try: self._reader.close()
            finally: self._sock.close()

    def execute(self, *args):
        self._protocol.write_response(self._writer, args)
        resp = self._protocol.handle_request(self._reader)
        if isinstance(resp, Error):
            raise CommandError(resp.message)
        return resp


def main(host='127.0.0.1', port=31337):
    print(f"Connected to mini-redis at {host}:{port}. Type EXIT to quit.")
    c = Client(host, port)
    try:
        while True:
            try:
                line = input("mini-redis> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not line:
                continue
            u = line.upper()
            if u in ('EXIT', 'QUIT', 'Q'):
                break
            try:
                parts = shlex.split(line)
            except ValueError as e:
                print(f"(parse) {e}")
                continue
            try:
                resp = c.execute(*parts)
                print(format_resp(resp))
            except CommandError as e:
                print(f"(error) {e}")
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                print(f"(disconnected) {e}")
                break
    finally:
        c.close()


def format_resp(x):
    if x is None:
        return "(nil)"
    if isinstance(x, list):
        return "\n".join(f"{i}) {format_resp(v)}" for i, v in enumerate(x, 1))
    if isinstance(x, dict):
        return "\n".join(f"{k}: {format_resp(v)}" for k, v in x.items())
    return str(x)


if __name__ == "__main__":
    import os
    host = "127.0.0.1"
    port = int(os.getenv("MINIREDIS_PORT", "31337"))
    main(host, port)
