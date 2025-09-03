import socketserver
from dataclasses import dataclass

from protocol import ProtocolHandler


class CommandError(Exception):
    pass


class Disconnect(Exception):
    pass


@dataclass(frozen=True)
class Error:
    message: str


class RequestHandler(socketserver.StreamRequestHandler):
    def setup(self):
        super().setup()
        self.rtext = self.rfile.detach().makefile("r", encoding="utf-8", newline="")
        self.wbuf  = self.wfile.detach().makefile("wb", buffering=0)

    def handle(self):

        while True:
            try:
                _req = self.server.protocol.handle_request(self.rtext)
                resp = "OK"
            except Disconnect:
                break
            except CommandError as exc:
                resp = Error(exc.args[0])
            except Exception as exc:
                resp = Error(f"internal error: {type(exc).__name__}")

            self._write_response(resp)

    def _write_response(self, resp):
        if isinstance(resp, Error):
            self.wbuf.write(b"-ERR " + resp.message.encode("utf-8") + b"\r\n")
        else:
            self.wbuf.write(b"+OK\r\n")


class CacheTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


class Server:
    def __init__(self, host="127.0.0.1", port=6380):
        self.protocol = ProtocolHandler()
        self._tcp = CacheTCPServer((host, port), RequestHandler)
        self._tcp.protocol = self.protocol

    def run(self):
        host, port = self._tcp.server_address
        print(f"listening on {host}:{port}  (Ctrl+C to stop)")
        try:
            self._tcp.serve_forever()
        except KeyboardInterrupt:
            print("\nshutting down...")
        finally:
            self._tcp.shutdown()
            self._tcp.server_close()


if __name__ == "__main__":
    Server().run()
