import threading
import socketserver
from io import TextIOWrapper

from errors import CommandError, Error, Disconnect
from protocol import ProtocolHandler


class ThreadingLimitedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address, RequestHandlerClass, max_clients=64, bind_and_activate=True):
        super().__init__(server_address, RequestHandlerClass, bind_and_activate)
        self.clients_sem = threading.Semaphore(max_clients)


class KVServer:
    def __init__(self, host='127.0.0.1', port=31337, max_clients=64):
        self.host = host
        self.port = port

        self._kv = {}
        self._lock = threading.RLock()

        self._protocol = ProtocolHandler()
        self._commands = self.get_commands()

        handler_cls = self._make_handler_class()
        self._server = ThreadingLimitedTCPServer((host, port), handler_cls, max_clients=max_clients)
        self._server.app = self

    def get_commands(self):
        return {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            'FLUSH': self.flush,
            'MGET': self.mget,
            'MSET': self.mset,
        }

    def get_response(self, data):
        if not isinstance(data, list):
            try:
                data = data.split()
            except Exception:
                raise CommandError('Request must be list or simple string.')

        if not data:
            raise CommandError('Missing command')

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError('Unrecognized command: %s' % command)

        return self._commands[command](*data[1:])

    def get(self, key):
        with self._lock:
            return self._kv.get(key)

    def set(self, key, value):
        with self._lock:
            self._kv[key] = value
            return 1

    def delete(self, key):
        with self._lock:
            if key in self._kv:
                del self._kv[key]
                return 1
            return 0

    def flush(self):
        with self._lock:
            n = len(self._kv)
            self._kv.clear()
            return n

    def mget(self, *keys):
        with self._lock:
            return [self._kv.get(key) for key in keys]

    def mset(self, *items):
        if len(items) % 2 != 0:
            raise CommandError('MSET requires even number of arguments: key value [key value] ...')
        with self._lock:
            for k, v in zip(items[::2], items[1::2]):
                self._kv[k] = v
            return len(items) // 2

    def _make_handler_class(self):
        app = self
        proto = self._protocol

        class Handler(socketserver.StreamRequestHandler):
            def handle(self):
                app._server.clients_sem.acquire()
                try:
                    reader = TextIOWrapper(self.rfile, encoding='utf-8', newline='')

                    while True:
                        try:
                            req = proto.handle_request(reader)
                        except Disconnect:
                            break
                        except Exception as e:
                            proto.write_response(self.wfile, Error(str(e)))
                            break

                        try:
                            resp = app.get_response(req)
                        except CommandError as e:
                            resp = Error(str(e))
                        except Exception as e:
                            resp = Error('internal error: %s' % e)

                        proto.write_response(self.wfile, resp)

                finally:
                    app._server.clients_sem.release()

        return Handler

    def serve_forever(self):
        print(f'Serving on {self.host}:{self.port}')
        try:
            self._server.serve_forever()
        finally:
            self._server.server_close()

    def shutdown(self):
        self._server.shutdown()
        self._server.server_close()


if __name__ == '__main__':
    srv = KVServer(host='127.0.0.1', port=31337, max_clients=64)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print('\nShutting down...')
        srv.shutdown()
