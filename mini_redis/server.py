import os
import threading
import socketserver
import time
from io import TextIOWrapper
from time import monotonic

from mini_redis.errors import CommandError, Error, Disconnect
from mini_redis.protocol import ProtocolHandler


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

        self._expiry = {}

        _env = os.getenv("MINIREDIS_DEFAULT_TTL", "-1")
        try:
            _val = int(_env)
        except ValueError:
            _val = -1
        self._default_ttl = None if _val < 0 else _val

        self._commands = self.get_commands()

        handler_cls = self._make_handler_class()
        self._server = ThreadingLimitedTCPServer((host, port), handler_cls, max_clients=max_clients)
        self._server.app = self

        self._reaper_thr = threading.Thread(target=self._reaper_loop, daemon=True)
        self._reaper_thr.start()

    def get_commands(self):
        return {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            'FLUSH': self.flush,
            'MGET': self.mget,
            'MSET': self.mset,
            'EXPIRE': self.expire,
            'TTL': self.ttl,
            'DEFAULTTTL': self.defaultttl,
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
            self._purge_if_expired(key)
            return self._kv.get(key)

    def set(self, key, value):
        with self._lock:
            self._kv[key] = value
            self._expiry.pop(key, None)
            self._apply_default_ttl_unlocked(key)
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
        if len(items) % 2:
            raise CommandError('MSET requires even number of arguments: key value ...')
        with self._lock:
            now = monotonic()
            for k, v in zip(items[::2], items[1::2]):
                self._kv[k] = v
                self._expiry.pop(k, None)
                if self._default_ttl is not None:
                    self._expiry[k] = now + self._default_ttl
            return len(items)//2

    def expire(self, key, seconds):
        try:
            seconds = int(seconds)
        except ValueError:
            raise CommandError('EXPIRE seconds must be integer')
        if seconds < 0:
            seconds = 0
        with self._lock:
            if key not in self._kv:
                return 0
            self._expiry[key] = monotonic() + seconds
            return 1

    def ttl(self, key):
        with self._lock:
            if key not in self._kv:
                return -2
            exp = self._expiry.get(key)
            if exp is None:
                return -1
            now = monotonic()
            if exp <= now:
                self._kv.pop(key, None)
                self._expiry.pop(key, None)
                return -2
            return int(exp - now)

    # NEW: управление дефолтным TTL
    def defaultttl(self, *args):
        """
        DEFAULTTTL            -> вернуть текущее (сек) или -1, если выключен
        DEFAULTTTL <seconds>  -> установить (нев отрицательное)
        DEFAULTTTL OFF        -> выключить
        """
        if len(args) == 0:
            return -1 if self._default_ttl is None else int(self._default_ttl)
        if len(args) != 1:
            raise CommandError('DEFAULTTTL takes 0 or 1 arg (seconds|OFF)')
        val = args[0]
        if isinstance(val, str) and val.upper() == 'OFF':
            with self._lock:
                self._default_ttl = None
            return 1
        try:
            sec = int(val)
            if sec < 0:
                raise ValueError()
        except ValueError:
            raise CommandError('DEFAULTTTL must be non-negative integer or OFF')
        with self._lock:
            self._default_ttl = sec
        return 1

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

    def _purge_if_expired(self, key, now=None):
        if now is None: now = monotonic()
        exp = self._expiry.get(key)
        if exp is not None and exp <= now:
            self._kv.pop(key, None)
            self._expiry.pop(key, None)
            return True
        return False

    def _apply_default_ttl_unlocked(self, key, now=None):
        if self._default_ttl is None:
            return
        if now is None: now = monotonic()
        self._expiry[key] = now + self._default_ttl

    def _reaper_loop(self):
        while True:
            time.sleep(1)
            now = monotonic()
            with self._lock:
                expired = [k for k, t in self._expiry.items() if t <= now]
                for k in expired:
                    self._kv.pop(k, None)
                    self._expiry.pop(k, None)


if __name__ == '__main__':
    host = os.getenv("BIND", "127.0.0.1")
    port = int(os.getenv("PORT", "31337"))
    srv = KVServer(host=host, port=port, max_clients=64)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print('\nShutting down...')
        srv.shutdown()
