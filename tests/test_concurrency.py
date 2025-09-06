import concurrent.futures as cf
from conftest import send_cmd


def test_parallel_set_and_mget(run_server):
    host, port, stop = run_server()
    try:
        N = 50
        keys = [f"k{i}" for i in range(N)]
        vals = [f"v{i}" for i in range(N)]

        def do_set(i):
            return send_cmd(host, port, "SET", keys[i], vals[i])

        with cf.ThreadPoolExecutor(max_workers=20) as ex:
            results = list(ex.map(do_set, range(N)))

        assert all(r == 1 for r in results)

        got = send_cmd(host, port, "MGET", *keys)
        assert got == vals
    finally:
        stop()
