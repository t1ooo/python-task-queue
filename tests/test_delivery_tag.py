from python_task_queue.broker import gen_delivery_tag


def test_uniqueness():
    n = 1000
    s = []
    for _ in range(n):
        s.append(gen_delivery_tag())

    # NOTE: should be uniq
    assert len(set(s)) == n
