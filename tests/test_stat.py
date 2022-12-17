from procstat.stat import Stat


def test_inc_default_count():
    stat = Stat()
    stat.inc("foo")
    assert stat.counters["foo"] == 1


def test_inc_specific_count():
    stat = Stat()
    stat.inc("foo", 2)
    assert stat.counters["foo"] == 2


def test_inc_multiple():
    stat = Stat()
    total = 0
    for num in range(5):
        stat.inc("foo", num)
        total += num
        assert stat.counters["foo"] == total


def test_render_moment():
    stat = Stat()
    stat.inc("foo")
    assert stat.render_moment() == "EPS: | TOTAL: foo=1"
