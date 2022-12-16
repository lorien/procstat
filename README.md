# Procstat Documentation

[![Test Status](https://github.com/lorien/procstat/actions/workflows/test.yml/badge.svg)](https://github.com/lorien/procstat/actions/workflows/test.yml)
[![Code Quality](https://github.com/lorien/procstat/actions/workflows/check.yml/badge.svg)](https://github.com/lorien/procstat/actions/workflows/test.yml)
[![Type Check](https://github.com/lorien/procstat/actions/workflows/mypy.yml/badge.svg)](https://github.com/lorien/procstat/actions/workflows/mypy.yml)
[![Test Coverage Status](https://coveralls.io/repos/github/lorien/procstat/badge.svg)](https://coveralls.io/github/lorien/procstat)
[![Documentation Status](https://readthedocs.org/projects/procstat/badge/?version=latest)](http://user-agent.readthedocs.org)

This library helps to count runtime metrics. For specific events you increment manually the
corresponding counters. By default the library dumps all counters into stderr each 3 seconds.
It also can display the speed of metric is changing.

It also can dump metrics into influxdb, will be documented later, see source code for details.

# Installation

```shell
pip install -U procstat
```

# Usage

```python
import time
from procstat import Stat

stat = Stat(speed_keys=['foo', 'bar'])
while True:
    stat.inc('foo') # default increment is 1
    stat.inc('bar', 100)
    stat.inc('baz', 500)
    time.sleep(1)
```

That code will produce output like:
```
DEBUG:procstat:EPS: bar: 0.0, foo: 0.0 | TOTAL:
DEBUG:procstat:EPS: bar: 1.0, foo: 0.0+ | TOTAL: bar: 40, baz: 80, foo: 4
DEBUG:procstat:EPS: bar: 2.0, foo: 0.0+ | TOTAL: bar: 70, baz: 140, foo: 7
DEBUG:procstat:EPS: bar: 3.0, foo: 0.0+ | TOTAL: bar: 100, baz: 200, foo: 10
DEBUG:procstat:EPS: bar: 4.0, foo: 0.0+ | TOTAL: bar: 130, baz: 260, foo: 13
DEBUG:procstat:EPS: bar: 5.0, foo: 0.0+ | TOTAL: bar: 160, baz: 320, foo: 16
DEBUG:procstat:EPS: bar: 6.0, foo: 0.0+ | TOTAL: bar: 190, baz: 380, foo: 19
DEBUG:procstat:EPS: bar: 7.0, foo: 0.0+ | TOTAL: bar: 220, baz: 440, foo: 22
DEBUG:procstat:EPS: bar: 8.0, foo: 0.0+ | TOTAL: bar: 250, baz: 500, foo: 25
DEBUG:procstat:EPS: bar: 9.0, foo: 0.0+ | TOTAL: bar: 280, baz: 560, foo: 28
DEBUG:procstat:EPS: bar: 10.0, foo: 1.0 | TOTAL: bar: 310, baz: 620, foo: 31
DEBUG:procstat:EPS: bar: 10.0, foo: 1.0 | TOTAL: bar: 340, baz: 680, foo: 34
DEBUG:procstat:EPS: bar: 10.0, foo: 1.0 | TOTAL: bar: 370, baz: 740, foo: 37
...
```
