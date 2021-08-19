# procstat

This library helps to count runtime metrics. For specific events you increment manually the
corresponding counters. By default the library dumps all counters into stderr each 3 seconds.
It also can display the speed of metric is changing.

It also can dump metrics into influxdb, will be documented later, see source code for details.

Usage:

```
import time
from procstat import Stat

stat = Stat(speed_keys=['foo', 'bar'])
while True:
    stat.inc('foo') # default increment is 1
    stat.inc('bar', 2)
    stat.inc('baz', 3)
    time.sleep(1)
```
