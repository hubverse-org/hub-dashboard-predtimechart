# time-series.csv creation

We created the test [time-series.csv](time-series.csv) file from https://github.com/reichlab/flu-metrocast/blob/main/target-data/time-series.csv in early 2025-03 via these commands:

```python
from pathlib import Path

import polars as pl


path = Path('tests/hubs/flu-metrocast/target-data/time-series.csv')
df = pl.read_csv(path, schema_overrides={'location': pl.String, 'value': pl.Float64, 'observation': pl.Float64},
                 null_values=["NA"])
df = df.filter(pl.col('target_end_date').is_in(['2025-02-15', '2025-02-08', '2025-02-01']))
df.write_csv(path.with_name('time-series-filtered.csv'))
```