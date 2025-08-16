from typing import Literal

Expected = Literal['timeseries', 'categorical', 'distribution', 'scatter']


def is_sql_shape_valid(expected_schema: str, columns: list[str]) -> bool:
    es = expected_schema.lower()
    cols = [c.lower() for c in columns]
    if es.startswith('timeseries'):
        return 'x' in cols and 'y' in cols
    if es.startswith('categorical') or es.startswith('distribution'):
        return 'label' in cols and 'value' in cols
    if 'scatter' in es:
        return 'x' in cols and 'y' in cols
    return True