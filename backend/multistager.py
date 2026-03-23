import pandas as pd
import io
from backend.hero_schema import load_hero_schema


def stage_multi(files, mapping):

    hero = load_hero_schema()

    dfs = {}

    for f in files:

        name = f["name"]
        data = f["bytes"]

        if name.endswith(".csv"):
            dfs[name] = pd.read_csv(io.BytesIO(data))
        else:
            dfs[name] = pd.read_excel(io.BytesIO(data))

    # pick main table (largest)
    main = max(dfs.values(), key=lambda x: len(x))

    staged = pd.DataFrame(index=main.index)

    for h in hero:

        if h in mapping:

            src = mapping[h]

            file = src["file"]
            col = src["column"]

            df = dfs[file]

            staged[h] = df[col]

        else:
            staged[h] = None

    return staged
