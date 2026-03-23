import pandas as pd
from backend.hero_schema import load_hero_schema


def stage(df, mapping):

    hero = load_hero_schema()

    staged = pd.DataFrame(columns=hero)

    for h in hero:

        # find source col that maps to this hero col
        src = None

        for k,v in mapping.items():
            if v == h:
                src = k
                break

        if src and src in df.columns:
            staged[h] = df[src]
        else:
            staged[h] = None

    return staged
