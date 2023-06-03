# We know, this is not real test

import datetime as dt
import pandas as pd

import pandas_business


df = pd.DataFrame(
    [
        {'month_subscribed': dt.datetime(2020, 1, 1), 'unsubscribed_at': dt.datetime(2020, 3, 1), 'client_life': 2},
        {'month_subscribed': dt.datetime(2020, 1, 1), 'unsubscribed_at': dt.datetime(2020, 3, 1), 'client_life': 2},
        {'month_subscribed': dt.datetime(2020, 1, 1), 'unsubscribed_at': dt.datetime(2020, 4, 1), 'client_life': 3},
        {'month_subscribed': dt.datetime(2020, 2, 1), 'unsubscribed_at': dt.datetime(2020, 3, 1), 'client_life': 1},
        {'month_subscribed': dt.datetime(2020, 2, 1), 'unsubscribed_at': dt.datetime(2020, 3, 1), 'client_life': 1},
        {'month_subscribed': dt.datetime(2020, 2, 1), 'unsubscribed_at': dt.datetime(2020, 4, 1), 'client_life': 2},
    ]
)


df_cohort = df.cohort(
    cohort_event_cols=["month_subscribed"],
    transaction_event_col="unsubscribed_at",
    metrics={"client_life": ["mean"]},
    row_col_granularities=[("monthly", "monthly")],
)
