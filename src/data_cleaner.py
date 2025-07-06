import pandas as pd

def clean_data(df, numeric_columns):
    df = df.dropna()
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna()  # Після приведення типів ще раз чистимо
    return df
