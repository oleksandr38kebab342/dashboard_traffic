import pandas as pd
import os

def load_dataset(dataset_type='synthetic'):

    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(base_path, "data") 
    
    if dataset_type.lower() == 'real':
        file_path = os.path.join(data_path, "dataset.csv")
        print(f"Loading real dataset from {file_path}")
    else:
        file_path = os.path.join(data_path, "dataset1.csv")
        print(f"Loading synthetic dataset from {file_path}")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Dataset file not found: {file_path}")
    
    return pd.read_csv(file_path)

def load_data(file_path=None):

    if file_path is None:
        # Default to synthetic data if no path specified
        return load_dataset('synthetic')
    else:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Data file not found: {file_path}")
        return pd.read_csv(file_path)
