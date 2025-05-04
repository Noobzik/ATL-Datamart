import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
from sklearn.preprocessing import StandardScaler

# Function to explore the initial data
def explore_data(df):
    print("Data Shape: ", df.shape)
    print("Data Types: ", df.dtypes)
    print("Memory Usage: ", df.memory_usage(deep=True))
    print("Missing Values:\n", df.isnull().sum())
    print("Basic Statistics:\n", df.describe())

# Function to handle missing values
def handle_missing_values(df):
    # Drop columns with more than 50% missing values
    df = df.dropna(thresh=int(0.5*len(df)), axis=1)
    
    # Impute missing values for numerical columns (mean imputation)
    df.fillna(df.mean(), inplace=True)
    
    # Impute missing values for categorical columns (mode imputation)
    df.fillna(df.mode().iloc[0], inplace=True)
    
    return df

# Function to remove duplicate rows
def remove_duplicates(df):
    df = df.drop_duplicates()
    return df

# Function to remove outliers using IQR
def remove_outliers(df, column_name):
    Q1 = df[column_name].quantile(0.25)
    Q3 = df[column_name].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    df = df[(df[column_name] >= lower_bound) & (df[column_name] <= upper_bound)]
    return df

# Function to fix data types
def fix_data_types(df):
    # Example of date conversion
    df['date_column'] = pd.to_datetime(df['date_column'], errors='coerce')
    
    # Convert numeric columns
    df['numeric_column'] = pd.to_numeric(df['numeric_column'], errors='coerce')
    
    return df

# Function to standardize categorical data
def standardize_categorical(df):
    df['category_column'] = df['category_column'].str.lower().str.strip()
    return df

# Function to clean text data
def clean_text_data(df, column_name):
    df[column_name] = df[column_name].apply(lambda x: re.sub(r'\W', ' ', str(x)))
    df[column_name] = df[column_name].apply(lambda x: x.lower())
    return df

# Function to create new features (e.g., extract year and month from date)
def feature_engineering(df):
    # Example: Extract year and month from a date column
    df['year'] = df['date_column'].dt.year
    df['month'] = df['date_column'].dt.month
    return df

# Function to normalize/scale numerical columns
def scale_data(df, columns):
    scaler = StandardScaler()
    df[columns] = scaler.fit_transform(df[columns])
    return df

# Function to encode categorical variables using one-hot encoding
def encode_categorical(df):
    df = pd.get_dummies(df, drop_first=True)
    return df

# Function to visualize missing values or other data issues
def visualize_data(df):
    sns.heatmap(df.isnull(), cbar=False, cmap='viridis')
    plt.show()

# Function to save the cleaned data to a new file
def save_cleaned_data(df, file_path):
    df.to_csv(file_path, index=False)
    print(f"Cleaned data saved to {file_path}")

# Main cleaning function that calls all steps
def clean_data(df):
    # Step 1: Explore the initial data
    explore_data(df)
    
    # Step 2: Handle missing values
    df = handle_missing_values(df)
    
    # Step 3: Remove duplicates
    df = remove_duplicates(df)
    
    # Step 4: Remove outliers for a specific column (adjust column names as needed)
    df = remove_outliers(df, 'numeric_column')
    
    # Step 5: Fix data types (adjust columns as needed)
    df = fix_data_types(df)
    
    # Step 6: Standardize categorical data (adjust column names as needed)
    df = standardize_categorical(df)
    
    # Step 7: Clean text data (adjust column names as needed)
    df = clean_text_data(df, 'text_column')
    
    # Step 8: Feature engineering (e.g., extract year, month from date)
    df = feature_engineering(df)
    
    # Step 9: Scale numerical columns (adjust column names as needed)
    df = scale_data(df, ['numeric_column'])
    
    # Step 10: Encode categorical variables
    df = encode_categorical(df)
    
    # Step 11: Visualize missing data or issues
    visualize_data(df)
    
    # Step 12: Save cleaned data to a new file
    save_cleaned_data(df, 'cleaned_data.csv')
    
    return df

# Example usage
if __name__ == "__main__":
    # Load your data (adjust file path and type as needed)
    df = pd.read_csv('your_dataset.csv')
    
    # Clean the data
    df_cleaned = clean_data(df)
