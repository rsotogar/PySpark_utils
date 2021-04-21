#this file contains useful methods when dealing with spark dataframes from pyspark

def rename(df, columns):
    for old_column, new_column in columns.items():
        df = df.withColumnRenamed(old_column, new_column)
    return df

