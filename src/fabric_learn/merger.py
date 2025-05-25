from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, lit

# Initialize Spark Session
spark = SparkSession.builder.appName("DataFrameMerge").getOrCreate()

# Assuming you already have these two DataFrames:
# people_df: Your original people DataFrame
# updates_df: DataFrame containing updates to be merged

# Method 1: Using DataFrame operations
def merge_dataframes(people_df, updates_df, key_columns):
    """
    Merge updates DataFrame into people DataFrame based on key columns.

    Args:
        people_df: Original DataFrame
        updates_df: Updates to be merged
        key_columns: List of column names to join on

    Returns:
        Merged DataFrame
    """
    # Get all columns from both DataFrames
    people_columns = people_df.columns
    updates_columns = updates_df.columns

    # Find non-key columns that need to be updated
    update_columns = [col for col in updates_columns if col not in key_columns]

    # Join the DataFrames
    joined_df = people_df.join(
        updates_df,
        on=key_columns,
        how="left_outer"  # Keep all records from people_df
    )

    # Create a new DataFrame with updated values
    # For each column in update_columns, use the value from updates_df if it exists,
    # otherwise use the value from people_df
    select_expr = []

    for col in people_columns:
        if col in update_columns:
            # Use coalesce to prefer the update value if it exists
            select_expr.append(coalesce(f"updates_df.{col}", f"people_df.{col}").alias(col))
        else:
            # For key columns and columns not in updates, just use the original
            select_expr.append(f"people_df.{col}")

    updated_df = joined_df.select(*select_expr)

    # Find records in updates_df that don't exist in people_df (for insertion)
    # Anti-join to find updates that don't match any record in people_df
    new_records_df = updates_df.join(
        people_df,
        on=key_columns,
        how="left_anti"  # Only keep records from updates_df that don't match in people_df
    )

    # Combine the updated records with new records
    result_df = updated_df.union(new_records_df)

    return result_df

# Example usage:
# Assuming 'id' is the key column
result = merge_dataframes(people_df, updates_df, ["id"])

# Method 2: Using Spark SQL's merge capability (Delta Lake)
# Note: This requires the Delta Lake library and tables saved in Delta format

def merge_with_delta(people_df, updates_df, key_columns):
    """
    Merge updates DataFrame into people DataFrame using Delta Lake.

    Args:
        people_df: Original DataFrame (must be a Delta table)
        updates_df: Updates to be merged
        key_columns: List of column names to join on

    Returns:
        None (updates are performed in-place)
    """
    # Convert people_df to Delta format if not already
    people_df.write.format("delta").mode("overwrite").save("/tmp/people_delta")

    # Create a Delta table
    from delta.tables import DeltaTable
    people_delta = DeltaTable.forPath(spark, "/tmp/people_delta")

    # Build the merge condition
    merge_condition = " AND ".join([f"people.{col} = updates.{col}" for col in key_columns])

    # Build the update expressions
    update_expr = {col: f"updates.{col}" for col in updates_df.columns if col not in key_columns}

    # Perform the merge operation
    people_delta.alias("people").merge(
        updates_df.alias("updates"),
        merge_condition
    ).whenMatchedUpdate(
        set=update_expr
    ).whenNotMatchedInsertAll(
    ).execute()

    # Return the updated DataFrame
    return people_delta.toDF()

# Example usage with Delta Lake:
# result = merge_with_delta(people_df, updates_df, ["id"])
