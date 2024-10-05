from pathlib import Path

from pyspark.sql import DataFrame


def write_df_to_csv(df: DataFrame, output_dir: Path, node_name: str):
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / f"TABLE_{node_name}.csv"

    df.write.csv(str(output_csv), header=True, sep="\t", mode="overwrite")

    return output_csv
