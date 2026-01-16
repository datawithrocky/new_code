from sqlalchemy import create_engine
import os
import pandas as pd
import numpy as np
from data_std import ds


# SOURCE_CONN_STRING = "postgresql+psycopg2://admin:admin123@40.81.136.92/Data_Migration_Pipeline"
# TARGET_CONN_STRING = "postgresql+psycopg2://admin:admin123@40.81.136.92/Data_Migration_Pipeline"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "lnx2304.ch3.dev.i.com")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow_v2")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow_v2")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "darshan")

SOURCE_CONN_STRING = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

TARGET_CONN_STRING = SOURCE_CONN_STRING


def _read_table(engine, table_name, schema_name=None):
    if schema_name:
        return pd.read_sql_table(table_name, engine, schema=schema_name)
    return pd.read_sql_table(table_name, engine)


def _map_columns(source_df, mapping_list, filename):
    missing_sources = []
    mapped_df = pd.DataFrame(index=source_df.index)

    for mapping in mapping_list:
        source_col = mapping["source_column"]
        target_col = mapping["target_column"]
        if source_col not in source_df.columns:
            missing_sources.append(source_col)
            continue
        mapped_df[target_col] = source_df[source_col]

    mapped_df = mapped_df.replace({np.nan: None})
    return mapped_df, missing_sources


def _extract_join_keys(mapping_list):
    source_keys = list({m.get("source_key") for m in mapping_list if m.get("source_key")})
    target_keys = list({m.get("target_key") for m in mapping_list if m.get("target_key")})

    if len(source_keys) != 1 or len(target_keys) != 1:
        raise ValueError(
            "relational=True requires exactly one source_key and one target_key in mapping."
        )

    return source_keys[0], target_keys[0]


def _merge_frames_on_key(frames, join_key):
    if not frames:
        return pd.DataFrame()

    base = frames[0]
    for frame in frames[1:]:
        if join_key not in frame.columns:
            raise ValueError(f"Join key '{join_key}' missing in one of the mapped frames.")

        base = base.merge(frame, on=join_key, how="outer", suffixes=("", "_new"))

        new_cols = [c for c in base.columns if c.endswith("_new")]
        for col in new_cols:
            orig = col[:-4]
            base[orig] = base[col].combine_first(base[orig])
            base = base.drop(columns=[col])

    return base


def file_map(file_dict, schema=None, tabl=None):
    # 1) Print received mapping payload
    pd.set_option("display.max_columns", None)
    print("file_dict", "--------")
    print(file_dict)

    # 2) Create source/target DB engines
    source_engine = create_engine(SOURCE_CONN_STRING)
    target_engine = create_engine(TARGET_CONN_STRING)

    # 3) Collect results per mapping entry
    results = []

    # 3.1) Accumulate per target table (supports multiple source tables)
    target_batches = {}

    # 4) Loop each source table entry in JSON
    for filename, config in file_dict.items():

        # 4.1) Normalize source table name
        source_table = filename
        if ".csv" in source_table:
            source_table = source_table.replace(".csv", "")
        if ".xlsx" in source_table:
            source_table = source_table.replace(".xlsx", "")

        # 4.2) Resolve source/target schema + table
        source_schema = config.get("source_schema")
        target_schema = config.get("schema") or schema
        target_table = config.get("tablename") or tabl
        relational = str(config.get("relational", "False"))

        # 4.3) Validate required target info
        if not target_schema or not target_table:
            raise ValueError("Missing target schema or target table in JSON.")

        # 4.4) Read source table into DataFrame
        source_df = _read_table(source_engine, source_table, source_schema)
        source_df = source_df.replace({np.nan: None})
        mapping_list = config.get("mapping", [])

        # 4.5) Apply mapping rules
        if relational == "True":
            source_key, join_key = _extract_join_keys(mapping_list)
            mapped_df = ds(mapping_list, source_df, config, filename)

            if join_key not in mapped_df.columns:
                if source_key not in source_df.columns:
                    raise ValueError(
                        f"Source key '{source_key}' not found in {source_table}."
                    )
                mapped_df[join_key] = source_df[source_key]
        else:
            mapped_df, missing_sources = _map_columns(
                source_df, mapping_list, filename
            )

        # 4.6) Stage mapped data for target table
        print("mapped_df", "--------")
        print(mapped_df)

        mapped_df = mapped_df.dropna(axis=1, how="all")

        target_table_key = f"{target_schema}.{target_table}"
        if target_table_key not in target_batches:
            target_batches[target_table_key] = {
                "schema": target_schema,
                "table": target_table,
                "frames": [],
                "join_key": join_key if relational == "True" else None,
            }

        if target_batches[target_table_key]["join_key"] and relational != "True":
            raise ValueError(
                "Mixed relational and non-relational mappings for same target table."
            )

        if relational == "True" and target_batches[target_table_key]["join_key"] != join_key:
            raise ValueError(
                "All relational mappings for the same target table must use the same target_key."
            )

        target_batches[target_table_key]["frames"].append(mapped_df)

        # 4.7) Store summary result
        results.append(
            {
                "source_table": source_table,
                "target_schema": target_schema,
                "target_table": target_table,
                "rows_written": len(mapped_df),
            }
        )

    print("results", "--------")
    print(results)

    # 5) Write each target table once (append multiple sources)
    for target_key, batch in target_batches.items():
        if batch["join_key"]:
            combined_df = _merge_frames_on_key(batch["frames"], batch["join_key"])
        else:
            combined_df = pd.concat(batch["frames"], ignore_index=True)

        combined_df.to_sql(
            batch["table"],
            con=target_engine,
            schema=batch["schema"],
            if_exists="replace",
            index=False,
        )

    # 6) Dispose DB engines
    source_engine.dispose()
    target_engine.dispose()

    print("combined_df", "--------")
    print(combined_df)

    # 7) Return summary
    return {"results": results}
