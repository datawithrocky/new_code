def ds(file_mapping_list, source_df, config, source_name):
    print("file_mapping_list", "--------")
    print(file_mapping_list)
    print("source_df", "--------")
    print(source_df)
    print("config", "--------")
    print(config)
    print("source_name", "--------")
    print(source_name)

    mapped_df = pd.DataFrame(index=source_df.index)
    print("mapped_df", "--------")
    print(mapped_df)

    for mapping in file_mapping_list:
        source_col = mapping["source_column"]
        target_col = mapping["target_column"]

        if source_col not in source_df.columns:
            continue

        mapped_df[target_col] = source_df[source_col]

    mapped_df = mapped_df.replace({np.nan: None})
    return mapped_df
