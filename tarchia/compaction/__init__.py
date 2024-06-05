"""
For compaction we rewrite data into 2Mb blocks.

We do that by reading 10k rows
    We convert that to parquet/Draken SST and measure the size
    if we're within 10Kb of 2Mb
        commit
    else:
        We estimate either
            - The number of rows to loose to get back to 2Mb
            - The number of rows to add to get to 2Mb
        We add/remove those rows

"""
