"""to run ETL job"""

import os
from app.src.etl_job import Transformation
from app.config.config import set_env_vars


if __name__ == "__main__":
    set_env_vars()
    input_path = os.getenv("input_path_dataset")
    output_path = os.getenv("output_path_dataset")
    # create object transformer
    transformer = Transformation(input_path)
    # apply transformation
    final_dataset = transformer.apply_transformations()
    print("transformation is done")
    print(final_dataset.count())
    # load
    transformer.write_dataframe_to_parquet(final_dataset, output_path)
