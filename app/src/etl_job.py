"""ETL job script"""

import glob
from pyspark.sql import SparkSession, DataFrame
from app.utils.funcs import select_filename


class Transformation:
    spark = SparkSession.builder.appName("Pyspark-exercice").getOrCreate()
    table_names = [
        "maag_master_agrem",
        "rtpa_ref_third_party",
        "maag_repa_rrol_linked",
        "maag_raty_linked",
        "reac_ref_act_type",
    ]


    def __init__(self, input_path: str):
        """constructor"""
        self.input_path = input_path


    def read_parquet_file(self, parquet_file_path: str) -> DataFrame:
        """Loads the csv file and returns the result as a DataFrame"""
        dataset = self.spark.read.parquet(parquet_file_path)
        return dataset


    def select_path_files(self) -> list:
        """return list of paths for tables"""
        list_of_files = glob.glob(self.input_path + "/*")  # * means all if need specific format then *.cs
        res = [f for f in list_of_files if f.split("/")[-1] in self.table_names]
        return res


    def apply_transformations(self) -> DataFrame:
        """apply all transformations on the dataset"""

        # read datasets
        list_of_files = self.select_path_files()
        print("****ETL JOB start****")
        for file in list_of_files:
            sub_files = glob.glob(file + "/*")
            latest_file = select_filename(sub_files) + "/n_applic_infq=38"
            key = "df_{0}".format(file.split("/")[-1])
            if key == "df_maag_repa_rrol_linked":
                df_maag_repa_rrol_linked = self.read_parquet_file(latest_file)
            if key == "df_maag_master_agrem":
                df_maag_master_agrem = self.read_parquet_file(latest_file)
            if key == "df_maag_raty_linked":
                df_maag_raty_linked = self.read_parquet_file(latest_file)
            if key == "df_reac_ref_act_type":
                df_reac_ref_act_type = self.read_parquet_file(latest_file)
            if key == "df_rtpa_ref_third_party":
                df_rtpa_ref_third_party = self.read_parquet_file(latest_file)
            # globals()[key] = self.read_parquet_file(latest_file)

        # rename column
        df_maag_repa_rrol_linked = df_maag_repa_rrol_linked.withColumnRenamed(
            "c_part_refer", "c_thir_part_refer"
        )
        # select columns
        df_maag_master_agrem = df_maag_master_agrem.select(
            [
                "C_ACT_TYPE",
                "D_CRDT_COMMITTEE_APRV",
                "CENMES",
                "L_MAST_AGREM_NAME",
                "C_ACT_MNG_STG",
                "C_MAST_AGREM_REFER",
                "N_APPLIC_INFQ",
                "N_MAST_AGREM_VALI_PER",
            ]
        )
        df_maag_raty_linked = df_maag_raty_linked.select(
            ["C_M", "M_ORIG", "M_ORIG_SHAR", "C_MAST_AGREM_REFER", "N_APPLIC_INFQ"]
        )
        df_reac_ref_act_type = df_reac_ref_act_type.select(
            ["C_ACT_TYPE", "L_ACT_TYPE", "N_APPLIC_INFQ"]
        )
        df_rtpa_ref_third_party = df_rtpa_ref_third_party.select(
            ["C_THIR_PART_REFER", "L_THIR_PART_NAME", "N_APPLIC_INFQ"]
        )
        # left join
        df1 = df_maag_master_agrem.join(
            df_reac_ref_act_type, on=["C_ACT_TYPE", "N_APPLIC_INFQ"], how="left"
        )
        df2 = df1.join(
            df_maag_repa_rrol_linked,
            on=["N_APPLIC_INFQ", "C_MAST_AGREM_REFER"],
            how="left",
        )
        # df3 = df2.join(df_maag_raty_linked, on="C_MAST_AGREM_REFER", how="left")
        df3 = df2.join(df_maag_raty_linked,on=["N_APPLIC_INFQ","C_MAST_AGREM_REFER"],how="left")
        df4 = df3.join(
            df_rtpa_ref_third_party,
            on=["N_APPLIC_INFQ", "C_THIR_PART_REFER"],
            how="left",
        )
        return df4

    @staticmethod
    def write_dataframe_to_parquet(dataframe: DataFrame, output_path: str) -> None:
        """save pyspark dataframe in parquet file"""
        dataframe.write.mode("overwrite").parquet(output_path)
