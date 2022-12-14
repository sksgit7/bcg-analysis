import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *

if os.path.exists("src.zip"):
    sys.path.insert(0, "src.zip")
else:
    sys.path.insert(0, "./src")

from utilities import utils


class CarCrash:
    def __init__(self, config_path):
        """
        Initialize dataframes
        :param config_path: path of the config file
        """
        input_paths = utils.read_config(config_path).get("input_paths")
        self.persons = utils.load_df(spark, input_paths.get("persons"))
        self.units = utils.load_df(spark, input_paths.get("units"))
        self.damages = utils.load_df(spark, input_paths.get("damages"))
        self.charges = utils.load_df(spark, input_paths.get("charges"))
        self.endorses = utils.load_df(spark, input_paths.get("endorses"))
        self.restricts = utils.load_df(spark, input_paths.get("restricts"))

    def analysis1(self, output_path):
        """
        Find the number of crashes (accidents) in which number of persons killed are male?
        :param output_path: output file path
        :return: list of crash ids
        """

        df = (
            self.persons.select("CRASH_ID")
            .subtract(
                self.persons.filter(
                    (self.persons.PRSN_GNDR_ID != "MALE")
                    & (self.persons.DEATH_CNT == 1)
                )
                .groupBy("CRASH_ID")
                .agg(count("*").alias("count"))
                .filter(col("count") > 0)
                .select("CRASH_ID")
            )
            .orderBy("CRASH_ID")
        )

        utils.write_csv(df, output_path)
        return df.count()

    def analysis2(self, output_path):
        """
        How many two wheelers are booked for crashes?
        :param output_path: output file path
        :return: number of two wheelers booked
        """

        units_clean = self.units.dropDuplicates(["CRASH_ID", "UNIT_NBR"])
        df = units_clean.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE")).agg(
            count("*").alias("TWO_WHEELERS_BOOKED")
        )

        utils.write_csv(df, output_path)
        return df.collect()[0][0]

    def analysis3(self, output_path):
        """
        Which state has highest number of accidents in which females are involved?
        :param output_path: output file path
        :return: State name
        """

        df = (
            self.persons.filter(col("PRSN_GNDR_ID") == "FEMALE")
            .groupBy("DRVR_LIC_STATE_ID")
            .agg(count("*").alias("CRASH_COUNT"))
            .orderBy(col("CRASH_COUNT").desc())
            .limit(1)
        )

        utils.write_csv(df, output_path)
        return df.collect()[0][0]

    def analysis4(self, output_path):
        """
        Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        Note - Taking sum of TOT_INJRY_CNT and DEATH_CNT into consideration.
        :param output_path: output file path
        :return: Vehicle make IDs as list
        """

        units_clean = self.units.dropDuplicates(["CRASH_ID", "UNIT_NBR"])
        df = spark.createDataFrame(
            units_clean.withColumn(
                "SUM_INJRY_DEATH", col("TOT_INJRY_CNT") + col("DEATH_CNT")
            )
            .groupBy("VEH_MAKE_ID")
            .agg(sum("SUM_INJRY_DEATH").alias("SUM_INJRY_DEATH_PER_VEH_MAKE_ID"))
            .orderBy(col("SUM_INJRY_DEATH_PER_VEH_MAKE_ID").desc())
            .limit(15)
            .tail(11)
        )

        utils.write_csv(df, output_path)
        return [row[0] for row in df.collect()]

    def analysis5(self, output_path):
        """
        For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
        :param output_path: output file path
        :return: None
        """

        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        units_clean = self.units.dropDuplicates(["CRASH_ID", "UNIT_NBR"])
        df = (
            units_clean.join(
                self.persons,
                (self.units["CRASH_ID"] == self.persons["CRASH_ID"])
                & (self.units["UNIT_NBR"] == self.persons["UNIT_NBR"]),
                how="inner",
            )
            .filter(
                ~self.units["VEH_BODY_STYL_ID"].isin(["NA", "UNKNOWN", "NOT REPORTED"])
            )
            .filter(~self.persons["PRSN_ETHNICITY_ID"].isin(["NA", "UNKNOWN"]))
            .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            .agg(count("PRSN_ETHNICITY_ID").alias("count"))
            .withColumn("row_num", row_number().over(w))
            .filter(col("row_num") == 1)
            .select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            .orderBy("VEH_BODY_STYL_ID")
        )

        utils.write_csv(df, output_path)
        df.show()

    def analysis6(self, output_path):
        """
        Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the
        contributing factor to a crash (Use Driver Zip Code)
        :param output_path: output file path
        :return: list of zip codes
        """

        units_clean = self.units.dropDuplicates(["CRASH_ID", "UNIT_NBR"])
        df = (
            self.persons.join(
                units_clean,
                (units_clean["CRASH_ID"] == self.persons["CRASH_ID"])
                & (units_clean["UNIT_NBR"] == self.persons["UNIT_NBR"]),
                how="inner",
            )
            .filter(
                (
                    (units_clean["CONTRIB_FACTR_1_ID"].contains("ALCOHOL"))
                    | units_clean["CONTRIB_FACTR_2_ID"].contains("ALCOHOL")
                    | units_clean["CONTRIB_FACTR_P1_ID"].contains("ALCOHOL")
                )
                & (col("DRVR_ZIP").isNotNull())
            )
            .groupBy("DRVR_ZIP")
            .agg(count("*").alias("CRASH_COUNT"))
            .orderBy(col("CRASH_COUNT").desc())
            .limit(5)
        )

        utils.write_csv(df, output_path)
        return [row[0] for row in df.collect()]

    def analysis7(self, output_path):
        """
        Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
        and car avails Insurance
        :param output_path: output file path
        :return: Count of distinct crash IDs
        """

        units_clean = self.units.dropDuplicates(["CRASH_ID", "UNIT_NBR"])
        damages_clean = self.damages.dropDuplicates()
        df = (
            units_clean.join(
                damages_clean,
                units_clean["CRASH_ID"] == damages_clean["CRASH_ID"],
                how="inner",
            )
            .filter(
                (col("VEH_DMAG_SCL_1_ID") > "DAMAGED 4")
                & (~col("VEH_DMAG_SCL_1_ID").isin("INVALID VALUE", "NA", "NO DAMAGE"))
                | (col("VEH_DMAG_SCL_2_ID") > "DAMAGED 4")
                & (~col("VEH_DMAG_SCL_2_ID").isin("INVALID VALUE", "NA", "NO DAMAGE"))
            )
            .filter(col("FIN_RESP_TYPE_ID") != "NA")
            .filter(col("DAMAGED_PROPERTY") == "NONE")
            .select(units_clean["CRASH_ID"])
            .distinct()
        )

        utils.write_csv(df, output_path)
        return df.count()

    def analysis8(self, output_path):
        """
        Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed
        Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        offences (to be deduced from the data)
        :param output_path: output file path
        :return: list of top 5 vehicle makes
        """

        charges_clean = self.charges.dropDuplicates()
        units_clean = self.units.dropDuplicates(["CRASH_ID", "UNIT_NBR"])

        top25_states = [
            row[0]
            for row in units_clean.filter(
                (col("VEH_LIC_STATE_ID").cast("int").isNull())
                & (col("VEH_LIC_STATE_ID") != "NA")
            )
            .groupBy("VEH_LIC_STATE_ID")
            .agg(count("*").alias("count"))
            .orderBy(col("count").desc())
            .head(25)
        ]
        top10_colors = [
            row[0]
            for row in units_clean.filter(
                (col("VEH_COLOR_ID").cast("int").isNull())
                & (col("VEH_COLOR_ID") != "NA")
            )
            .groupBy("VEH_COLOR_ID")
            .agg(count("*").alias("count"))
            .orderBy(col("count").desc())
            .head(10)
        ]

        df = (
            units_clean.join(
                self.persons,
                (self.persons["CRASH_ID"] == units_clean["CRASH_ID"])
                & (self.persons["CRASH_ID"] == units_clean["CRASH_ID"]),
                how="inner",
            )
            .join(
                charges_clean,
                (self.persons["CRASH_ID"] == charges_clean["CRASH_ID"])
                & (self.persons["CRASH_ID"] == charges_clean["CRASH_ID"])
                & (self.persons["CRASH_ID"] == charges_clean["CRASH_ID"]),
            )
            .filter(col("CHARGE").contains("SPEED"))
            .filter(~col("DRVR_LIC_TYPE_ID").isin(["NA", "UNKNOWN", "UNLICENSED"]))
            .filter(col("VEH_LIC_STATE_ID").isin(top25_states))
            .filter(col("VEH_COLOR_ID").isin(top10_colors))
            .groupBy("VEH_MAKE_ID")
            .agg(count("*").alias("count"))
            .orderBy(col("count").desc())
            .limit(5)
        )

        utils.write_csv(df, output_path)
        return [row[0] for row in df.collect()]


if __name__ == "__main__":
    # Spark session
    spark = SparkSession.builder.master("local[*]").appName("CarCrash").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    config_path = "config.json"
    output_paths = utils.read_config(config_path).get("output_paths")

    cc = CarCrash(config_path)

    print("\n\n")
    print("=" * 100)

    # Analysis 1
    print(
        "Analysis1\nNumber of crashes in which number of persons killed are male?",
        cc.analysis1(output_paths.get("A1")),
        "\n",
    )

    # Analysis 2
    print(
        "Analysis2\nNumber of two wheelers that are booked for crashes-",
        cc.analysis2(output_paths.get("A2")),
    )

    # Analysis 3
    print(
        "Analysis3\nState has highest number of accidents in which females are involved-",
        cc.analysis3(output_paths.get("A3")),
        "\n",
    )

    # Analysis 4
    print(
        "Analysis4\nThe Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death-",
        cc.analysis4(output_paths.get("A4")),
        "\n",
    )

    # Analysis 5
    print(
        "Analysis5\nFor all the body styles involved in crashes, the top ethnic user group of each unique body style"
    )
    cc.analysis5(output_paths.get("A5"))

    # Analysis 6
    print(
        "\nAnalysis6\nTop 5 Zip Codes with highest number crashes with alcohols as contributing factor to a crash-",
        cc.analysis6(output_paths.get("A6")),
        "\n",
    )

    # Analysis 7
    print(
        "Analysis7\nCount of Distinct Crash IDs where No Damaged Property was observed and Damage Level "
        + "(VEH_DMAG_SCL~) is above 4 and car avails Insurance-",
        cc.analysis7(output_paths.get("A7")),
        "\n",
    )

    # Analysis 8
    print(
        "Analysis8\nTop 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed "
        + "Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number "
        + "of offences-",
        cc.analysis8(output_paths.get("A8")),
        "\n",
    )

    spark.stop()
