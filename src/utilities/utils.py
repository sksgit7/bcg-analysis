import json

def read_config(path):
    """
    Read config.json file
    :param path: file path to config.json
    :return: config dictionary
    """

    with open(path, "r") as f:
        return json.load(f, strict=False)


def load_df(spark, path):
    """
    Read data from csv files and make dataframe
    :param spark:
    :param path:
    :return:
    """

    return spark.read.options(header=True, inferSchema=True).csv(path)


def write_csv(df, path):
    """
    Write output to csv file
    :param df:
    :param path:
    :return:
    """

    df.coalesce(1).write.mode('overwrite').options(header=True).csv(path)
