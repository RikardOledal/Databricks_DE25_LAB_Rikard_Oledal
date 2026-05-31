from pyspark import pipelines as dp

BASE_DIR = "/Volumes/marathos/default/raw"

schema = spark.read.format("csv").options(header=True, inferSchema=True).load(f"{BASE_DIR}/countries/code_countries.csv").schema

@dp.table(
    name="marathos.bronze.dim_countries",
    comment="Countries codes and names",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def raw_supply_chain():
    return spark.readStream.format("csv").options(header=True, encoding="UTF-8").schema(schema).load(f"{BASE_DIR}/countries")