import os
from pyspark import SparkContext, SparkConf


conf = SparkConf().setAppName("data preparation")
sc = SparkContext(conf=conf)

data_dir = "data"
rows = []

for filename in os.listdir(data_dir):
    if not filename.endswith(".txt"):
        continue
    name = filename[:-4]  # remove .txt
    # Split on first underscore: id_title
    parts = name.split("_", 1)
    if len(parts) < 2:
        continue
    doc_id = parts[0]
    title = parts[1].replace("_", " ")

    filepath = os.path.join(data_dir, filename)
    with open(filepath, "r") as f:
        text = f.read().replace("\t", " ").replace("\n", " ").strip()

    if text:
        rows.append(f"{doc_id}\t{title}\t{text}")

print(f"Prepared {len(rows)} documents")

# Save tab-separated format to HDFS /input/data for MapReduce
rdd = sc.parallelize(rows)
rdd.saveAsTextFile("/input/data")

sc.stop()
