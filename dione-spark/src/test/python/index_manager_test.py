import pytest
from dione import IndexManager


# create dummy data
@pytest.mark.usefixtures("spark_session")
def test_init_data(spark_session):

    spark = spark_session

    spark.sql("drop database if exists test_db cascade")
    spark.sql("create database test_db")
    spark.sql("use test_db")

    spark.conf.set("index.manager.btree.height", 3)
    spark.conf.set("index.manager.btree.num.parts", 10)
    spark.conf.set("index.manager.btree.interval", 20)

    local_data = []
    num_cols = 10

    for i in range(0,1000):
        local_data.append(["c"+str(i)+"_"+str(c) for c in range(0,num_cols)])

    cols = ["col"+str(c) for c in range(0,num_cols)]
    local_df = spark.createDataFrame(local_data, cols)
    spark.sql("CREATE TABLE `local_tbl_p` ("+" string,".join(cols)+" string) partitioned by (dt string) stored as parquet")
    # spark.sql("CREATE TABLE `local_tbl_p` ("+" string,".join(cols)+" string) partitioned by (dt string) stored as avro")
    local_df.createOrReplaceTempView("tmp_local")
    spark.table("tmp_local").show()

    spark.sql("""
        insert overwrite table local_tbl_p partition (dt='2021-09-14')
        select * from tmp_local
    """)

    spark.table("local_tbl_p").show()


# index owner
@pytest.mark.usefixtures("spark_session")
def test_create(spark_session):
    IndexManager.create_new(spark_session, "local_tbl_p", "local_tbl_p_idx", ["col0"], ["col1"])


@pytest.mark.usefixtures("spark_session")
def test_load(spark_session):
    IndexManager.load(spark_session, "local_tbl_p_idx")


@pytest.mark.usefixtures("spark_session")
def test_append_new_partitions(spark_session):
    im = IndexManager.load(spark_session, "local_tbl_p_idx")
    im.append_new_partitions([[('dt', '2021-09-14')]])


@pytest.mark.usefixtures("spark_session")
def test_append_missing_partitions(spark_session):
    im = IndexManager.load(spark_session, "local_tbl_p_idx")
    im.append_missing_partitions()


# index client
## Multi-Row
@pytest.mark.usefixtures("spark_session")
def test_load_by_index(spark_session):
    query_df = spark_session.table("local_tbl_p_idx").where("hash(col1) % 10 = 7")
    im = IndexManager.load(spark_session, "local_tbl_p_idx")
    im.load_by_index(query_df, ["col3", "col4"]).show()


## Single-Row
@pytest.mark.usefixtures("spark_session")
def test_fetch(spark_session):
    im = IndexManager.load(spark_session, "local_tbl_p_idx")
    r = im.fetch(["c6_0"], [("dt", "2021-09-14")], ["col7"])
    r.get().toString()
