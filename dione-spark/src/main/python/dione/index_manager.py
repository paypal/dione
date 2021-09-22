from pyspark.sql import DataFrame


class PythonToScalaHelper(object):

    def __init__(self, spark):
        self._jvm = spark._sc._gateway.jvm

    def get_object(self, fullObjectName):
        return self._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass(fullObjectName + "$") \
            .getField("MODULE$").get(None)

    def list_to_seq(self, lst):
        scala_converter = self.get_object("scala.collection.JavaConverters")
        return scala_converter.iterableAsScalaIterableConverter(lst).asScala().toSeq()

    def to_option(self, obj):
        scala_option = self.get_object("scala.Option")
        return scala_option.apply(obj)

    def to_tuple2(self, t):
        scala_tuple2 = self.get_object("scala.Tuple2")
        return scala_tuple2.apply(t[0], t[1])


class ScalaToPythonHelper(object):
    def __init__(self, spark):
        self._jvm = spark._sc._gateway.jvm

    def get_object(self, fullObjectName):
        return self._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass(fullObjectName + "$") \
            .getField("MODULE$").get(None)

    def map_to_dict(self, mp):
        scala_converter = self.get_object("scala.collection.JavaConverters")
        return dict(scala_converter.mapAsJavaMapConverter(mp).asJava())


class IndexManager(object):

    def __init__(self, spark, im):
        self._spark = spark
        self._im = im
        self._scala_helper = PythonToScalaHelper(spark)
        self._python_helper = ScalaToPythonHelper(spark)

    @staticmethod
    def create_new(spark, data_table_name, index_table_name, keys, more_fields=None):
        scala_helper = PythonToScalaHelper(spark)
        key = scala_helper.list_to_seq(keys)
        moreFields = scala_helper.list_to_seq(more_fields)
        idx_spec = scala_helper.get_object("com.paypal.dione.spark.index.IndexSpec")
        is_java = idx_spec.apply(data_table_name, index_table_name, key, moreFields)
        im = scala_helper.get_object("com.paypal.dione.spark.index.IndexManager") \
            .createNew(is_java, spark._jsparkSession)
        return IndexManager(spark, im)

    @staticmethod
    def load(spark, index_table_name):
        scala_helper = PythonToScalaHelper(spark)
        im = scala_helper.get_object("com.paypal.dione.spark.index.IndexManager") \
            .load(index_table_name, spark._jsparkSession)
        return IndexManager(spark, im)

    def append_new_partitions(self, partitions_spec):
        partitions_spec_seq = self._scala_helper.list_to_seq(
            [self._scala_helper.list_to_seq([self._scala_helper.to_tuple2(kv) for kv in partition_spec
                                             ]) for partition_spec in partitions_spec])
        self._im.appendNewPartitions(partitions_spec_seq)

    def append_missing_partitions(self):
        self._im.appendMissingPartitions()

    def load_by_index(self, query_df, fields):
        fields_seq = self._scala_helper.list_to_seq(fields)
        fields_seq_opt = self._scala_helper.to_option(fields_seq)
        res_df = self._im.loadByIndex(query_df._jdf, fields_seq_opt)
        return DataFrame(res_df, self._spark._wrapped)

    def fetch(self, key, partition_spec, fields):
        key_seq = self._scala_helper.list_to_seq(key)
        partition_spec_tuple_seq = self._scala_helper.list_to_seq(
            [self._scala_helper.to_tuple2(kv) for kv in partition_spec])
        fields_seq_opt = self._scala_helper.to_option(self._scala_helper.list_to_seq(fields))
        res_opt = self._im.fetch(key_seq, partition_spec_tuple_seq, fields_seq_opt)
        if res_opt.nonEmpty():
            d = self._python_helper.map_to_dict(res_opt.get())
            return d
        else:
            return None
