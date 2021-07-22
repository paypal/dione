package com.paypal.dione.kvstorage.hadoop

import org.junit.jupiter.api.Assertions

object TestKeyValueStorageFile {

  def simpleTest[T](kvStorageFileWriter: KVStorageFileWriter[T, T],
                    kvStorageFileReader: KVStorageFileReader[T, T],
                    func: String => T,
                    funcB: T => String = (_:T).toString
                   ): Unit = {

    val entries = Seq("a" -> "1", "c" -> "3", "b" -> "2")
    kvStorageFileWriter.write(entries.iterator.map(e => (func(e._1), func(e._2))), kvStorageFileReader.path)

    Assertions.assertEquals("1", funcB(kvStorageFileReader.get(func("a")).get))
    Assertions.assertEquals(None, kvStorageFileReader.get(func("aa")))
    Assertions.assertEquals(3, kvStorageFileReader.getIterator().size)
  }
}
