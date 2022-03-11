package com.paypal.dione.kvstorage.hadoop

/**
 * Top level APIs for the K-V storage hierarchy.
 *
 * KVStorageFileWriter
 * -------------------
 * write       - given an iterator of types (K,V) and a desired Hadoop location `path`, persists the
 *               `entries` to this location
 *
 * KVStorageFileReader
 * -------------------
 * get         - given the above `path` and a specific key K, quickly gets the persisted value V
 * getIterator - reads all (K,V) pairs from `path`
 */


abstract class KVStorageFileWriter[K, V] {

  def write(entries: Iterator[(K, V)], path: String)

}

abstract class KVStorageFileReader[K, V](val path: String) {

  def getIterator(key: K): Iterator[V]

  def get(key: K): Option[V] = {
    val it = getIterator(key)
    if (it.hasNext)
      Some(it.next())
    else None
  }

  def getIterator(): Iterator[(K, V)]

}
