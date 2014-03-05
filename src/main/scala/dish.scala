import java.io.File
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.nio.MappedByteBuffer
import scala.collection.mutable.ArrayBuffer

class Dish(path: String, bucketSize: Int = 8, capacity: Long = 1024 * 1024, maxStringSize: Int = 24) {
  var sz: Long = 0
  // record size in bytes
  val recLength: Int = maxStringSize + 8
  val bucketsPerFile: Int = (Integer.MAX_VALUE - (4096 * 1024)) / (bucketSize * recLength)
  println("buckets per file: " + bucketsPerFile)
  val numFiles: Int = (capacity / bucketsPerFile).toInt + 1
  val mbbs: Array[MappedByteBuffer] = {
    var t = ArrayBuffer[MappedByteBuffer]()
    var i = 0
    while (i < numFiles) {
      t += null
      i += 1
    }
    i = 0
    while (i < numFiles) {
      t(i) = initFile(i)
      i += 1
    }
    t.toArray
  }

  def initFile(idx: Int) = {
    val file = new File(path + "." + idx)
    val fileHandle = new RandomAccessFile(file, "rw")
    val fileSize = if (capacity < bucketsPerFile) {
      capacity * recLength * bucketSize
    } else {
      bucketsPerFile * recLength * bucketSize
    }
    println("initializing (zeroing) filesize: " + fileSize)
    val start = System.currentTimeMillis
    //val buffer = Array.fill[Byte](recLength * bucketSize)(0)
    val buffer = Array.fill[Byte](4096 * 1024)(0)
    if (file.exists()) {
      // TODO refactor this section so we can persist stuff
      //file.delete()
      var pos: Long = 0
      fileHandle.seek(0)
      while (pos < fileSize) {
        fileHandle.write(buffer)
        pos += buffer.length
      }
    } else {
      //file.delete()
      var pos: Long = 0
      fileHandle.seek(0)
      while (pos < fileSize) {
        fileHandle.write(buffer)
        pos += buffer.length
      }
    }
    println("done zeroing filesize for " + file.getName + ", elapsed: " + (System.currentTimeMillis - start) / 1000.0 + "s")

    val fileChannel = fileHandle.getChannel()
    fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize)
  }

  def size = {
    sz
  }

  def hash(str: String) = {
    var h = 1125899906842597L // prime
    val len = str.length

    var i = 0
    while (i < len) {
      h = 31 * h + str.charAt(i)
      i += 1
    }
    if (h < 0) {
      -h
    } else {
      h
    }
  }

  def put(k: String, v: Long) = {
    val h = hash(k)
    val idx: Long = h % capacity
    val file: Int = (idx / bucketsPerFile).toInt
    //println("putting into file: " + file)
    val bucket: Int = (idx - (file.toLong * bucketsPerFile.toLong)).toInt
    //println("putting into bucket: "+bucket)
    var i = 0
    while (i < bucketSize) {
      mbbs(file).position((bucket * bucketSize * recLength) + (i * recLength) + maxStringSize)
      val test = mbbs(file).getLong()
      if (test == 0) {
        mbbs(file).position(bucket * bucketSize * recLength + i * recLength)
        //println("putting key into position: "+mbbs(file).position()+"; i: "+i)
        mbbs(file).put(k.getBytes, 0, k.getBytes.length)
        mbbs(file).position((bucket * bucketSize * recLength) + (i * recLength) + maxStringSize)
        //println("putting long into position: "+mbbs(file).position()+"; i: "+i)
        mbbs(file).putLong(v)
        // break out of while early
        i = bucketSize
      }
      i += 1
    }
    if (i == bucketSize) {
      throw new Exception("bucket overflow!")
    }
    sz += 1
  }

  var keyBuff = new Array[Byte](maxStringSize)

  def getOrElse(k: String, i: Long): Long = {
    val v = get(k)
    if (v == 0) {
    return i
  }
    return v
  }

  def get(k: String): Long = {
    val h = hash(k)
    val idx: Long = h % capacity
    val file: Int = (idx / bucketsPerFile).toInt
    //println("getting from file: " + file)
    val bucket: Int = (idx - (file.toLong * bucketsPerFile.toLong)).toInt
    var i = 0
    //println("getting from bucket: "+bucket)
    while (i < bucketSize) {
      //println("inspecting bucket[i]: "+i)
      mbbs(file).position(bucket * bucketSize * recLength + i *
        recLength + maxStringSize)
      //println("test position: "+mbbs(file).position())
      val test = mbbs(file).getLong()
      if (test != 0) {
        mbbs(file).position(bucket * bucketSize * recLength + i * recLength)
        //println("getting from position: "+mbbs(file).position()+"; i: "+i)
        mbbs(file).get(keyBuff, 0, maxStringSize)
        if (compareKey(k.getBytes, keyBuff)) {
          return mbbs(file).getLong()
        }
      } else {
        // break out of while early
        i = bucketSize
      }
      i += 1
    }
    return 0
  }

  // flushes hash to disk
  def flush = {
    mbbs.foreach(_.force)
  }

  // only compares the first k.length elements (buf can be larger than k)
  def compareKey(k: Array[Byte], buf: Array[Byte]): Boolean = {
    var i = 0
    while (i < k.length) {
      if (k(i) != buf(i)) return false
      i += 1
    }
    true
  }
}