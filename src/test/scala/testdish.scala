import java.util.UUID
import org.scalatest._

class testdish extends FlatSpec with Matchers {
  "A dish" should "put/get values" in {
    println("STARTING put/get test")
    val dish = new Dish("/Users/wfreeman/dish/testfile.dish", 2, 10, 9)
    val k = "asdfasdf"
    val v = 1234123
    dish.put(k, v)
    dish.get(k) should be (v)
  }

  it should "increment the size" in {
    println("STARTING size test")
    val dish = new Dish("/Users/wfreeman/dish/testfile.dish", 2, 10, 5)
    dish.size should be (0)
    dish.put("asdf", 1)
    dish.size should be (1)
  }

  it should "allow adding a lot of stuff" in {
    val N = 1000000000
    var start = System.currentTimeMillis
    println("STARTING a lot of adding; N = "+N)
    val dish = new Dish("/Volumes/extssd/testfile.dish", 16, 1024*1024*1024, 24)
    println("done initiating empty map, elapsed: "+(System.currentTimeMillis-start)/1000.0+"s")
    start = System.currentTimeMillis
    var i = 0
    val r = new java.util.Random()
    start = System.currentTimeMillis
    while(i < N){
      val k = r.nextLong().toString
      val v = r.nextLong()
      if(dish.getOrElse(k, -1) == -1) {
        dish.put(k, v)
        i += 1
      }
    }
    println("done generating randoms/adding to dish, elapsed: "+(System.currentTimeMillis-start)/1000.0+"s")
  }

  it should "allow adding a lot of stuff, and validating with a hashmap" in {
    val N = 100000
    var start = System.currentTimeMillis
    println("STARTING a lot of adding; N = "+N)
    val dish = new Dish("/Volumes/extssd/testfile.dish", 8, 1024*1024, 24)
    println("done initiating empty map, elapsed: "+(System.currentTimeMillis-start)/1000.0+"s")
    start = System.currentTimeMillis
    val hm = new collection.mutable.HashMap[String, Long]
    var i = 0
    val r = new java.util.Random()
    start = System.currentTimeMillis
    while(i < N){
      val k = r.nextLong().toString
      val v = r.nextLong()
      if(hm.getOrElse(k, -1) == -1) {
        hm.put(k, v)
        i += 1
      }
    }
    println("done generating randoms/adding to HashMap, elapsed: "+(System.currentTimeMillis-start)/1000.0+"s")
    start = System.currentTimeMillis
    for((k, v) <- hm) {
      dish.put(k, v)
    }
    println("done adding to dish, elapsed: "+(System.currentTimeMillis-start)/1000.0+"s")
    start = System.currentTimeMillis
    for((k, v) <- hm) {
      val v2 = dish.get(k)
      v2 should be (v)
    }
    println("done validating randoms, elapsed: "+(System.currentTimeMillis-start)/1000.0+"s")
    start = System.currentTimeMillis
    dish.flush
    println("done flushing, elapsed: "+(System.currentTimeMillis-start)/1000.0+"s")
  }
}