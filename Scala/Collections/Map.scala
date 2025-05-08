


1.For Comprehension for chaining Map and flatMap for concise code.

for {
 a <- supportedOs  //flatMap
 b <- input			//flatMap
 if a == b		  	// Map
 }yield "Success"	//generator
 
 
 
2. Generate Frequency Map of Strings in Scala
 
 https://www.baeldung.com/scala/strings-frequency-map
 
scala> "hello".groupBy(x => x).mapValues(_.length)
res20: scala.collection.immutable.Map[Char,Int] = Map(h -> 1, e -> 1, l -> 2, o -> 1)

scala> "hello".groupBy(identity).mapValues(_.length)
res21: scala.collection.immutable.Map[Char,Int] = Map(h -> 1, e -> 1, l -> 2, o -> 1)


3. Filter “None” Values From a Map
https://www.baeldung.com/scala/filter-none-from-map

val xmen : Map[Rank, Option[String]] = Map(
	 1 -> Some("ProfessorX"),
     2 -> Some("Wolverine"),
     3 -> None,
     4 -> Some("Night Crawler"),
     5 -> None
)


val forMap = for {
(k,v) <- xmen
if v != None
} yield (k,v)


scala> val filterMap = xmen.filter(_._2 != None)
filterMap: scala.collection.mutable.Map[Rank,Option[String]] = Map(2 -> Some(Wolverine), 4 -> Some(Night Crawler), 1 -> Some(ProfessorX))

scala> val filterMap = xmen.filterNot(_._2 == None)
filterMap: scala.collection.mutable.Map[Rank,Option[String]] = Map(2 -> Some(Wolverine), 4 -> Some(Night Crawler), 1 -> Some(ProfessorX))

scala> val filterMap = xmen.collect{ case (k,Some(v)) => (k,Some(v))}
filterMap: scala.collection.mutable.Map[Rank,Some[String]] = Map(2 -> Some(Wolverine), 4 -> Some(Night Crawler), 1 -> Some(ProfessorX))


Create mutable Map

val xmenMutable: mutable.Map[Rank,Option[String]] = mutable.Map(
      (1,Some("ProfessorX")),
      (2,Some("Wolverine")),
      (3,None),
      (4,Some("Night Crawler")),
      (5,None))

scala> xmenMutable.filterInPlace((_,v) => v != None)
val res4: xmenMutable.type = HashMap(1 -> Some(ProfessorX), 2 -> Some(Wolverine), 4 -> Some(Night Crawler))


4.Querying Mappings in Scala Maps
https://www.baeldung.com/scala/querying-maps

	4.1. Check if a Given Key Exists
	
		Important : isDefinedAt() or contains() methods:
	
		scala> val m = Map(1 -> "a", 2 -> "b", 3 -> "c")
		m: scala.collection.mutable.Map[Int,String] = Map(2 -> b, 1 -> a, 3 -> c)
		
		scala> m.isDefinedAt(1)
		res32: Boolean = true
		
		scala> m.isDefinedAt(100)
		res33: Boolean = false
		
		scala> m.contains('a')
		res34: Boolean = false
		
		scala> m.contains(3)
		res35: Boolean = true
		
		scala> m.keys.exists(_ == 100)
		res66: Boolean = false
		
		scala> m.keys.exists(_ == 1)
		res67: Boolean = true
		
	4.2. Check if a Given Value Exists

		scala> m
		res69: scala.collection.mutable.Map[Int,String] = Map(2 -> b, 1 -> a, 3 -> c)
		
		scala> m.values.exists(_ == "a")
		res70: Boolean = true
		
		
		scala> m.exists(x => x._2 == "a")
		res73: Boolean = true
		
		scala> m.exists(_._2 == "z")
		res75: Boolean = false
		
		scala> m.exists{ case (k,v) => v == "a"}
		res78: Boolean = true
		
		scala> m.exists{ case (k,v) => v == "p"}
		res79: Boolean = false

	4.3. Check if a Given (key, value) Pair Exists
		
		scala> m.exists(pair => pair == (2,"b"))
		res83: Boolean = true

		scala> m.exists{ case(k,v) => k==2 && v == "b"}
		res80: Boolean = true
		
		scala> m.exists{case(key,value) => (key,value) == (2,"b") }
		res85: Boolean = true

		scala> m.exists{case(key,value) => (key,value) == (2,"z") }
		res86: Boolean = false
		
5. Using foreach() Method in Scala

https://www.baeldung.com/scala/foreach-collections

scala> List(10,20,30).foreach(elem => println(elem))
10
20
30

scala> List(10,20,30).foreach(println(_))
10
20
30

scala> List(10,20,30).foreach(println(_))
10
20
30

scala> Set(1,2,3).foreach(println)
1
2
3

scala> Array(1,2,3).foreach(println)
1
2
3

scala> Seq(1,2,3).foreach(println)
1
2
3

scala> Vector(1,2,3).foreach(println)
1
2
3

scala> Map(1 -> "a", 2 -> "b", 3 -> "c").foreach(println)
(2,b)
(1,a)
(3,c)


scala> Map(1 -> "a", 2 -> "b", 3 -> "c").foreach{ case(k,v) => println(s"The value for key $k is $v")}
The value for key 2 is b
The value for key 1 is a
The value for key 3 is c


scala> Map(1 -> "a", 2 -> "b", 3 -> "c").foreach(elem => println(s"The Value of key ${elem._1} is ${elem._2}"))
The Value of key 2 is b
The Value of key 1 is a
The Value of key 3 is c


6. Shuffle Collections in Scala
https://www.baeldung.com/scala/shuffle-list-map-set

-- List
scala> import scala.util.Random
import scala.util.Random

scala> val aList : List[Int] = List(10,2,4,5)
aList: List[Int] = List(10, 2, 4, 5)

scala> Random.shuffle(aList)
res103: List[Int] = List(2, 10, 5, 4)

scala> Random.shuffle(aList)
res104: List[Int] = List(2, 10, 4, 5)

scala> Random.shuffle(aList)
res105: List[Int] = List(4, 5, 2, 10)


-- Seq

scala> val bSeq : Seq[Int] = Seq(90,80,65,60)
bSeq: Seq[Int] = List(90, 80, 65, 60)

scala> Random.shuffle(bSeq)
res107: Seq[Int] = List(60, 80, 65, 90)

scala> Random.shuffle(bSeq)
res108: Seq[Int] = List(90, 60, 80, 65)

scala> Random.shuffle(bSeq)
res109: Seq[Int] = List(80, 90, 65, 60)

scala> Random.shuffle(bSeq)
res110: Seq[Int] = List(80, 65, 90, 60)


-- Map 

scala> val aMap : Map[Char,Int] = Map('A' -> 10 , 'B' -> 20 , 'C' -> 100)
aMap: scala.collection.mutable.Map[Char,Int] = Map(A -> 10, C -> 100, B -> 20)

scala> Random.shuffle(aMap)
res111: scala.collection.mutable.Iterable[(Char, Int)] = ArrayBuffer((B,20), (A,10), (C,100))

scala> Random.shuffle(aMap)
res114: scala.collection.mutable.Iterable[(Char, Int)] = ArrayBuffer((A,10), (C,100), (B,20))

scala> Random.shuffle(aMap)
res115: scala.collection.mutable.Iterable[(Char, Int)] = ArrayBuffer((C,100), (A,10), (B,20))


-- Set 

scala> val x = Set(80,55,77,33)
x: scala.collection.immutable.Set[Int] = Set(80, 55, 77, 33)

scala> Random.shuffle(x)
res137: scala.collection.immutable.Set[Int] = Set(77, 33, 55, 80)

scala> Random.shuffle(x)
res138: scala.collection.immutable.Set[Int] = Set(55, 33, 80, 77)

scala> Random.shuffle(x)
res139: scala.collection.immutable.Set[Int] = Set(77, 55, 80, 33)

7. Merge Two Maps in Scala

https://www.baeldung.com/scala/merge-two-maps

scala> val firstMap = Map(1 -> "Apple", 5 -> "Banana", 3 -> "Orange");
firstMap: scala.collection.mutable.Map[Int,String] = Map(5 -> Banana, 1 -> Apple, 3 -> Orange)

scala> val secondMap = Map(5 -> "Pear", 4 -> "Cherry");
secondMap: scala.collection.mutable.Map[Int,String] = Map(5 -> Pear, 4 -> Cherry)

scala> firstMap ++ secondMap
res149: scala.collection.mutable.Map[Int,String] = Map(5 -> Pear, 4 -> Cherry, 1 -> Apple, 3 -> Orange)

If both collection has same key then ++ will only preserve values from second collection/Map


-- Use case 1 where values are not iterables --

val duplicateMap = for {
(k1,v1) <- firstMap
(k2,v2) <- secondMap
if(k1 == k2)
} yield (k1,(v1,v2))


scala> val duplicateMap = for {
     | (k1,v1) <- firstMap
     | (k2,v2) <- secondMap
     | if(k1 == k2)
     | } yield (k1,(v1,v2))
duplicateMap: scala.collection.mutable.Map[Int,(String, String)] = Map(5 -> (Banana,Pear))

scala> duplicateMap
res164: scala.collection.mutable.Map[Int,(String, String)] = Map(5 -> (Banana,Pear))


scala> val combinedMap = firstMap ++ secondMap ++ duplicateMap
combinedMap: scala.collection.mutable.Map[Int,java.io.Serializable] = Map(5 -> (Banana,Pear), 4 -> Cherry, 1 -> Apple, 3 -> Orange)

-- Use case 2 where values are iterables --

#Step 1: Define own helper function 

def combineIterables[K,V](a: Map[K,Iterable[V]] , b: Map[K,Iterable[V]]) : Map [K, Iterable[V]] = {
	a ++ b.map{
	case (k,v) => k -> (v ++ a.getOrElse(k,Iterable.empty))}
}


#Step 2 : Declare variable and call the function with collections variables

scala> val firstMap = Map(1 -> List(1,2,3))
firstMap: scala.collection.immutable.Map[Int,List[Int]] = Map(1 -> List(1, 2, 3))

scala> val secondMap = Map(1 -> List(4,5))
secondMap: scala.collection.immutable.Map[Int,List[Int]] = Map(1 -> List(4, 5))


scala> def combineIterables[K,V](a: Map[K,Iterable[V]] , b: Map[K,Iterable[V]]) : Map [K, Iterable[V]] = {
     |   a ++ b.map{
     |   case (k,v) => k -> (v ++ a.getOrElse(k,Iterable.empty))}
     | }
combineIterables: [K, V](a: Map[K,Iterable[V]], b: Map[K,Iterable[V]])Map[K,Iterable[V]]


scala> val mergedMap = combineIterables[Int, Int](firstMap, secondMap)
mergedMap: Map[Int,Iterable[Int]] = Map(1 -> List(4, 5, 1, 2, 3))


def combinator[K,V](a : Map[K,Iterable[V]] , b: Map[K,Iterable[V]]) : Map[K,Iterable[V]] ={
	a ++ b.map {
	case(k,v) => k -> (v ++ a.getOrElse(k,Iterable.empty))
	}
}



-- Alternate approach using for comprehension 

val duplicateMap = for {
(k1,v1) <- Map1
(k2,v2) <- Map2
if(k1 == k2)
} yield Map1 ++ Map2.map {
	case(k,v) => k -> (v ++ Map1.getOrElse(k,Iterable.empty))
	}


scala> val duplicateMap = for {
     | (k1,v1) <- firstMap
     | (k2,v2) <- secondMap
     | if(k1 == k2)
     | } yield firstMap ++ secondMap.map {
     |   case(k,v) => k -> (v ++ firstMap.getOrElse(k,Iterable.empty))
     |   }
duplicateMap: scala.collection.immutable.Iterable[scala.collection.immutable.Map[Int,List[Int]]] = List(Map(1 -> List(4, 5, 1, 2, 3)))



scala> val result = duplicateMap.head
result: scala.collection.immutable.Map[Int,List[Int]] = Map(1 -> List(4, 5, 1, 2, 3))


-- More example 

scala> val Map1 = Map(400 -> List(90,60,20), 500 -> List(30,40,80))
Map1: scala.collection.immutable.Map[Int,List[Int]] = Map(400 -> List(90, 60, 20), 500 -> List(30, 40, 80))

scala> val Map2 = Map(400 -> List(11,22,33),55 -> List(333,444,555))
Map2: scala.collection.immutable.Map[Int,List[Int]] = Map(400 -> List(11, 22, 33), 55 -> List(333, 444, 555))


scala> val duplicateMap = for {
     | (k1,v1) <- Map1
     | (k2,v2) <- Map2
     | if(k1 == k2)
     | } yield Map1 ++ Map2.map {
     |   case(k,v) => k -> (v ++ Map1.getOrElse(k,Iterable.empty))
     |   }
duplicateMap: scala.collection.immutable.Iterable[scala.collection.immutable.Map[Int,List[Int]]] = List(Map(400 -> List(11, 22, 33, 90, 60, 20), 500 -> List(30, 40, 80), 55 -> List(333, 444, 555)))

scala> 

scala> val result = duplicateMap.head
result: scala.collection.immutable.Map[Int,List[Int]] = Map(400 -> List(11, 22, 33, 90, 60, 20), 500 -> List(30, 40, 80), 55 -> List(333, 444, 555))

8. Find the Element With Max Value in a Map in Scala

https://www.baeldung.com/scala/max-value-scala-map


	8.1 Maps With Multiple Max Values


		scala> val m = Map("a" -> 1, "b" -> 2, "c" -> 3, "d"->3)
		m: scala.collection.immutable.Map[String,Int] = Map(a -> 1, b -> 2, c -> 3, d -> 3)
		
		Step 1: find the maxvalue from the map
		
		scala> val maxValue = m.maxBy(_._2)
		maxValue: (String, Int) = (c,3)
		
		Step 2 : Apply Filter on original Map and retain only those pairs
		
		scala> m.filter(pair => pair._2 == maxValue._2)
		res34: scala.collection.immutable.Map[String,Int] = Map(c -> 3, d -> 3)


	8.2 Simple data type

		scala> val m = Map("a" -> 1, "b" -> 2, "c" -> 3)
		m: scala.collection.immutable.Map[String,Int] = Map(a -> 1, b -> 2, c -> 3)
		
		
		scala> m.max
		res1: (String, Int) = (c,3)
		
		scala> m.maxBy(_._2)
		res4: (String, Int) = (c,3)






9. Fold Left Map and List

scala> List(1 -> "first", 2 -> "second").foldLeft(Map(3 -> "Hello" , 4 -> "Johny")){ case(x,(k,v)) => x + (k -> v) }
res89: scala.collection.immutable.Map[Int,String] = Map(3 -> Hello, 4 -> Johny, 1 -> first, 2 -> second)


scala> List(1 -> "first", 2 -> "second").foldLeft(Map.empty[Int,String]){ case(x,(k,v)) => x + (k -> v) }
res79: scala.collection.immutable.Map[Int,String] = Map(1 -> first, 2 -> second)


scala> val aList = List(1,2,3)
aList: List[Int] = List(1, 2, 3)

scala> aList.foldLeft(0)(_+_)
res73: Int = 6




10. Maps


-- Sample code to illustrate Map

object test extends App{

  val initialMap : Map[Int,String] = Map(1 -> "first", 2 -> "second")

  def abbreviate : (Int,String) => (Int,String) ={
    case(key,value) =>
      val newValue = key + value.takeRight(2)
      key -> newValue
  }
  
  println(initialMap.map( x => abbreviate(x._1,x._2)))

}


-Few more syntactic sugar --

scala> val newMap : Map[Int,Int] = m map {case(_,v) => (1,v.length)}
newMap: Map[Int,Int] = Map(1 -> 2)

scala> val newMap : Map[String,Int] = m map { case(k,v) => (k.toString,v.length)}
newMap: Map[String,Int] = Map(1 -> 1, 2 -> 2)




11. MapValues

val initialMap : Map[Int,String] = Map(1 -> "first", 2 -> "second")


scala> val reverse: String => String = value => value.reverse
reverse: String => String = $Lambda$2560/0x0000000800f9d040@268c650d


val reversed = initialMap.mapValues(reverse).view.force
println(reversed)
Vector((1,tsrif), (2,dnoces))

scala> val reversed: Map[Int, String] = initialMap.mapValues(reverse)
reversed: Map[Int,String] = Map(1 -> tsrif, 2 -> dnoces)
  
--Use case of mapView and force after mapValues to call MapValues on all values

val initialMap : Map[Int,String] = Map(1 -> "first", 2 -> "second")


val counter = new AtomicInteger()
  val reverse : String => String = { x =>
    counter.incrementAndGet()
    x.reverse
  }

val reversed = initialMap.mapValues(reverse).view.force
println(reversed)
println(counter.get())

Vector((1,tsrif), (2,dnoces))
2


12. filter 

val predicate : ((Int,String)) => Boolean ={
    case(key,value) => key > 1 && value.length > 5
  }

val filtered : Map[Int,String] = initialMap.filter(predicate)
println(filtered)

Map(2 -> second)


13. filterKeys

val predicateKey : Int => Boolean = key => key > 1
val result : Map[Int,String] = initialMap.view.filterKeys(predicateKey).toMap
println(result)

Vector((2,second))

14. flatMap

  val m = Map(1 -> "A", 2 -> "B", 3 -> "C")
  val newMap : Map[Int,String] = m flatMap  {
    case(k,v) => (1 to k).map(i => i -> s"$i$v")
  }
  println(newMap)
  
  
15. transform 

scala> val newMap : Map[Int,String] = m transform{case(k,v) => s"$k$v"}
newMap: Map[Int,String] = Map(1 -> 1A, 2 -> 2B, 3 -> 3C)

mapValues can access valus of Map ,transform can access both keys and valus


