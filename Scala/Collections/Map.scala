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
