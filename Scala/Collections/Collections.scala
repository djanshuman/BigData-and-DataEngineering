1. extracting tuple from list


scala> val first :: second :: _ =  List(1, 2, 3, 4)
first: Int = 1
second: Int = 2


scala> val first :: second :: third :: _  =  List(1, 2, 3, 4)
first: Int = 1
second: Int = 2
third: Int = 3



2. sum element of list 


scala> var sum = 0
sum: Int = 0

scala> List(1,2,3,4).foreach(num => sum += num)

scala> sum
res27: Int = 10


// sum of element of List using tailRec
  def tailSumList(aList : List[Int]) ={

    def sumList(aList: List[Int], sum: Int) : Int ={
      aList match {
        case Nil => sum
        case head :: tail => sumList(tail , sum + head)
      }

    }
    sumList(aList,0)

  }
  println(tailSumList(List(10,20,30)))
  
  
  
  
3. Remove element from List


scala> val aList = List("abc","dec","abc","gfre","dec","abc")

scala> aList.foldLeft(List.empty[String])((acc,x) => if (x != "dec") acc :+ x else acc)
res33: List[String] = List(abc, abc, gfre, abc)


scala> aList.collect{case e if e!= "dec" => e}
res32: List[String] = List(abc, abc, gfre, abc)


scala> list.filter(_ != "def")
res28: List[String] = List(abc, xyz)


4.  calculate avg of List

scala> val aList = List(10,20,30,40,50)
aList: List[Int] = List(10, 20, 30, 40, 50)

scala> aList.sum/aList.size.toDouble
res34: Double = 30.0


 def calcAvg(aList : List[Int]) : Double ={

    var sum = 0.0
    var nElements = 0.0
    for (elem <- aList) {
      nElements += 1
      sum += elem
    }
    sum/nElements
  }

  println(calcAvg(List(1, 2, 3, 4)))
  
  
  
5. dropWhile and takeWhile


scala> (1 to 20).toList.takeWhile(_ < 20)
res39: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)



scala> List(1, 2, 3, 4).takeWhile(_ < 4)
res36: List[Int] = List(1, 2, 3)

scala> List(1, 2, 3, 4).dropWhile(_ < 2)
res37: List[Int] = List(2, 3, 4)



5. zip , zipAll()

scala> val numbers = List(1, 2, 3)
numbers: List[Int] = List(1, 2, 3)

scala> val words = List("one", "two", "three")
words: List[String] = List(one, two, three)

scala> numbers.zip(words)
res40: List[(Int, String)] = List((1,one), (2,two), (3,three))

scala> numbers.zipAll(words,0,"unknown")
res42: List[(Int, String)] = List((1,one), (2,two), (3,three), (0,four))

scala> numbers.zipAll(words.take(2),0,"unknown")
res43: List[(Int, String)] = List((1,one), (2,two), (3,unknown))


6. Convert String to Int for list element

scala> val lst = List("1","abc","2")
lst: List[String] = List(1, abc, 2)


scala> lst.map(x => Try(x.toInt))
res54: List[scala.util.Try[Int]] = List(Success(1), Failure(java.lang.NumberFormatException: For input string: "abc"), Success(2))


scala> lst.map(x => Try(x.toInt)).map(_.toOption)
res58: List[Option[Int]] = List(Some(1), None, Some(2))

scala> lst.map(x => Try(x.toInt)).map(_.toOption).flatten
res59: List[Int] = List(1, 2)



scala> val lst = List("1", "2", "3")
lst: List[String] = List(1, 2, 3)

scala> lst.map(x => x.toInt)
res49: List[Int] = List(1, 2, 3)


val lst = List("1", "abc", "2")

val final_list = lst.map(x => Try(x.toInt) match{
  case Success(value) => value
  case Failure(value) => None
}).filter(x => x != None)


7.
// check if collection is sorted in scala
enum Direction:
  case ASC, DESC

def isSorted[A] (aList: List[A], direction : Direction)(using ord: Ordering[A]) : Boolean= {
direction match{
  case Direction.ASC => aList.sorted == aList
  case Direction.DESC => aList.sorted.reverse == aList
}
}
  
println(isSorted(List(5,4,2,1), Direction.DESC))
println(isSorted(List(10,20,30,40), Direction.ASC))
println(isSorted(List("abc","bca","cda"), Direction.ASC))
  

8. Get index of max element in a list

scala> val lst = List(1,2,3,2,1)
lst: List[Int] = List(1, 2, 3, 2, 1)

scala> val max = lst.max
max: Int = 3

scala> lst.indexOf(max)
res64: Int = 2

9. Print reverse list

scala> val lst = List(1,2,3,4,5)
lst: List[Int] = List(1, 2, 3, 4, 5)

scala> for (i <- 0 until lst.size) println(lst(lst.size-i-1))
5
4
3
2
1

scala> lst.reverseIterator.foreach(println)
5
4
3
2
1


10. Sliding and grouped

scala> val numbers = List(1,2,3,4)
numbers: List[Int] = List(1, 2, 3, 4)

scala> numbers.sliding(2)
res74: Iterator[List[Int]] = <iterator>

scala> numbers.sliding(2).toList
res75: List[List[Int]] = List(List(1, 2), List(2, 3), List(3, 4))

scala> numbers.sliding(2,1).toList
res76: List[List[Int]] = List(List(1, 2), List(2, 3), List(3, 4))

scala> numbers.sliding(2,2).toList
res77: List[List[Int]] = List(List(1, 2), List(3, 4))


scala> numbers.grouped(2).toList
res83: List[List[Int]] = List(List(1, 2), List(3, 4))

scala> numbers.sliding(2).toList
res84: List[List[Int]] = List(List(1, 2), List(2, 3), List(3, 4))

scala> numbers.sliding(2,2).toList
res85: List[List[Int]] = List(List(1, 2), List(3, 4))




7. reverse a list

scala> val integers = 2 :: 4 :: 6 :: 7 :: 8 :: Nil
integers: List[Int] = List(2, 4, 6, 7, 8)

scala> integers.foldLeft(List.empty[Int])((acc,x) => x +: acc)
res129: List[Int] = List(8, 7, 6, 4, 2)

scala> integers
res130: List[Int] = List(2, 4, 6, 7, 8)


scala> integers.reverse
res132: List[Int] = List(8, 7, 6, 4, 2)



8.

scala> List(1,2,3,2,1).zipWithIndex.filter(pair => pair._1 == 2)
res6: List[(Int, Int)] = List((2,1), (2,3))


scala> List(1,2,3,2,1).zipWithIndex.filter(pair => pair._1 == 2).map(pair => pair._2)
res7: List[Int] = List(1, 3)


scala> List(1,2,3,2,1).lastIndexOf(2)
res13: Int = 3


scala> List(1,2,3,2,1).indexOf(1)
res12: Int = 0



val result = aList.foldLeft(0) {
				(acc,x) => 
			val sum = acc + x
			sum
			}
			
			
9. List Concatenation

scala> val list1 = 1 :: 2 :: 3 :: Nil
list1: List[Int] = List(1, 2, 3)

scala> val list2 = 4 :: 5 :: Nil
list2: List[Int] = List(4, 5)

scala> val list3 = 6 :: 7 :: Nil
list3: List[Int] = List(6, 7)


scala> list1 ::: list2 ::: list3
res14: List[Int] = List(1, 2, 3, 4, 5, 6, 7)



10.Creating a List in Scala

scala> val convertedList:List[String] = Array("Truck", "Car", "Bike").toList
convertedList: List[String] = List(Truck, Car, Bike)

scala> val vehiclesList:List[String] = "Truck" :: "Car" :: "Bike" :: Nil
vehiclesList: List[String] = List(Truck, Car, Bike)

scala> val vehiclesList:List[String] = Nil.::("Bike").::("Car").::("Truck")
vehiclesList: List[String] = List(Truck, Car, Bike)


scala> val vehiclesList:List[String] = List.apply("Truck", "Car", "Bike")
vehiclesList: List[String] = List(Truck, Car, Bike)



scala> aList .::("1").::("2")
res21: List[Any] = List(2, 1)


scala> val aList = List.empty[Int]
aList: List[Int] = List()


11. Remove Duplicates in a Scala List

scala> val withDuplicates = List(3, 7, 2, 7, 1, 3, 4)
withDuplicates: List[Int] = List(3, 7, 2, 7, 1, 3, 4)

Using foldLeft

withDuplicates.foldLeft(List.empty[Int]) { 

(partialResult,element) => if (partialResult.contains(element)) partialResult else partialResult:+element 

}

12. Find Unique Items in a List in Scala

scala> List(1,3,2,2,1).distinct
res27: List[Int] = List(1, 3, 2)

// no ordering is preserved

scala> List(1,3,2,2,1).toSet
res28: scala.collection.immutable.Set[Int] = Set(1, 3, 2)


13.Split sequence using partition and groupBy



scala> val intSequence: Seq[Int] = Seq(1,2,3,4,5,6)
intSequence: Seq[Int] = List(1, 2, 3, 4, 5, 6)

scala> intSequence.partition(_ %2 ==0)
res29: (Seq[Int], Seq[Int]) = (List(2, 4, 6),List(1, 3, 5))

scala> intSequence.groupBy(_%2==0)
res30: scala.collection.immutable.Map[Boolean,Seq[Int]] = Map(false -> List(1, 3, 5), true -> List(2, 4, 6))

scala> 


14. Create duplicate element in scala

scala> (1 to 5).map(_ => "element")
res39: scala.collection.immutable.IndexedSeq[String] = Vector(element, element, element, element, element)

scala> (1 to 5).map(_ => "element").toList
res40: List[String] = List(element, element, element, element, element)

scala> import util.Random
import util.Random

scala> (1 to 5).map(_ => Random.nextInt)
res35: scala.collection.immutable.IndexedSeq[Int] = Vector(-1409520003, -1457056379, -144582402, -1846163824, -905962178)

scala> Seq.fill(5)(Random.nextInt)
res36: Seq[Int] = List(-1763071107, -2104213820, -488959838, 1675945315, -395395312)



15.


scala> val lst = List('a', 'b', 'b', 'c', 'a', 'b', 'd', 'e', 'e', 'e', 'a', 'a')
lst: List[Char] = List(a, b, b, c, a, b, d, e, e, e, a, a)


scala> lst.groupBy(identity).mapValues(_.size)
res52: scala.collection.immutable.Map[Char,Int] = Map(e -> 3, a -> 4, b -> 3, c -> 1, d -> 1)

scala> lst.groupBy(identity)
res49: scala.collection.immutable.Map[Char,List[Char]] = Map(e -> List(e, e, e), a -> List(a, a, a, a), b -> List(b, b, b), c -> List(c), d -> List(d))



16. Remove an Item by Index From a List

scala> val lst = List('a', 'b', 'c', 'd', 'e')
lst: List[Char] = List(a, b, c, d, e)


scala> lst.zipWithIndex
res54: List[(Char, Int)] = List((a,0), (b,1), (c,2), (d,3), (e,4))

scala> lst.zipWithIndex.filter(pair => pair._2 != 2)
res55: List[(Char, Int)] = List((a,0), (b,1), (d,3), (e,4))

scala> lst.zipWithIndex.filter(pair => pair._2 != 2).map(_._1)
res56: List[Char] = List(a, b, d, e)



17. Check if a List Is a Sublist of Another List

def usingSliding(bigList: List[String], littleList: List[String]): Boolean = {
    bigList.sliding(littleList.size, 1).exists(_ == littleList)
  }
  
def usingContainsSlice(
  bigList: List[String],
  littleList: List[String]
): Boolean = {
  bigList.containsSlice(littleList)
}


18. Finding the First Element Matching a Condition in a Collection
Using .find(predicate)



19.Find the Most Frequent Element in a Scala Collection


scala> val list = List(1,2,2,3,4,5,6)
list: List[Int] = List(1, 2, 2, 3, 4, 5, 6)

scala> list.groupBy(identity)
res95: scala.collection.immutable.Map[Int,List[Int]] = Map(5 -> List(5), 1 -> List(1), 6 -> List(6), 2 -> List(2, 2), 3 -> List(3), 4 -> List(4))

scala> list.groupBy(identity).maxBy(_._2.size)
res96: (Int, List[Int]) = (2,List(2, 2))


scala> list.groupBy(identity).maxBy(_._2.size)._1
res103: Int = 2


20. Different Ways to Reverse a Sequence in Scala



scala> val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
list: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

scala> list.foldLeft(List.empty[Int]) {(sequence,element) => element +: sequence}
res104: List[Int] = List(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)


scala> list.reverse
res105: List[Int] = List(10, 9, 8, 7, 6, 5, 4, 3, 2, 1)


21. Create a Random Sample of Fixed Size From a Scala List

scala> list
res106: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

scala> list.map (elem => (Random.nextInt(),elem)).sortBy(_._1).map(_._2).take(5)
res115: List[Int] = List(1, 8, 4, 3, 6)


22. Different ways to filter collection

using .find(predicate), .filter(predicate) , .collect{} # this is partial function

scala> list.collect{case n if n % 2 ==0 =>  n}
res126: List[Int] = List(2, 4, 6, 8, 10)



23.Get the Index of Element With Max Value in a Scala List

scala> val lst = List(1,2,3,2,1)
lst: List[Int] = List(1, 2, 3, 2, 1)

scala> lst.zipWithIndex.maxBy(_._1)._2
res136: Int = 2


24. Convert a List of String to List of Int in Scala

scala> val lst = List("1", "abc", "2")
lst: List[String] = List(1, abc, 2)


scala> lst.map(x => Try(x.toInt)).map(_.toOption).flatten
res143: List[Int] = List(1, 2)

scala> lst.map(x => Try(x.toInt)).flatMap(_.toOption)
res145: List[Int] = List(1, 2)


25. trait implicit to find max of list or string , generic type

//trait implicit to find max of list or string , generic type

  trait MyOrdering[A] {
    def compare(x:A,y:A) : Int

  }
  implicit object IntOrdering extends MyOrdering[Int] {
    override def compare(x: Int, y: Int): Int = x - y
  }

  implicit object StringOrdering extends MyOrdering[String] {
    override def compare(x: String, y: String): Int = x.compareTo(y)
  }

  def findMax[A] (aList : List[A]) (implicit ord : MyOrdering[A] ): A ={

    aList.reduceLeft( (x,y) => if (ord.compare(x,y) > 0) x else y)
  }
 
26. Find difference between two list in scala


scala> val str1 = "I love scala coding"
str1: String = I love scala coding

scala> val str2 = "I love python coding"
str2: String = I love python coding


scala> val list1 = str1.split(" ")
list1: Array[String] = Array(I, love, scala, coding)

scala> val list2 = str2.split(" ")
list2: Array[String] = Array(I, love, python, coding)


-- Approach 1 -- [ diff and merging both arrays]

scala> list1.diff(list2) ++ list2.diff(list1)
res2: Array[String] = Array(scala, python)


-- Approach 2 -- [filter + contains and merging both array]

scala> list2.filter(x => !list1.contains(x)) ++ list1.filter(x => !list2.contains(x)).toSeq
res31: Array[String] = Array(python, scala)


-- Approach 3-- [filterNot + contains and merging both array]

scala> list1.filterNot(list2.contains) ++ list2.filterNot(list1.contains)
res41: Array[String] = Array(scala, python)


  
