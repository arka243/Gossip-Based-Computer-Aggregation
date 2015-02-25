import scala.math._
import scala.collection.mutable.ArrayBuffer
import akka.actor._
import scala.util.Random
import scala.concurrent.duration._

case object START
case object Gossip
case object Remove
case object Remind 
case object End
case object ShutDown
case object NoNeighborFound
case object RemindSelf 

case class AddNum(random: ActorRef)
case class PushSum(s: Double, w: Double)
case class SetValue(s: Double) 
case class SetNeighbor(nbrall: ArrayBuffer[ActorRef])
case class SetTopology(topology: String) 


////////////// GOSSIP ACTOR CLASS ////////////////

class GossipActor(topology: String) extends Actor 
{
  import context._

  var neighbor = new ArrayBuffer[ActorRef]()
  var gspcount, pshcount= 0
  var boss: ActorRef = null
  var done, finished, reached = false
  var s_value, w_value: Double = 1
  var lastvalue, currvalue: Double = 0
  
  def receive = 
  {
  	case SetNeighbor(nbrall) =>
      neighbor ++= nbrall
      boss = sender

    case SetValue(s) =>
      s_value = s
      lastvalue = s_value / w_value

    case AddNum(random) =>
      neighbor += random

    case ShutDown =>
      context.stop(self)
  
    case PushSum(s, w) =>
      if (!done) 
      {
        s_value = (s + s_value)/2
        w_value = (w + w_value)/2
        currvalue = s_value / w_value
        if (abs(currvalue - lastvalue) <= pow(10, -10) && w != 0) 
        {
          pshcount = pshcount + 1
        } 
        else 
        {
          pshcount = 0
        }
        lastvalue = currvalue
        if (pshcount < 3 && neighbor.length > 0) 
        {
          var p = Random.nextInt(neighbor.length)
          neighbor(p) ! PushSum(s_value, w_value)
          context.system.scheduler.scheduleOnce(1000 milliseconds, self, PushSum(s_value, w_value))
        } 
        else 
        {
          done = true
          for (ac: ActorRef <- neighbor)
            ac ! Remove
        }
      }

    case Gossip =>
      if (!done) 
      {
        if (!reached) 
        {
          if (!finished) 
          {
            boss ! End
            finished = true
          }
          reached = true
          self ! Remind
        }
        gspcount = gspcount + 1
        if (gspcount > 10 || neighbor.length < 0)
        {
          done = true
          for (ac: ActorRef <- neighbor)
            ac ! Remove
        }
      }
      
    case Remind =>
      if (gspcount < 10 && neighbor.length > 0 && !done) 
      {
        var p = Random.nextInt(neighbor.length)
        neighbor(p) ! Gossip
        context.system.scheduler.scheduleOnce(800 milliseconds, self, Remind)
      }

    case Remove =>
      if (!done) 
      {
        neighbor = neighbor - sender
        if (neighbor.length <= 0) 
        {
          done = true
          if (!finished) 
          {
            println("SHUTDOWN Node "+self)
            boss ! NoNeighborFound
            boss ! End
            finished = true
          }
        }
      }
  }
}


///////////////// MASTER CLASS //////////////////

class Master(numNodes: Int, topology: String, algorithm: String) extends Actor 
{

  val e = ceil(sqrt(numNodes)).toInt
  
  if (numNodes == 0) 
  {
    context.stop(self)
    context.system.shutdown()
  }
  var a, t = new ArrayBuffer[ActorRef]()
  var noNbrCount,finish = 0
  var timetaken: Long = 0
  var n = 0
  if (topology == "2d" || topology == "imperfect2d") 
  {
    n = pow(e, 2).toInt 
  } 
  else
  { 
    n = numNodes
  }
  
  for (i <- 0 until n) 
  {
    a += context.actorOf(Props(new GossipActor(topology)), name = "a" + i) 
  }

  if (algorithm == "push-sum") 
  {
    for (i <- 0 until n) 
    {
      a(i) ! SetValue(i.toDouble) 
    }
  }

  topology.toLowerCase() match 
  {
    case "full" =>     /////////// FULL NETWORK TOPOLOGY /////////////
      for (i <- 0 until n) 
      {
        a(i) ! SetNeighbor(a - a(i)) 
        t = new ArrayBuffer[ActorRef]()
      }
      
    case "line" =>	   ////////// LINE TOPOLOGY ////////////

      a(0) ! SetNeighbor(t += a(1)) 
      t = new ArrayBuffer[ActorRef]()
              
      for (i <- 1 to n - 2) 
      {
        a(i) ! SetNeighbor(t += (a(i - 1), a(i + 1)))
        t = new ArrayBuffer[ActorRef]()
      }

      a(n - 1) ! SetNeighbor(t += a(n - 2))
      t = new ArrayBuffer[ActorRef]()
      
    case "2d" =>       ////////// 2D TOPOLOGY ////////////

      a(0) ! SetNeighbor(t += (a(1), a(e))) 
      t = new ArrayBuffer[ActorRef]()

      for (i <- 1 to e - 2) 
      {
        a(i) ! SetNeighbor(t += (a(i - 1), a(i + 1), a(i + e))) 
        t = new ArrayBuffer[ActorRef]()
      }

      a(e - 1) ! SetNeighbor(t += (a(e - 2), a(e - 1 + e))) 
      t = new ArrayBuffer[ActorRef]()

      for (i: Int <- e to n - e - 1) 
      { 
        if (i % e == 0) 
        {
          a(i) ! SetNeighbor(t += (a(i - e), a(i + e), a(i + 1)))
          t = new ArrayBuffer[ActorRef]()
        } 
        else if (i % e == e - 1) 
        {
          a(i) ! SetNeighbor(t += (a(i - e), a(i + e), a(i - 1)))
          t = new ArrayBuffer[ActorRef]()
        } 
        else 
        {
          a(i) ! SetNeighbor(t += (a(i - e), a(i + e), a(i - 1), a(i + 1)))
          t = new ArrayBuffer[ActorRef]()
        }
      }

      a(n - e) ! SetNeighbor(t += (a(n - e - e), a(n - e + 1))) 
      t = new ArrayBuffer[ActorRef]()

      for (i <- n - e + 1 to n - 2) 
      {
        a(i) ! SetNeighbor(t += (a(i - 1), a(i + 1), a(i - e))) 
        t = new ArrayBuffer[ActorRef]()
      }

      a(n - 1) ! SetNeighbor(t += (a(n - 2), a(n - 1 - e)))
      t = new ArrayBuffer[ActorRef]()

    case "imperfect2d" =>      ////////// IMPERFECT-2D TOPOLOGY //////////

      var list = a.clone()
      var random: ActorRef = null
      a(0) ! SetNeighbor(t += (a(1), a(e))) 

      random = (list - a(0) -- t)(Random.nextInt((list - a(0) -- t).length))

      a(0) ! AddNum(random)

      random ! AddNum(a(0))

      list -= (a(0), random)

      t = new ArrayBuffer[ActorRef]()

      for (i <- 1 to e - 2) 
      {
        a(i) ! SetNeighbor(t += (a(i - 1), a(i + 1), a(i + e)))
        if (list.contains(a(i))) 
        {
          random = (list - a(i) -- t)(Random.nextInt((list - a(i) -- t).length))
          a(i) ! AddNum(random)
          random ! AddNum(a(i))
          list -= (a(i), random)
        }
        t = new ArrayBuffer[ActorRef]()
      }

      a(e - 1) ! SetNeighbor(t += (a(e - 2), a(e - 1 + e))) 
      if (list.contains(a(e - 1))) 
      {
        random = (list - a(e - 1) -- t)(Random.nextInt((list - a(e - 1) -- t).length))
        a(e - 1) ! AddNum(random)
        random ! AddNum(a(e - 1))
        list -= (a(e - 1), random)
      }
      t = new ArrayBuffer[ActorRef]()

      for (i: Int <- e to n - e - 1) 
      {
        if (i % e == 0) 
        {
          a(i) ! SetNeighbor(t += (a(i - e), a(i + e), a(i + 1)))
          if (list.contains(a(i)) && list.length >= 2) 
          {
            random = (list - a(i) -- t)(Random.nextInt((list - a(i) -- t).length))
            a(i) ! AddNum(random)
            random ! AddNum(a(i))
            list -= (a(i), random)
          }
          t = new ArrayBuffer[ActorRef]()
        } 
        else if (i % e == e - 1) 
        {
          a(i) ! SetNeighbor(t += (a(i - e), a(i + e), a(i - 1)))
          if (list.contains(a(i)) && list.length >= 2) 
          {
            random = (list - a(i) -- t)(Random.nextInt((list - a(i) -- t).length))
            a(i) ! AddNum(random)
            random ! AddNum(a(i))
            list -= (a(i), random)
          }
          t = new ArrayBuffer[ActorRef]()
        } 
        else 
        {
          a(i) ! SetNeighbor(t += (a(i - e), a(i + e), a(i - 1), a(i + 1)))
          if (list.contains(a(i)) && list.length >= 2) 
          {
            random = (list - a(i) -- t)(Random.nextInt((list - a(i) -- t).length))
            a(i) ! AddNum(random)
            random ! AddNum(a(i))
            list -= (a(i), random)
          }
          t = new ArrayBuffer[ActorRef]()
        }
      }

      a(n - e) ! SetNeighbor(t += (a(n - e - e), a(n - e + 1))) 
      if (list.contains(a(n - e)) && list.length >= 2) 
      {
        random = (list - a(n - e) -- t)(Random.nextInt((list - a(n - e) -- t).length))
        a(n - e) ! AddNum(random)
        random ! AddNum(a(n - e))
        list -= (a(n - e), random)
      }
      t = new ArrayBuffer[ActorRef]()

      for (i <- n - e + 1 to n - 2) 
      {
        a(i) ! SetNeighbor(t += (a(i - 1), a(i + 1), a(i - e))) 
        if (list.contains(a(i)) && list.length >= 2) 
        {
          random = (list - a(i) -- t)(Random.nextInt((list - a(i) -- t).length))
          a(i) ! AddNum(random)
          random ! AddNum(a(i))
          list -= (a(i), random)
        }
        t = new ArrayBuffer[ActorRef]()
      }

      a(n - 1) ! SetNeighbor(t += (a(n - 2), a(n - 1 - e))) 
      if (list.contains(a(n - 1)) && list.length >= 2) 
      {
        random = (list - a(n - 1) -- t)(Random.nextInt((list - a(n - 1) -- t).length))
        a(n - 1) ! AddNum(random)
        random ! AddNum(a(n - 1))
        list -= (a(n - 1), random)
      }
      t = new ArrayBuffer[ActorRef]()
      
    case whatever =>
      
      println("\nERROR: The Specified Topology is not Full, Line, 2D or Imperfect-2D\n")
      context.stop(self)
      context.system.shutdown()
  }
  
  def receive = 
  {
    case START =>
      timetaken = System.currentTimeMillis()
      if (algorithm.toLowerCase() == "gossip") 
      {
        a(Random.nextInt(n)) ! Gossip
      } 
      else if (algorithm.toLowerCase() == "push-sum") 
      {
        a(Random.nextInt(n)) ! PushSum(0, 0)
      } 
      else 
      {
        println("\nERROR: Only Gossip or Push-Sum algorithms are supported\n")
        context.stop(self)
        context.system.shutdown()
      }

    case End =>
      finish = finish + 1
      if (finish == n) 
      {
        context.system.shutdown()
        println("Number of Nodes: " + n)
        println("Time: " + (System.currentTimeMillis() - timetaken) + " milliseconds")
      }
      if (algorithm == "push-sum" && finish == 1) 
      {
        context.system.shutdown()
        println("Number of Nodes: " + n)
        println("Time: " + (System.currentTimeMillis() - timetaken) + " milliseconds")

      }
    case NoNeighborFound =>
      noNbrCount = noNbrCount + 1
  }
}


object project2 
{
  
  var flag = 0
  
  def gossip(numNodes: Int, topology: String, algorithm: String) 
  {
      val system = ActorSystem("GossipSimulator")
      val master = system.actorOf(Props(new Master(numNodes, topology, algorithm)), name = "master")
      master ! START
  }
  
  def main(args: Array[String]) 
  {
	  while(flag!=1)
	  {
		  println("Enter your specification(Format: <Number of Nodes> <Topology> <Algorithm>)")
		  var inputstring = Console.readLine()
		  val inputarr = inputstring.split(" ")
		  if(inputarr.length != 3)
		  {
			  println("Incorrect Input Parameters! Please Enter Again")
		  }
		  else 
		  {
			  flag = 1
			  gossip(numNodes = inputarr(0).toInt, topology = inputarr(1), algorithm = inputarr(2)) 
		  }
	  }
  }
}
