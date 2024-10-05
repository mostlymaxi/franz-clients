import java.net.Socket

class FranzProducer(broker: String, port: Int, topic: String):
  val sock = new Socket(broker, port)
  val out_stream = sock.getOutputStream()

  out_stream.write("0\n".getBytes())
  out_stream.write(topic.getBytes())
  out_stream.write("\n".getBytes())

  def send(msg: String): Unit = 
    out_stream.write(msg.getBytes())
    out_stream.write("\n".getBytes())

// 1. tcp sockets
// 2. strings maybe bufferuing??? 
//    consumer:
// 3. threads - background keepalive thread
//        loop {
//          sends msg PING
//        }
class FranzConsumer(broker: String, port: Int, topic: String):
  val sock = new Socket(broker, port)
  val out_stream = sock.getOutputStream()

  val in_stream = sock.getInputStream()
  val in_buf = io.Source.fromInputStream(in_stream).getLines()

  // TODO: Buffer using BufferedLineIterator
  out_stream.write("1\n".getBytes())
  out_stream.write(topic.getBytes())
  out_stream.write("\n".getBytes())

  scala.concurrent.Future(test_keepalive())

  def recv(): String = 
    return in_buf.next()


def test_keepalive(): Unit =
    while():
      Thread.sleep(60_000)
      println("PING")




@main def hello(): Unit =
  val p = new FranzProducer("127.0.0.1", 8085, "test")
  p.send("testing123")

  val c = new FranzConsumer("127.0.0.1", 8085, "test")
  println(c.recv())

