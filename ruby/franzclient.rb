require 'socket'

class Consumer
  def initialize(addr, port, topic)
    sock = TCPSocket.new(addr, port)
    header = "version=1,topic=#{topic},api=consume"
    sock.write([header.bytesize].pack('N*'), header)
    sock.flush

    @keepalive = Thread.new do
      loop do
        sock.write("PING\n")
        sock.flush
        sleep(60)
      end
    end

    @sock = sock
  end

  def recv
    @sock.readline
  end
end

class Producer
  def initialize(addr, port, topic)
    sock = TCPSocket.new(addr, port)
    header = "version=1,topic=#{topic},api=produce"
    sock.write([header.bytesize].pack('N*'), header)
    sock.flush

    @sock = sock
  end

  def send(msg)
    @sock.write("#{msg}\n")
  end

  def send_unbuffered(msg)
    @sock.write("#{msg}\n")
    @sock.flush
  end
end

p = Producer.new('127.0.0.1', 8085, 'test')
p.send_unbuffered 'a'
p.send_unbuffered 'b'
p.send_unbuffered 'asdf'

c = Consumer.new('127.0.0.1', 8085, 'test')
puts c.recv
puts c.recv
puts c.recv
puts c.recv
