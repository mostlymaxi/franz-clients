require 'socket'

class Consumer
  def initialize(addr, port, topic, group = nil)
    sock = TCPSocket.new(addr, port)
    header = if group.nil?
               "version=1,topic=#{topic},api=consume"
             else
               "version=1,topic=#{topic},group=#{group},api=consume"
             end
    sock.write([header.bytesize].pack('N*'), header)
    sock.flush

    @keepalive = Thread.new do
      sleep(15)
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

c = Consumer.new('127.0.0.1', 8085, 'test', 'groupma')
puts c.recv
puts c.recv
puts c.recv
puts c.recv
