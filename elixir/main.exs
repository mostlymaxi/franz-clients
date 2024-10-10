defmodule FranzConsumer do
  use GenServer

  @initial_state %{socket: nil}

  def start_link do
    GenServer.start_link(__MODULE__, @initial_state)
  end

  def init(state) do
    opts = [:binary, line_delimeter: ?\n, active: false]
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", 8085, opts)
    :ok = :gen_tcp.send(socket, "1" <> "\n")
    :ok = :gen_tcp.send(socket, "test" <> "\n")
    {:ok, %{state | socket: socket}}
  end

  def handle_call(:pop, _from, %{socket: socket} = state) do
    {:ok, msg} = :gen_tcp.recv(socket, 0)
    {:reply, msg, state}
  end

  def pop(pid) do
    GenServer.call(pid, :pop)
  end
end

defmodule FranzProducer do
  use GenServer

  @initial_state %{socket: nil}

  def start_link do
    GenServer.start_link(__MODULE__, @initial_state)
  end

  def init(state) do
    opts = [:binary, active: false]
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", 8085, opts)
    :ok = :gen_tcp.send(socket, "0" <> "\n")
    :ok = :gen_tcp.send(socket, "test" <> "\n")
    {:ok, %{state | socket: socket}}
  end

  def handle_cast({:push, msg}, %{socket: socket} = state) do
    :ok = :gen_tcp.send(socket, msg <> "\n")
    {:noreply, state}
  end

  def push(pid, msg) do
    GenServer.cast(pid, {:push, msg})
  end
end

{:ok, pid} = FranzProducer.start_link()
FranzProducer.push(pid, "hello")

{:ok, pid} = FranzConsumer.start_link()
FranzConsumer.pop(pid)
