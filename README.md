### Franz Protocol
the franz protocol consists of two messages separated by newlines ('\n')

Message 1: What kind of client am I?
(Producer / Consumer)

Producer = "0"
Consumer = "1"

Message 2: What topic to consume or produce to?
Topic Name

"some string"

### The Speedrun
make a library that can create consumers and producer with recv and 
send functions respectively

Tier 1: 
- make methods to instantiate consumers and producers
- make methods to send and receive messages

speedrun ends when we can write:
-  make producer
-  write: "a", "b", "cdef"
- make consumer
- receive: "a", "b", "cdef"


