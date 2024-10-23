// TODO: IFDEF
//
#include <stdint.h>
#include <stdio.h>

typedef struct FranzConsumer {
  FILE *socket;
} franz_consumer_t;

typedef struct FranzProducer {
  FILE *socket;
} franz_producer_t;

franz_consumer_t franz_consumer_new(char *addr, uint16_t port, char *topic,
                                    int16_t group);

char *franz_recv(franz_consumer_t rx);

franz_producer_t franz_producer_new(char *addr, uint16_t port, char *topic);
void franz_send(franz_producer_t tx, char *data, size_t len);
void franz_send_unbuffered(franz_producer_t tx, char *data, size_t len);
