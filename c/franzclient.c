#include "franzclient.h"
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

franz_consumer_t franz_consumer_new(char *addr, uint16_t port, char *topic,
                                    int16_t group) {
  int32_t socketfd;
  uint32_t len;
  char *handshake;
  franz_consumer_t rx;
  struct sockaddr_in sa_addr;

  sa_addr.sin_family = AF_INET;
  sa_addr.sin_port = htons(port);
  sa_addr.sin_addr.s_addr = inet_addr(addr);
  // sa_addr.sin_zero[8] = '\0';

  socketfd = socket(AF_INET, SOCK_STREAM, 0);

  if ((connect(socketfd, (struct sockaddr *)&sa_addr,
               sizeof(struct sockaddr))) == -1) {
    perror("couldn't connect to broker");
  };

  rx.socket = fdopen(socketfd, "r+");

  handshake = calloc(1024, sizeof(char));
  if (group == -1) {
    char fmt[] = "version=1,topic=%s,api=consume";
    snprintf(handshake, 1024, fmt, topic);
  } else {
    char fmt[] = "version=1,topic=%s,api=consume,group=%lu";
    snprintf(handshake, 1024, fmt, topic, group);
  }

  len = htonl(strlen(handshake));
  fwrite(&len, sizeof(uint32_t), 1, rx.socket);
  fwrite(handshake, sizeof(char), strlen(handshake), rx.socket);
  fflush(rx.socket);

  return rx;
}

franz_producer_t franz_producer_new(char *addr, uint16_t port, char *topic) {
  int32_t socketfd;
  uint32_t len;
  franz_producer_t tx;
  struct sockaddr_in sa_addr;

  sa_addr.sin_family = AF_INET;
  sa_addr.sin_port = htons(port);
  sa_addr.sin_addr.s_addr = inet_addr(addr);
  // sa_addr.sin_zero[8] = '\0';

  socketfd = socket(AF_INET, SOCK_STREAM, 0);

  if ((connect(socketfd, (struct sockaddr *)&sa_addr,
               sizeof(struct sockaddr))) == -1) {
    perror("couldn't connect to broker");
    exit(1);
  };

  tx.socket = fdopen(socketfd, "r+");

  char fmt[] = "version=1,topic=%s,api=produce";
  char *handshake = calloc(1024, sizeof(char));
  snprintf(handshake, 1024, fmt, topic);
  len = htonl(strlen(handshake));

  fwrite(&len, sizeof(uint32_t), 1, tx.socket);
  fwrite(handshake, sizeof(char), strlen(handshake), tx.socket);
  fflush(tx.socket);

  return tx;
}

void franz_send(franz_producer_t tx, char *data, size_t len) {
  fwrite(data, sizeof(char), len, tx.socket);
  fwrite("\n", sizeof(char), strlen("\n"), tx.socket);
}

void franz_send_unbuffered(franz_producer_t tx, char *data, size_t len) {
  franz_send(tx, data, len);
  fflush(tx.socket);
}

char *franz_recv(franz_consumer_t rx) {
  char *msg;
  size_t n;

  msg = NULL;
  n = 0;
  getline(&msg, &n, rx.socket);
  return msg;
}

int main() {
  franz_producer_t tx;
  franz_consumer_t rx;
  tx = franz_producer_new("127.0.0.1", 8085, "test");

  franz_send_unbuffered(tx, "asdf", 4);
  franz_send_unbuffered(tx, "mostlypacket", 12);

  rx = franz_consumer_new("127.0.0.1", 8085, "test", -1);

  while (1) {
    fprintf(stderr, "%s", franz_recv(rx));
  }
}
