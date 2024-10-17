#include "franzclient.h"
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>

FranzReceiver new_FranzReceiver(unsigned short port, char* sa_data) {
	int socketfd;
	FranzReceiver rx;
	struct sockaddr_in addr;

	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr.s_addr = inet_addr("127.0.0.1");
	addr.sin_zero[8] = '\0';

	socketfd = socket(AF_INET, SOCK_STREAM, 0);

	if ((connect(socketfd, (struct sockaddr*)&addr, sizeof(struct sockaddr))) == -1) {
		fprintf(stderr, "fuck\n");
	};

	rx.socket = fdopen(socketfd, "r+");
	char handshake[] = "1\ntest\n";
	fwrite(handshake, sizeof(char), strlen(handshake), rx.socket);

	return rx;
}

FranzSender new_FranzSender(unsigned short port, char* sa_data) {
	int socketfd;
	FranzSender tx;
	struct sockaddr_in addr;

	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr.s_addr = inet_addr("127.0.0.1");
	addr.sin_zero[8] = '\0';

	socketfd = socket(AF_INET, SOCK_STREAM, 0);

	if ((connect(socketfd, (struct sockaddr*)&addr, sizeof(struct sockaddr))) == -1) {
		fprintf(stderr, "fuck\n");
	};

	tx.socket = fdopen(socketfd, "r+");
	char handshake[] = "0\ntest\n";
	fwrite(handshake, sizeof(char), strlen(handshake), tx.socket);

	return tx;
}

void push(FranzSender tx, char* data, size_t len) {
	char* buf;
	buf = malloc(len + 2);
	memcpy(buf, data, len + 1);
	strcat(buf, "\n");

	fprintf(stderr, "%d\n", fwrite(buf, sizeof(char), strlen(buf), tx.socket));
}

char* pop(FranzReceiver rx) {
	char* lineptr;
	size_t n;

	lineptr = NULL;
	n = 0;
	getline(&lineptr, &n, rx.socket);
	return lineptr;
}

int main() {
	FranzSender tx;
	FranzReceiver rx;
	tx = new_FranzSender(8085, "127.0.0.1");

	push(tx, "asdf", 4);
	push(tx, "mostlypacket", 12);

	rx = new_FranzReceiver(8085, "127.0.0.1");

	while(1) {
		fprintf(stderr, "%s", pop(rx));
	}
}


