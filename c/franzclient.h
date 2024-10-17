// TODO: IFDEF
//
#include <stdio.h>

typedef struct FranzReceiver {
	FILE* socket;
} FranzReceiver;


typedef struct FranzSender {
	FILE* socket;
} FranzSender;

FranzReceiver new_FranzReceiver(unsigned short port, char* sa_data);
char* pop(FranzReceiver rx);

FranzSender new_FranzSender(unsigned short port, char* sa_data);
void push(FranzSender tx, char* data, size_t len);

