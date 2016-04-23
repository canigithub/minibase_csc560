#include <stdio.h>
#include <stdlib.h>

int main(int argc, char** argv) {
	char *a = malloc(-1);
	printf("%ud\n", (unsigned int)(-1));
}
