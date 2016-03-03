#include <stdio.h>

int dum(int &a) {
	a=3;
}

int main(int argc, char* argv[]) {
	int a = 5;
	dum(a);
	printf("%d\n", a);
}
