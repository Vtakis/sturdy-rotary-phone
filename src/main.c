#include <stdio.h>
#include <stdlib.h>
#include "../header-files/functions.h"
#include <unistd.h>

int main(int argc,char** argv)
{


	uint32_t size_A=150;
	int32_t A[size_A];
	uint32_t size_B=50;
	int32_t B[size_B];

	writeFile(size_A,size_B);

	readFile(A,&size_A,B,&size_B);

	resultList* resList;
	relation *S,*R;

	createRelations(A,size_A,B,size_B,&S,&R);
	resList = RadixHashJoin(R,S,size_A,size_B);
	printResults(resList);

	deleteResultList(resList);
}

