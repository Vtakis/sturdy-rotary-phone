#include <stdio.h>
#include <stdlib.h>
#include "../header-files/functions.h"

void createRelations(int32_t A[],uint32_t size_A,int32_t B[],uint32_t size_B,relation **S,relation **R){
	int32_t i;

	*S=malloc(sizeof(relation));
	(*S)->num_of_tuples=size_A;
	(*S)->tuples=malloc(size_A*sizeof(tuple));
	for(i=0;i<size_A;i++){
		(*S)->tuples[i].id=i+1;
		(*S)->tuples[i].value=A[i];
	}

	*R=malloc(sizeof(relation));
	(*R)->num_of_tuples=size_B;
	(*R)->tuples=malloc(size_B*sizeof(tuple));
	for(i=0;i<size_B;i++){
		(*R)->tuples[i].id=i+1;
		(*R)->tuples[i].value=B[i];
	}
	
}
