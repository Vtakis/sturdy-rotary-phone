#include <stdio.h>
#include <stdlib.h>
#include "../header-files/functions.h"


int main()
{
	uint32_t size_A=200;
	int32_t A[size_A];
	uint32_t size_B=100;
	int32_t B[size_B];
	int32_t i;

	resultList* resList;
	relation *S,*R;

	//case 1
	printf("Case 1\n");
	for(i=0;i<size_A;i++){
		A[i]=1;
	}
	for(i=0;i<size_B;i++){
		B[i]=1;
	}

	createRelations(A,size_A,B,size_B,&S,&R);
	resList = RadixHashJoin(R,S,size_A,size_B);
	printResults(resList);
	if(resList->numberOfResults == size_A*size_B){
		printf("Successful Join!\n");
	}
	else{
		printf("Failure!!!\n");
	}
	deleteResultList(resList);

	//case 2
	printf("\nCase 2\n");
	for(i=0;i<size_A;i++){
		A[i]=1;
	}
	for(i=0;i<size_B;i++){
		B[i]=2;
	}

	createRelations(A,size_A,B,size_B,&S,&R);
	resList = RadixHashJoin(R,S,size_A,size_B);
	printResults(resList);
	if(resList->numberOfResults == 0){
		printf("Successful Join!\n");
	}
	else{
		printf("Failure!!!\n");
	}
	deleteResultList(resList);

	//case 3
	printf("\nCase 3\n");
	for(i=0;i<size_A;i++){
		A[i]=i;
	}
	for(i=0;i<size_B;i++){
		B[i]=i;
	}

	createRelations(A,size_A,B,size_B,&S,&R);
	resList = RadixHashJoin(R,S,size_A,size_B);
	printResults(resList);
	int32_t forcompare;
	if(size_A <=size_B){
		forcompare=size_A;
	}
	else{
		forcompare=size_B;
	}
	if(resList->numberOfResults == forcompare){
		printf("Successful Join!\n");
	}
	else{
		printf("Failure!!!\n");
	}
	deleteResultList(resList);

	//case 4
	printf("\nCase 4\n");
	int32_t mod=10;
	for(i=0;i<size_A;i++){
		A[i]=i%mod;
	}
	for(i=0;i<size_B;i++){
		B[i]=i%mod;
	}
	createRelations(A,size_A,B,size_B,&S,&R);
	resList = RadixHashJoin(R,S,size_A,size_B);
	printResults(resList);
	if(resList->numberOfResults == mod*((size_A/mod)*(size_B/mod))){
		printf("Successful Join!\n");
	}
	else{
		printf("Failure!!!\n");
	}
	deleteResultList(resList);

}
