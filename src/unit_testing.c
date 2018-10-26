#include <stdio.h>
#include <stdlib.h>
#include "../header-files/functions.h"


int main()
{
	uint32_t size_A=1000;
	int32_t A[size_A];
	uint32_t size_B=1000;
	int32_t B[size_B];
	FILE *fp;
	fp=fopen("input.txt","w");
	fprintf(fp,"%d %d\n",size_A,size_B);
	for(int32_t i=0;i<size_A;i++)
	{
		fprintf(fp,"%d,",1);
	}
	fprintf(fp,"\n");
	for(int32_t i=0;i<size_B;i++)
	{
		fprintf(fp,"%d,",1);
	}
	fclose(fp);


	fp=fopen("input.txt","r");
	int32_t el1,el2;
	fscanf(fp,"%d %d",&size_A,&size_B);

	for(int32_t i=0;i<size_A;i++){
		fscanf(fp,"%d,",&el1);
		A[i]=el1;
	}
	for(int32_t i=0;i<size_B;i++){
		fscanf(fp,"%d,",&el2);
		B[i]=el2;
	}

	resultList* resList;
	relation *S,*R;

	createRelations(A,size_A,B,size_B,&S,&R);
	resList = RadixHashJoin(R,S,size_A,size_B);
	printResults(resList);



}
