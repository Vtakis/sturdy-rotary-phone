#include <stdio.h>
#include <stdlib.h>
#include "../header-files/functions.h"

int main(int argc,char** argv)
{
	int32_t i;
	int32_t A[]={1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20};
	uint32_t size_A=20;
	int32_t B[]={1,1,3};
	uint32_t size_B=3;

	relation *S,*R;

	createRelations(A,size_A,B,size_B,&S,&R);

	printf("Takis start test\n");
	createHistArray(&S);
	createHistArray(&R);
	printf("Takis end test\n");

	printf("S->num_of_tuples = %d \n",S->num_of_tuples);
	for(i=0;i<size_A;i++){
		printf("%d %d\n",S->tuples[i].id,S->tuples[i].value);
	}
	printf("R->num_of_tuples = %d \n",R->num_of_tuples);
	for(i=0;i<size_B;i++){
		printf("%d %d\n",R->tuples[i].id,R->tuples[i].value);
	}

	relation *RS,*RR;
	int32_t *sumArrayS;
	/*sumArrayS=malloc(4*sizeof(int32_t));//n=2
	sumArrayS[0]=0;
	sumArrayS[1]=5;
	sumArrayS[2]=10;
	sumArrayS[3]=15;*/

	sumArrayS=malloc(8*sizeof(int32_t));//n=3
	sumArrayS[0]=0;
	sumArrayS[1]=2;
	sumArrayS[2]=5;
	sumArrayS[3]=8;
	sumArrayS[4]=11;
	sumArrayS[5]=14;
	sumArrayS[6]=16;
	sumArrayS[7]=18;

	RR=createReOrderedArray(S,sumArrayS,8);//pinakas,sumarray gia ton pinaka,n^2

	printf("\n\n\n\n\nRR->num_of_tuples = %d \n",RR->num_of_tuples);
	for(i=0;i<RR->num_of_tuples;i++){
		printf("%d %d\n",RR->tuples[i].id,RR->tuples[i].value);
	}
}
