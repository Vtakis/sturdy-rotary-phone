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
	hist *histArrayR,*histSumArrayR,*histArrayS,*histSumArrayS;   							//to xrhsimopoiw gia na ftiaksw to hashtable kathe bucket //
	createRelations(A,size_A,B,size_B,&S,&R);

/////////////////////////////////////////////////////////
	printf("Takis start test\n");
	hist *sumArrayS;	
	
	sumArrayS=createSumHistArray(createHistArray(&S));
	printf("---------------\n");
	createSumHistArray(createHistArray(&R));
	
	printf("Takis end test\n\n");
////////////////////////////////////////////////////////	
	
	printf("S->num_of_tuples = %d \n",S->num_of_tuples);
	for(i=0;i<size_A;i++){
		printf("%d %d\n",S->tuples[i].id,S->tuples[i].value);
	}
	printf("R->num_of_tuples = %d \n",R->num_of_tuples);
	for(i=0;i<size_B;i++){
		printf("%d %d\n",R->tuples[i].id,R->tuples[i].value);
	}

	relation *RS,*RR;
/*	hist *sumArrayS;
	sumArrayS=malloc(4*sizeof(int32_t));//n=2
	sumArrayS[0]=0;
	sumArrayS[1]=5;
	sumArrayS[2]=10;
	sumArrayS[3]=15;

	sumArrayS=malloc(sizeof(hist));//n=3
	sumArrayS->histSize=8;
	sumArrayS->histArray=malloc(sumArrayS->histSize*sizeof(histNode));
	//den exw kanei to tuple
	sumArrayS->histArray[0].count=0;
	sumArrayS->histArray[1].count=2;
	sumArrayS->histArray[2].count=5;
	sumArrayS->histArray[3].count=8;
	sumArrayS->histArray[4].count=11;
	sumArrayS->histArray[5].count=14;
	sumArrayS->histArray[6].count=16;
	sumArrayS->histArray[7].count=18;
*/
	RR=createReOrderedArray(S,sumArrayS);//pinakas,sumarray gia ton pinaka,n^2

	printf("\n\n\n\n\nRR->num_of_tuples = %d \n",RR->num_of_tuples);

	for(i=0;i<RR->num_of_tuples;i++){
		printf("%d %d\n",RR->tuples[i].id,RR->tuples[i].value);
	}
return;
	int32_t startR,endR,startS,endS,counter = 0;
	startR = 0;
	startS = 0;
	while(1)
	{
		if(counter+1 > histArrayR->histSize-1)break;				//counter+1 giati einai h thesh gia to end ,dld an h thesh pou tha einai to end einai ektos oriwn break//
		startR = histArrayR->histArray[counter].count;
		startS = histArrayS->histArray[counter].count;
		endR = histArrayR->histArray[counter+1].count-1;
		endS = histArrayS->histArray[counter+1].count-1;
		if((endR - startR) >= (endS - startS))
		{
			createHashTable(RS,startS,endS);
			/*toDo 	compare*/
		}
		else
		{
			createHashTable(RR,startR,endR);
			/*toDo 	compare*/
		}
		if(endR == -1 || endS == -1){
			counter++;
			continue;
		}
	}

}
