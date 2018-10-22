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
	indexHT* ht;
	relation *S,*R;
	hist *histSumArrayR,*histSumArrayS; 			//to xrhsimopoiw gia na ftiaksw to hashtable kathe bucket //
	//indexHT *ht;

	createRelations(A,size_A,B,size_B,&S,&R);

/////////////////////////////////////////////////////////
	printf("Takis start test\n");
		
	
	histSumArrayS=createSumHistArray(createHistArray(&S));
	
	printf("---------------\n");
	histSumArrayR=createSumHistArray(createHistArray(&R));
	
	printf("Takis end test\n\n");
////////////////////////////////////////////////////////	
	

	relation *RS,*RR;

	RR=createReOrderedArray(R,histSumArrayR);//pinakas,sumarray gia ton pinaka,n^2
	RS=createReOrderedArray(S,histSumArrayS);//pinakas,sumarray gia ton pinaka,n^2


	for(i=0;i<RS->num_of_tuples;i++){
		printf("%d %d\n",RS->tuples[i].id,RS->tuples[i].value);
	}

	printf("\n\n\n\n\nRR->num_of_tuples = %d \n",RR->num_of_tuples);
	for(i=0;i<RR->num_of_tuples;i++){
		printf("RR[%d] -> %d %d \n",i,RR->tuples[i].value,RR->tuples[i].id);
	}

	printf("\n\n\n\n\nRS->num_of_tuples = %d \n",RS->num_of_tuples);
	for(i=0;i<RS->num_of_tuples;i++){
		printf("RS[%d] -> %d %d\n",i,RS->tuples[i].value,RS->tuples[i].id);
	}
	for(i=0;i<histSumArrayR->histSize;i++){
		printf("1Hist[%d]=%d\n",i,histSumArrayR->histArray[i].count);
	}
	printf("\n");
	for(i=0;i<histSumArrayS->histSize;i++){
		printf("2Hist[%d]=%d\n",i,histSumArrayS->histArray[i].count);
	}

	int32_t startR,endR,startS,endS,counter = 0;
	startR = 0;
	startS = 0;
	while(1)
	{
		if(counter+1 > histSumArrayR->histSize-1)break;				//counter+1 giati einai h thesh gia to end ,dld an h thesh pou tha einai to end einai ektos oriwn break//
	//	printf("counter=%d\n",counter);
		startR = histSumArrayR->histArray[counter].count;
		startS = histSumArrayS->histArray[counter].count;
		endR = histSumArrayR->histArray[counter+1].count-1;
		endS = histSumArrayS->histArray[counter+1].count-1;

		//printf("startR=%d \nendR=%d \nstartS=%d \nendS=%d\n",startR,endR,startS,endS);
		if(endR+1 == startR || endS+1 == startS){//enas elegxos an kapoio einai keno
			//printf("SKIP\n");
			counter++;
			continue;
		}else counter++;

		if((endR - startR) >= (endS - startS))
		{
			ht=createHashTable(RS,startS,endS);
			for(i=0;i<ht->bucketSize;i++)
			{
				printf("Bucket[%d]=%d\n",i,ht->bucketArray[i].lastChainPosition);
			}
			for(i=0;i<ht->chainSize;i++)
			{
				printf("Chain[%d]->bucketPos=%d\nChain[%d]->prevChainPost=%d\n",i,ht->chainNode[i].bucketPos,i,ht->chainNode[i].prevchainPosition);
			}
			deleteHashTable(&ht);
			//return 1;
			/*toDo 	compare*/
		}
		else
		{
			ht=createHashTable(RR,startR,endR);
			for(i=0;i<ht->bucketSize;i++)
			{
				printf("Bucket[%d]=%d\n",i,ht->bucketArray[i].lastChainPosition);
			}
			for(i=0;i<ht->chainSize;i++)
			{
				printf("Chain[%d]->bucketPos=%d\nChain[%d]->prevChainPost=%d\n",i,ht->chainNode[i].bucketPos,i,ht->chainNode[i].prevchainPosition);
			}
			deleteHashTable(&ht);
			//return 1;
			/*toDo 	compare*/
		}

	}
	printf("\n\nTELOS\n\n");
}
