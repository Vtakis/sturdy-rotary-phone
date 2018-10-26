#include <stdio.h>
#include <stdlib.h>
#include "../header-files/functions.h"

int main(int argc,char** argv)
{
	int32_t A[]={100,100,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20};
	uint32_t size_A=22;
	int32_t B[]={1,9,3};
	uint32_t size_B=3;
	int32_t startR=0,endR,startS=0,endS,counter = 0;
	indexHT* ht;
	relation *S,*R;
	relation *RS,*RR;
	hist *histSumArrayR,*histSumArrayS;
	resultList *resList;

	createRelations(A,size_A,B,size_B,&S,&R);
	
	histSumArrayS=createSumHistArray(createHistArray(&S));//dhmiourgia hist sum arrays
	histSumArrayR=createSumHistArray(createHistArray(&R));

	RR=createReOrderedArray(R,histSumArrayR);//dhmiourgia reordered array
	RS=createReOrderedArray(S,histSumArrayS);

	resList=initializeResultList();

	while(1)
	{
		if(counter+1 > histSumArrayR->histSize-1)break;		//counter+1 giati einai h thesh gia to end ,dld an h thesh pou tha einai to end einai ektos oriwn break//
		startR = histSumArrayR->histArray[counter].count;
		startS = histSumArrayS->histArray[counter].count;
		endR = histSumArrayR->histArray[counter+1].count-1;
		endS = histSumArrayS->histArray[counter+1].count-1;

		counter++;
		if(endR+1 == startR || endS+1 == startS) continue;  //elegxos an kapoio einai keno , opote den bgazei apotelesma

		if((endR - startR) >= (endS - startS))//kanoume hash to pio mikro bucket
		{
			ht=createHashTable(RS,startS,endS);
			compareRelations(ht,RR,startR,endR,RS,resList);
			deleteHashTable(&ht);
		}
		else
		{
			ht=createHashTable(RR,startR,endR);
			compareRelations(ht,RS,startS,endS,RR,resList);
			deleteHashTable(&ht);
		}

	}

	while(resList->end!=NULL){
		for(int i=0;i<resList->end->tupleSize;i++){
			printf("%d %d\n",resList->end->array_tuple[i].id,resList->end->array_tuple[i].value);
		}
		resList=resList->end;
	}
}




















