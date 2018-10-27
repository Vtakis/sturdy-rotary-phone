#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "../header-files/functions.h"
#define bucketPosNum 50
#define N 12

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

hist* createHistArray(relation **rel){
	int32_t i,j,*freq;
	int32_t count;
	hist *Hist;
	Hist=malloc(sizeof(Hist));
	Hist->histSize=pow(2,N);
	Hist->histArray=malloc(Hist->histSize*sizeof(histNode));	
	for(i=0;i<Hist->histSize;i++){
		Hist->histArray[i].count=0;
		Hist->histArray[i].point=0;
	}

	freq=malloc((*rel)->num_of_tuples*sizeof(int32_t));
	for(i=0;i<(*rel)->num_of_tuples;i++)
		freq[i]=-1;

	for(i=0;i<(*rel)->num_of_tuples;i++){
		count=1;
		for(j=i+1;j<(*rel)->num_of_tuples;j++){
			if((*rel)->tuples[i].value==(*rel)->tuples[j].value){
				count++;				
				freq[j]=0;
			}
		}
		if(freq[i]!=0){
			freq[i]=count;
			Hist->histArray[(*rel)->tuples[i].value%Hist->histSize].count+=freq[i];
		}
	}
	free(freq);
	return Hist;
}

hist* createSumHistArray(hist *array){
	int32_t i,nextBucket=0;
	hist *Hist;
	Hist=malloc(sizeof(Hist));
	Hist->histSize=pow(2,N);
	Hist->histArray=malloc(Hist->histSize*sizeof(histNode));
	
	for(i=0;i<Hist->histSize;i++){
		Hist->histArray[i].count=0;
		Hist->histArray[i].point=0;
	}
	for(i=0;i<array->histSize;i++){
		if(i==0){
			nextBucket=array->histArray[i].count;
			Hist->histArray[i].count=0;
			Hist->histArray[i].point=0;
		}
		else{
			Hist->histArray[i].count=nextBucket;
			Hist->histArray[i].point=nextBucket;
			nextBucket+=array->histArray[i].count;
		}
	}
	free(array->histArray);
	free(array);
	return Hist;
}

relation* createReOrderedArray(relation *array,hist *sumArray){
	relation *reOrderedArray;
	int32_t i;

	reOrderedArray=malloc(sizeof(relation));
	reOrderedArray->num_of_tuples=array->num_of_tuples;
	reOrderedArray->tuples=malloc(array->num_of_tuples*sizeof(tuple));

	for(i=0;i<array->num_of_tuples;i++){
		reOrderedArray->tuples[sumArray->histArray[array->tuples[i].value%sumArray->histSize].point].id=array->tuples[i].id;//koitazoume ton sumArray gia na brw to offset pou 8a balw to epomeno tuple
		reOrderedArray->tuples[sumArray->histArray[array->tuples[i].value%sumArray->histSize].point++].value=array->tuples[i].value;//thn deuterh fora pou bazoume to value kanw ++ gia na deixnei sthn epomenh kenh 8esh
	}
	return reOrderedArray;
}

unsigned int hash(int32_t x,int mod) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x%mod;
}
indexHT* initiliazeIndexHT(relation* reOrderedArray,int32_t chainNumSize)
{
	int32_t i;
	indexHT* indexht = malloc(sizeof(indexHT));
	indexht->bucketArray = malloc(bucketPosNum*sizeof(bucketNode));
	indexht->chainNode = malloc(chainNumSize*sizeof(chainNode));
	indexht->bucketSize = bucketPosNum;
	indexht->chainSize = chainNumSize;
	for(i=0;i<bucketPosNum;i++){
		indexht->bucketArray[i].lastChainPosition = -1;
	}
	for(i=0;i<indexht->chainSize;i++){
		indexht->chainNode[i].prevchainPosition = -1;
		indexht->chainNode[i].bucketPos = -1;
	}
	return indexht;
}
indexHT* createHashTable(relation* reOrderedArray,int32_t start,int32_t end){

	int32_t i;
	indexHT* indexht;
	indexht = initiliazeIndexHT(reOrderedArray,end-start+1);

	/*for(i=0;i<reOrderedArray->num_of_tuples;i++)
	{
		printf("reOrdered[%d]->id=%d  ->value=%d\n",i,reOrderedArray->tuples[i].id,reOrderedArray->tuples[i].value);
	}*/

	for(i=start;i<=end;i++){
		if(indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition == -1)//elegxos an to h 8esh tou bucket einai kenh
		{
			indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition = i-start;		//i-start gia na paroume thn swsth thesh apo to reorder
			indexht->chainNode[i-start].bucketPos=i;
		}
		else
		{
			//printf("mpika\n");
			indexht->chainNode[i-start].prevchainPosition = indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition;
			indexht->chainNode[i-start].bucketPos = i;
			indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition = i-start;
		}
	}
	return indexht;
}
void deleteHashTable(indexHT **ht)
{
	free((*ht)->bucketArray);				/*diagrafh tou pinaka bucket*/
	free((*ht)->chainNode);					/*diagrafh tou pinaka chain*/
	free((*ht));							/*diagrafh olhs ths domhs*/
}
void compareRelations(indexHT *ht,relation *array,int32_t start,int32_t end,relation *hashedArray,resultList *resList,int32_t fromArray){
	int32_t i,chain_offset,j,bucket_offset;

	for(i=start;i<=end;i++){
		if(ht->bucketArray[hash(array->tuples[i].value,bucketPosNum)].lastChainPosition == -1){//an einai -1 tote den exoume match opote to agnwoume
			continue;
		}
		else{
			chain_offset=ht->bucketArray[hash(array->tuples[i].value,bucketPosNum)].lastChainPosition;//pairnw ws offset thn teleutaia 8esh tou chain
			bucket_offset=ht->chainNode[chain_offset].bucketPos;
			while(1){
				//printf("i=%d num_of_tuples=%d offset=%d\n",i,hashedArray->num_of_tuples,offset);
				if(array->tuples[i].value==hashedArray->tuples[bucket_offset].value){//elegxoume an exoun idio value
					//printf("RESULT = %d\n\n",hashedArray->tuples[bucket_offset].value);
					insertResult(resList,hashedArray->tuples[bucket_offset].id,array->tuples[i].id,fromArray);
				}
				for(j=0;j<ht->chainSize;j++)
				{
					//printf("Chain[%d]=%d -->offset=%d\n",j,ht->chainNode[j].prevchainPosition,offset);
				}

				chain_offset=ht->chainNode[chain_offset].prevchainPosition;//to neo offset einai apo to prevchainPosition
				if(chain_offset==-1)break;//den exei alla stoixeia na doume
				bucket_offset = ht->chainNode[chain_offset].bucketPos;
			}
		}
	}
}

resultList *initializeResultList(void){
	resultList *list=malloc(sizeof(resultList));
	list->start=NULL;
	list->end=NULL;
	list->numberOfNodes=0;
	return list;
}
void insertResult(resultList *list,uint32_t id1,uint32_t id2,int32_t fromArray){
	int32_t numberoftuples=(1024*1024)/sizeof(rowResult);
	//printf("numberOfResults=%d  %ld\n",numberoftuples,sizeof(rowResult));
	if(list->end==NULL){//kenh lista
		list->start=malloc(sizeof(resultNode));
		list->end=list->start;
		list->numberOfNodes=1;
		list->numberOfResults=1;

		list->start->row_Array=malloc(numberoftuples*sizeof(rowResult));
		list->start->next=NULL;
		list->start->rowSize=1;

		if(fromArray == 0){
			list->start->row_Array[0].idR=id1;
			list->start->row_Array[0].idS=id2;
		}else{
			list->start->row_Array[0].idR=id2;
			list->start->row_Array[0].idS=id1;
		}
	}
	else{
		list->numberOfResults++;
		if( numberoftuples > list->end->rowSize ){//exei xwro

			if(fromArray == 0){
				list->end->row_Array[list->end->rowSize].idR=id1;
				list->end->row_Array[list->end->rowSize].idS=id2;
			}else{
				list->end->row_Array[list->end->rowSize].idR=id2;
				list->end->row_Array[list->end->rowSize].idS=id1;
			}
			list->end->rowSize++;
		}
		else{//ftiaxnw neo kombo
			list->end->next=malloc(sizeof(resultNode));
			list->end=list->end->next;
			list->numberOfNodes++;

			list->end->row_Array=malloc(numberoftuples*sizeof(rowResult));
			list->end->next=NULL;
			list->end->rowSize=1;

			if(fromArray == 0){
				list->end->row_Array[0].idR=id1;
				list->end->row_Array[0].idS=id2;
			}else{
				list->end->row_Array[0].idR=id2;
				list->end->row_Array[0].idS=id1;
			}
		}
	}
}
void printResults(resultList *list){
	resultNode *temp;
	temp =list->start;
	printf("\nRowidR\t\tRowidS\n");
	while(temp!=NULL){
		for(int i=0;i<temp->rowSize;i++){
			printf("%d\t\t%d\n",temp->row_Array[i].idR,temp->row_Array[i].idS);
		}
		temp=temp->next;
	}
	printf("\n%d Results\n\n",list->numberOfResults);
}

void createHT_CompareBuckets(resultList* resList,hist *histSumArrayR,hist *histSumArrayS,relation *RR, relation*RS,int32_t size_A,int32_t size_B)
{
	indexHT* ht;
	int32_t startR=0,endR,startS=0,endS,counter = 0,cnt=0;
	while(1)
	{
		if(counter+1 > histSumArrayR->histSize)break;		//counter+1 giati einai h thesh gia to end ,dld an h thesh pou tha einai to end einai ektos oriwn break//
		startR = histSumArrayR->histArray[counter].count;
		startS = histSumArrayS->histArray[counter].count;
		if(counter+1==histSumArrayR->histSize){
			endR=size_A-1;
			endS=size_B-1;
		}else{
		endR = histSumArrayR->histArray[counter+1].count-1;
		endS = histSumArrayS->histArray[counter+1].count-1;
		}

		counter++;
		if(endR+1 == startR || endS+1 == startS) continue;  //elegxos an kapoio einai keno , opote den bgazei apotelesma
		cnt++;
		if((endR - startR) >= (endS - startS))//kanoume hash to pio mikro bucket
		{
			ht=createHashTable(RS,startS,endS);
			compareRelations(ht,RR,startR,endR,RS,resList,0);		// 0 -> hashedRs
			deleteHashTable(&ht);
		}
		else
		{
			ht=createHashTable(RR,startR,endR);
			compareRelations(ht,RS,startS,endS,RR,resList,1);		// 1 -> hashedRr
			deleteHashTable(&ht);
		}
	}
}
resultList* RadixHashJoin(relation *relR,relation *relS,int32_t size_A,int32_t size_B){


	hist *histSumArrayR,*histSumArrayS;
	relation *RS,*RR;
	resultList *resList;

	histSumArrayS=createSumHistArray(createHistArray(&relS));//dhmiourgia hist sum arrays
	histSumArrayR=createSumHistArray(createHistArray(&relR));

	RR=createReOrderedArray(relR,histSumArrayR);//dhmiourgia reordered array
	RS=createReOrderedArray(relS,histSumArrayS);

	resList=initializeResultList();


	createHT_CompareBuckets(resList,histSumArrayR,histSumArrayS,RR,RS,size_A,size_B);

	free(histSumArrayR->histArray);
	free(histSumArrayR);
	free(histSumArrayS->histArray);
	free(histSumArrayS);

	free(RS->tuples);
	free(RS);
	free(RR->tuples);
	free(RR);

	free(relS->tuples);
	free(relS);
	free(relR->tuples);
	free(relR);

	return resList;

}

void writeFile(uint32_t size_A,uint32_t size_B){
	FILE *fp;
	fp=fopen("input-files/input.txt","w");
	fprintf(fp,"%d %d\n",size_A,size_B);
	for(int32_t i=0;i<size_A;i++)
	{
		fprintf(fp,"%d,",i);
	}
	fprintf(fp,"\n");
	for(int32_t i=0;i<size_B;i++)
	{
		fprintf(fp,"%d,",i+2);
	}
	fclose(fp);
}

void readFile(int32_t A[],uint32_t *size_A,int32_t B[],uint32_t *size_B){
	FILE *fp;
	fp=fopen("input-files/input.txt","r");
	int32_t el1,el2;
	fscanf(fp,"%d %d",size_A,size_B);

	for(int32_t i=0;i<*size_A;i++){
		fscanf(fp,"%d,",&el1);
		A[i]=el1;
	}
	for(int32_t i=0;i<*size_B;i++){
		fscanf(fp,"%d,",&el2);
		B[i]=el2;
	}
	fclose(fp);
}

void deleteResultList(resultList * reslist){
	resultNode *temp,*temp2;
	temp=reslist->start;

	while(temp!=NULL){
		free(temp->row_Array);
		temp2=temp;
		temp=temp->next;
		free(temp2);
	}

	free(reslist);
}



















