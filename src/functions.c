#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "../header-files/functions.h"
#define bucketPosNum 15
#define N 3

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
		indexht->chainNode[i].bucketPos = i;
	}
	return indexht;
}
indexHT* createHashTable(relation* reOrderedArray,int32_t start,int32_t end){

	int32_t i;
	indexHT* indexht;
	indexht = initiliazeIndexHT(reOrderedArray,end-start+1);
	for(i=start;i<=end;i++){
		if(indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition == -1)//elegxos an to h 8esh tou bucket einai kenh
		{
			indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition = i;
		}
		else
		{
			indexht->chainNode[i].prevchainPosition = indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition;
			indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition = indexht->chainNode[i].bucketPos;
		}
	}
	return indexht;
}
void deleteHashTable(indexHT **ht)
{
	free((*ht)->bucketArray);				/*diagrafh tou pinaka bucket*/
	free((*ht)->chainNode);					/*diagrafh tou pinaka chain*/
	free((*ht));						/*diagrafh olhs ths domhs*/
}
void compareRelations(indexHT *ht,relation *array,int32_t start,int32_t end,relation *hashedArray){
	int32_t i,offset;

	for(i=start;i<=end;i++){
		if(ht->bucketArray[hash(array->tuples[i].value,bucketPosNum)].lastChainPosition == -1){//an einai -1 tote den exoume match opote to agnwoume
			continue;
		}
		else{
			offset=ht->bucketArray[hash(array->tuples[i].value,bucketPosNum)].lastChainPosition;//pairnw ws offset thn teleutaia 8esh tou chain
			while(1){
				if(offset==-1)break;//den exei alla stoixeia na doume
				if(array->tuples[i].value==hashedArray->tuples[offset].value){//elegxoume an exoun idio value
					printf("RESULT = %d\n",hashedArray->tuples[offset].value);
				}
				offset=ht->chainNode[offset].prevchainPosition;//to neo offset einai apo to prevchainPosition
			}
		}
	}
}



