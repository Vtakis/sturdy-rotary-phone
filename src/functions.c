#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "../header-files/functions.h"
#include "../header-files/Job_Scheduler.h"
#define bucketPosNum 50
#define N 12
#define THREADS_NUM 4
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>

pthread_mutex_t *reOrdered_mutex;
void createRelationsUT(int32_t A[],uint32_t size_A,int32_t B[],uint32_t size_B,oneColumnRelation **S,oneColumnRelation **R){
	int32_t i;
	*S=malloc(sizeof(oneColumnRelation));
	(*S)->num_of_tuples=size_A;
	(*S)->tuples=malloc(size_A*sizeof(tuple));
	for(i=0;i<size_A;i++){
		(*S)->tuples[i].id=i+1;
		(*S)->tuples[i].value=A[i];
	}
	*R=malloc(sizeof(oneColumnRelation));
	(*R)->num_of_tuples=size_B;
	(*R)->tuples=malloc(size_B*sizeof(tuple));
	for(i=0;i<size_B;i++){
		(*R)->tuples[i].id=i+1;
		(*R)->tuples[i].value=B[i];
	}
}
hist* createHistArray(oneColumnRelation **rel,int start,int end){
	int32_t i;
	hist *Hist;
	Hist=malloc(sizeof(hist));
	Hist->histSize=pow(2,N);
	Hist->histArray=malloc(Hist->histSize*sizeof(histNode));
	for(i=0;i<Hist->histSize;i++){
		Hist->histArray[i].count=0;
		Hist->histArray[i].point=0;
	}
	for(i=start;i<end;i++){
		Hist->histArray[(*rel)->tuples[i].value%Hist->histSize].count++;
	}
	return Hist;
}
hist* createSumHistArray(hist *array){

	int32_t i,nextBucket=0;
	hist *Hist;
	Hist=malloc(sizeof(hist));
	Hist->histSize=pow(2,N);
	Hist->histArray=malloc(Hist->histSize*sizeof(histNode));
	for(i=0;i<Hist->histSize;i++){
		Hist->histArray[i].count=0;
		Hist->histArray[i].point=0;
	}
	for(i=0;i<array->histSize;i++){
		if(i==0){//1o bucket - vriskoume ousiastika pou 8a arxizei to epomeno bucket
			nextBucket=array->histArray[i].count;
			Hist->histArray[i].count=0;
			Hist->histArray[i].point=0;
		}
		else{
			Hist->histArray[i].count=nextBucket;//pou arxizei to trexon bucket
			Hist->histArray[i].point=nextBucket;
			nextBucket+=array->histArray[i].count;//pou 8a arxizei to epomeno bucket
		}
	}
	free(array->histArray);
	free(array);
	return Hist;
}
void createReOrderedArray(oneColumnRelation *array,hist *sumArray,int start,int end,oneColumnRelation *reOrderedArray){
	int32_t i;
	for(i=start;i<end;i++){//to point mou dixnei pou prepei na balw to stoixeio
		pthread_mutex_lock(&(reOrdered_mutex[array->tuples[i].value%sumArray->histSize]));
			reOrderedArray->tuples[sumArray->histArray[array->tuples[i].value%sumArray->histSize].point].id=array->tuples[i].id;//koitazoume ton sumArray gia na brw to offset pou 8a balw to epomeno tuple
			reOrderedArray->tuples[sumArray->histArray[array->tuples[i].value%sumArray->histSize].point++].value=array->tuples[i].value;//thn deuterh fora pou bazoume to value kanw ++ gia na deixnei sthn epomenh kenh 8esh
		pthread_mutex_unlock(&(reOrdered_mutex[array->tuples[i].value%sumArray->histSize]));
	}
}
unsigned int hash(int32_t x,int mod) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x%mod;
}
indexHT* initiliazeIndexHT(oneColumnRelation* reOrderedArray,int32_t chainNumSize)
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
indexHT* createHashTable(oneColumnRelation* reOrderedArray,int32_t start,int32_t end){

	int32_t i;
	indexHT* indexht;
	indexht = initiliazeIndexHT(reOrderedArray,end-start+1);
	for(i=start;i<=end;i++){
		if(indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition == -1)//elegxos an to h 8esh tou bucket einai kenh
		{
			indexht->bucketArray[hash(reOrderedArray->tuples[i].value,bucketPosNum)].lastChainPosition = i-start;		//i-start gia na paroume thn swsth thesh apo to reorder
			indexht->chainNode[i-start].bucketPos=i;
		}
		else
		{
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
void compareRelations(indexHT *ht,oneColumnRelation *array,int32_t start,int32_t end,oneColumnRelation *hashedArray,resultList *resList,int32_t fromArray){
	int32_t i,chain_offset,bucket_offset;

	for(i=start;i<=end;i++){
		if(ht->bucketArray[hash(array->tuples[i].value,bucketPosNum)].lastChainPosition == -1){//an einai -1 tote den exoume match opote to agnwoume
			continue;
		}
		else{
			chain_offset=ht->bucketArray[hash(array->tuples[i].value,bucketPosNum)].lastChainPosition;//pairnw ws offset thn teleutaia 8esh tou chain
			bucket_offset=ht->chainNode[chain_offset].bucketPos;
			while(1){
				if(array->tuples[i].value==hashedArray->tuples[bucket_offset].value){//elegxoume an exoun idio value
					insertResult(resList,hashedArray->tuples[bucket_offset].id,array->tuples[i].id,fromArray);
				}
				chain_offset=ht->chainNode[chain_offset].prevchainPosition;//to neo offset einai apo to prevchainPosition
				if(chain_offset==-1)break;						//den exei alla stoixeia na doume
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
	list->numberOfResults=0;
	return list;
}
pthread_mutex_t insert_mutex = PTHREAD_MUTEX_INITIALIZER;
void insertResult(resultList *list,uint32_t id1,uint32_t id2,int32_t fromArray){
	int32_t numberoftuples=(128*1024)/sizeof(rowResult);
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
		}else if(fromArray==1){
			list->start->row_Array[0].idR=id2;
			list->start->row_Array[0].idS=id1;
		}else{
			list->start->row_Array[0].idR=id1;
		}
	}
	else{
		list->numberOfResults++;
		if( numberoftuples > list->end->rowSize ){//exei xwro

			if(fromArray == 0){
				list->end->row_Array[list->end->rowSize].idR=id1;
				list->end->row_Array[list->end->rowSize].idS=id2;
			}else if(fromArray==1){
				list->end->row_Array[list->end->rowSize].idR=id2;
				list->end->row_Array[list->end->rowSize].idS=id1;
			}else{
				list->start->row_Array[0].idR=id1;
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
			}else if(fromArray==1){
				list->end->row_Array[0].idR=id2;
				list->end->row_Array[0].idS=id1;
			}else{
				list->start->row_Array[0].idR=id1;
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
void createHT_CompareBuckets(resultList* resList,hist *histSumArrayR,hist *histSumArrayS,oneColumnRelation *RR, oneColumnRelation *RS,int32_t size_A,int32_t size_B)
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
resultList* RadixHashJoin(oneColumnRelation *relR,oneColumnRelation *relS){

	Job_Scheduler* job_scheduler;
	job_scheduler=initialize_scheduler(THREADS_NUM,relR,relS);
	hist *histSumArrayR,*histSumArrayS;
	oneColumnRelation *RS,*RR;

	Job job;
	int segSize=relR->num_of_tuples/THREADS_NUM;
	for(int i=0;i<THREADS_NUM;i++){
		job=initializeJob("hist");
		job.histjob.start=i*segSize;
		job.histjob.end=job.histjob.start+segSize;
		job.histjob.rel='R';
		if(i==THREADS_NUM-1)
			job.histjob.end+=relR->num_of_tuples%THREADS_NUM;
		submit_Job(job_scheduler,job);
	}
	segSize=relS->num_of_tuples/THREADS_NUM;
	for(int i=0;i<THREADS_NUM;i++){
		job=initializeJob("hist");
		job.histjob.start=i*segSize;
		job.histjob.end=job.histjob.start+segSize;
		job.histjob.rel='S';
		if(i==THREADS_NUM-1)
			job.histjob.end+=relS->num_of_tuples%THREADS_NUM;
		submit_Job(job_scheduler,job);
	}

	hist *histR;
	hist *histS;
	histR=malloc(sizeof(hist));
	histS=malloc(sizeof(hist));
	histR->histSize=pow(2,N);
	histS->histSize=histR->histSize;
	histR->histArray=malloc(histR->histSize*sizeof(histNode));
	histS->histArray=malloc(histS->histSize*sizeof(histNode));
	for(int f=0;f<histR->histSize;f++){
		histR->histArray[f].count=0;
		histS->histArray[f].count=0;
	}
	sleep_producer(job_scheduler,0);
	for(int c=0;c<THREADS_NUM;c++){
		for(int q=0;q<histR->histSize;q++){
			histR->histArray[q].count+=job_scheduler->shared_data.histArrayR[c]->histArray[q].count;
			histS->histArray[q].count+=job_scheduler->shared_data.histArrayS[c]->histArray[q].count;
		}
	}
	histSumArrayR=createSumHistArray(histR);
	histSumArrayS=createSumHistArray(histS);//dhmiourgia hist sum arrays

	for(int i=0;i<job_scheduler->execution_threads;i++)
	{
		free(job_scheduler->shared_data.histArrayR[i]->histArray);
		free(job_scheduler->shared_data.histArrayS[i]->histArray);
		free(job_scheduler->shared_data.histArrayR[i]);
		free(job_scheduler->shared_data.histArrayS[i]);
	}
	free(job_scheduler->shared_data.histArrayR);
	free(job_scheduler->shared_data.histArrayS);

	job_scheduler->shared_data.histArrayR=&histSumArrayR;
	job_scheduler->shared_data.histArrayS=&histSumArrayS;
	reOrdered_mutex=malloc(histSumArrayR->histSize*sizeof(pthread_mutex_t));//dimiourgeia mutex gia kathe thesi tou pinaka

	for(int j=0;j<histSumArrayR->histSize;j++)
	{
		pthread_mutex_init(&(reOrdered_mutex[j]),NULL);//arxikopoiisi ton mutex gia kathe thesi tou pinaka
	}
	RR=malloc(sizeof(oneColumnRelation));
	RR->num_of_tuples=relR->num_of_tuples;
	RR->tuples=malloc(relR->num_of_tuples*sizeof(tuple));

	RS=malloc(sizeof(oneColumnRelation));
	RS->num_of_tuples=relS->num_of_tuples;
	RS->tuples=malloc(relS->num_of_tuples*sizeof(tuple));

	job_scheduler->shared_data.RR=&RR;
	job_scheduler->shared_data.RS=&RS;

	segSize=relR->num_of_tuples/THREADS_NUM;
	for(int i=0;i<THREADS_NUM;i++){
		job=initializeJob("partition");
		job.partitionjob.start=i*segSize;
		job.partitionjob.end=job.partitionjob.start+segSize;
		job.partitionjob.rel='R';
		if(i==THREADS_NUM-1)
			job.partitionjob.end+=relR->num_of_tuples%THREADS_NUM;
		submit_Job(job_scheduler,job);
	}

	segSize=relS->num_of_tuples/THREADS_NUM;
	for(int i=0;i<THREADS_NUM;i++){
		job=initializeJob("partition");
		job.partitionjob.start=i*segSize;
		job.partitionjob.end=job.partitionjob.start+segSize;
		job.partitionjob.rel='S';
		if(i==THREADS_NUM-1)
			job.partitionjob.end+=relS->num_of_tuples%THREADS_NUM;
		submit_Job(job_scheduler,job);
	}

	sleep_producer(job_scheduler,0);
	for(int j=0;j<histSumArrayR->histSize;j++)
	{
		pthread_mutex_destroy(&(reOrdered_mutex[j]));//arxikopoiisi ton mutex gia kathe thesi tou pinaka
	}
	free(reOrdered_mutex);

	resultList **resList;
	resList = malloc(histSumArrayR->histSize*sizeof(resultList*));
	resultList *returnList;

	int32_t startR=0,endR,startS=0,endS,counter = 0,cnt=0;
	job_scheduler->shared_data.resList=resList;

	while(1)
	{
		if(counter+1 > histSumArrayR->histSize)break;		//counter+1 giati einai h thesh gia to end ,dld an h thesh pou tha einai to end einai ektos oriwn break//
		startR = histSumArrayR->histArray[counter].count;
		startS = histSumArrayS->histArray[counter].count;
		if(counter+1==histSumArrayR->histSize){
			endR=relR->num_of_tuples-1;
			endS=relS->num_of_tuples-1;
		}else{
			endR = histSumArrayR->histArray[counter+1].count-1;
			endS = histSumArrayS->histArray[counter+1].count-1;
		}
		counter++;
		if(endR+1 == startR || endS+1 == startS) continue;  //elegxos an kapoio einai keno , opote den bgazei apotelesma
		cnt++;
		if((endR - startR) >= (endS - startS))//kanoume hash to pio mikro bucket
		{
			job=initializeJob("join");
            job.joinjob.startR=startR;
            job.joinjob.endR=endR;
            job.joinjob.startS=startS;
            job.joinjob.endS=endS;
            job.joinjob.rel='S';
            submit_Job(job_scheduler,job);
		}
		else
		{
            job=initializeJob("join");
            job.joinjob.startR=startR;
            job.joinjob.endR=endR;
            job.joinjob.startS=startS;
            job.joinjob.endS=endS;
            job.joinjob.rel='R';
            submit_Job(job_scheduler,job);
		}
	}
	sleep_producer(job_scheduler,1);
	int first=0;
	for(int i=1;i<cnt;i++)
	{
			if(resList==NULL)
			{
				continue;
			}
			if(resList[i]==NULL)
			{
				continue;
			}
			if(resList[i]->start==NULL || resList[i]->end==NULL)
			{
				free(resList[i]);
				continue;
			}
			if(resList[first]->end==NULL)
			{
				free(resList[first]);
				resList[first]=resList[i];
				continue;
			}
			resList[first]->end->next=resList[i]->start;
			resList[first]->end = resList[i]->end;
			resList[first]->numberOfNodes+=resList[i]->numberOfNodes;
			resList[first]->numberOfResults+=resList[i]->numberOfResults;
			free(resList[i]);
	}
	if(cnt==0)
	{
		resList[first]=initializeResultList();
	}
	returnList=resList[first];
	free(resList);

	free(histSumArrayR->histArray);
	free(histSumArrayR);
	free(histSumArrayS->histArray);
	free(histSumArrayS);

	free(RS->tuples);
	free(RS);
	free(RR->tuples);
	free(RR);

	return returnList;
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
		fprintf(fp,"%d,",size_B-i);
	}
	fclose(fp);
}
void readFile(int32_t A[],uint32_t *size_A,int32_t B[],uint32_t *size_B){
	FILE *fp;
	fp=fopen("input-files/input.txt","r");
	int32_t el1,el2;
	if(fscanf(fp,"%d %d",size_A,size_B)==EOF)
	{
		perror("fscanf:");
		exit(0);
	}

	for(int32_t i=0;i<*size_A;i++){
		if(fscanf(fp,"%d,",&el1)==EOF)
		{
			perror("fscanf:");
			exit(0);
		}
		A[i]=el1;
	}
	for(int32_t i=0;i<*size_B;i++){
		if(fscanf(fp,"%d,",&el2))
		{
			perror("fscanf:");
			exit(0);
		}
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
char **readRelations(int *number_of_files){//
	char **relation,*token,buffer[4096];
	if(read(STDIN_FILENO,buffer,sizeof(buffer))==-1)//diavazoume apo to harness ta relations
	{
		perror("Read :");
		exit(0);
	}
	token=strtok(buffer,"D");
	token = strtok(buffer,"\n");
	relation=malloc(sizeof(char*));
	relation[0]=malloc(strlen(token)+1);
	strcpy(relation[0],token);

	while(token!=NULL){ 
		token=strtok(NULL,"\n");
		if(token==NULL)
			break;
		relation=realloc(relation,(*number_of_files+1)*sizeof(char*));
		relation[*number_of_files]=malloc(strlen(token)+1);
		strcpy(relation[(*number_of_files)++],token);
	}
	return relation; //epistrefoume ena pinaka apo string pou exei ta onomata twn relations
}
void createRelations(int number_of_files,multiColumnRelation **relationArray,all_stats **statsArray,char **relation){

	int i=0,j,k;
	FILE *fp;

	(*statsArray)->rels=number_of_files;
	(*statsArray)->cols=malloc(number_of_files*sizeof(uint64_t));
	(*statsArray)->array_with_stats=malloc(number_of_files*sizeof(stats *));


	for(i=0;i<number_of_files;i++){
		fp=fopen(relation[i],"rb"); //me vash ton pinaka me ta onomata twn relations, anoigoume to ka8e relation
		if(fp==NULL){
			printf("error\n");
			return;
		}
		uint64_t temp;
		if(fread(&temp,sizeof(uint64_t),1,fp)<=0)
		{
			perror("Fread :");
			exit(0);
		}
		(*relationArray)[i].rowCount=temp;
		if(fread(&temp,sizeof(uint64_t),1,fp)<=0)
		{
			perror("Fread :");
			exit(0);
		}
		(*relationArray)[i].colCount=temp;
		(*relationArray)[i].columns = malloc((*relationArray)[i].colCount*sizeof(oneColumnRelation));
		(*statsArray)->cols[i]=(*relationArray)[i].colCount;
		(*statsArray)->array_with_stats[i]=malloc((*relationArray)[i].colCount*sizeof(stats));//

		for(j=0;j<(*relationArray)[i].colCount;j++){
			(*statsArray)->array_with_stats[i][j].f=(*relationArray)[i].rowCount;//
			(*relationArray)[i].columns[j].tuples=malloc((*relationArray)[i].rowCount*sizeof(tuple));//mporei na prepei na bgoun eksw

			for(k=0;k<(*relationArray)[i].rowCount;k++){
				if(fread(&temp,sizeof(uint64_t),1,fp)<=0)
				{
					perror("Fread :");
					exit(0);
				}
				(*relationArray)[i].columns[j].tuples[k].value=temp;
				(*relationArray)[i].columns[j].tuples[k].id=k;

				if(k==0){
					(*statsArray)->array_with_stats[i][j].l=temp;
					(*statsArray)->array_with_stats[i][j].u=temp;
				}
				else{
					if((*statsArray)->array_with_stats[i][j].l>temp){
						(*statsArray)->array_with_stats[i][j].l=temp;
					}

					if((*statsArray)->array_with_stats[i][j].u<temp){
						(*statsArray)->array_with_stats[i][j].u=temp;
					}
				}
			}
			int d=0;
			if((*statsArray)->array_with_stats[i][j].u-(*statsArray)->array_with_stats[i][j].l+1>500000){
				int sizeofbool=500000;
				bool distArray[sizeofbool];
				for(int qaz=0;qaz<sizeofbool;qaz++){
					distArray[qaz]=false;
				}
				for(k=0;k<(*relationArray)[i].rowCount;k++){
					if(distArray[((*relationArray)[i].columns[j].tuples[k].value-(*statsArray)->array_with_stats[i][j].l)%sizeofbool]==false){
						distArray[((*relationArray)[i].columns[j].tuples[k].value-(*statsArray)->array_with_stats[i][j].l)%sizeofbool]=true;
						d++;
					}
				}
			}
			else{
				int sizeofbool=(*statsArray)->array_with_stats[i][j].u-(*statsArray)->array_with_stats[i][j].l+1;
				bool distArray[sizeofbool];
				for(int qaz=0;qaz<sizeofbool;qaz++){
					distArray[qaz]=false;
				}
				for(k=0;k<(*relationArray)[i].rowCount;k++){
					if(distArray[(*relationArray)[i].columns[j].tuples[k].value-(*statsArray)->array_with_stats[i][j].l]==false){
						distArray[(*relationArray)[i].columns[j].tuples[k].value-(*statsArray)->array_with_stats[i][j].l]=true;
						d++;
					}
				}
			}
			(*statsArray)->array_with_stats[i][j].d=d;
		}
		fclose(fp);
	}
	return;
}
void executeBatches(multiColumnRelation *relationArray,all_stats *statsArray){
	char buffer[4096];
	char *token,*queryString;
	while(1)
	{
		ssize_t bytes = read(STDIN_FILENO,buffer,sizeof(buffer)); //diavasma batch apo harness
		if(bytes==0)		//to programma den diavase allo batch ara termatizei
		{
			break;
		}
		int numBatchQueries=1,cur=0;
		token=strtok(buffer,"\n");

		char **execQueries;
		execQueries=malloc(sizeof(char*));
		execQueries[0]=malloc(strlen(token)+1);
		strcpy(execQueries[0],token);

		while(token!=NULL){
			token=strtok(NULL,"\n");
			if(token==NULL || strcmp(token,"F")==0)
				break;
			execQueries=realloc(execQueries,(numBatchQueries+1)*sizeof(char*));
			execQueries[numBatchQueries]=malloc(strlen(token)+1);
			strcpy(execQueries[numBatchQueries++],token);
		}
		int teamCounter=1,phrase_size=100,middleResultsSize=100,i,columnIndx,relationId,relationIndex,flag=0,middleResultsCounter=0,j,leftRelationId,rightRelationId,leftRelationIndx,leftTeam=0,rightTeam=0,rightRelationIndx,leftColumnIndx,rightColumnIndx;
		queryString = malloc(phrase_size*sizeof(char));
		middleResults *middleResArray=malloc(middleResultsSize*sizeof(middleResults));
		for(i=0;i<middleResultsSize;i++){
			middleResArray[i].team=0;
			middleResArray[i].relation=-1;
			middleResArray[i].rowIdsNum=0;
			middleResArray[i].fromArray=-1;
			middleResArray[i].relation_id=-1;
		}
		queryDataIndex *data;
		int y=1;
		for(cur=0;cur<numBatchQueries;cur++){ //ekteloume ta queries pou exei to batch
				oneColumnRelation *column;
				y++;
				data=analyzeQuery(execQueries[cur]);
				//ftiaxnw enan mikrotero pinaka me ta stastika pou xreiazomai , mporei na exw kapoio relation 2 fores
				all_stats *temp_stats;
				temp_stats=malloc(sizeof(all_stats));
				temp_stats->rels=data->numRelQuery;
				temp_stats->cols=malloc(temp_stats->rels*sizeof(uint64_t));
				temp_stats->array_with_stats=malloc(temp_stats->rels*sizeof(stats *));
				for(i=0;i<temp_stats->rels;i++){
					temp_stats->cols[i]=statsArray->cols[data->QueryRelArray[i]];

					temp_stats->array_with_stats[i]=malloc(temp_stats->cols[i]*sizeof(stats));
					for(j=0;j<temp_stats->cols[i];j++){
						temp_stats->array_with_stats[i][j].l=statsArray->array_with_stats[data->QueryRelArray[i]][j].l;
						temp_stats->array_with_stats[i][j].u=statsArray->array_with_stats[data->QueryRelArray[i]][j].u;
						temp_stats->array_with_stats[i][j].f=statsArray->array_with_stats[data->QueryRelArray[i]][j].f;
						temp_stats->array_with_stats[i][j].d=statsArray->array_with_stats[data->QueryRelArray[i]][j].d;
					}
				}

				if(data->numPredFilter>0)				//kanoume prwta tis prakseis me arithmous
				{
					for(i=0;i<data->numPredFilter;i++)		//apo 0 mexri ton arithmo twn filtrwn pou uparxoun//
					{
						relationId = data->predFilterArray[i].relColumn->rel;
						columnIndx= data->predFilterArray[i].relColumn->col;
						relationIndex = data->QueryRelArray[relationId];
						flag=0;
						if(middleResultsCounter>0)			//exoume endiamesa apotelesmata
						{
							for(j=0;j<middleResultsCounter;j++)			//trexoume ton pinaka me ta relations kai psaxnoume an uparxei antistoixia//
							{
								if(middleResArray[j].relation==relationIndex)
								{
									column=setColumnFromMiddleArray(middleResArray,relationIndex,columnIndx,j,relationArray);

									free(middleResArray[j].rowIds);
									int team =middleResArray[j].team;
									middleResArray[j]=executeFilter(column,data->predFilterArray[i].value,data->predFilterArray[i].typeOperation,relationIndex,temp_stats,relationId,columnIndx);
									middleResArray[j].team=team;
									middleResArray[j].relation_id=data->predFilterArray[i].relColumn->rel;

									flag=1;
									free(column->tuples);
									free(column);
									break;
								}
							}
							if(flag==1)continue;
						}
						//edw tha erthei an den uparxei kapoio apotelesma apo to middleResults h an uparxoun apotelesmata sto middleResult pou den mas ikanopoioun.ara ftiaxnoume neo RowID sto middleResult
						column = setColumnFromFirstArray(relationArray,relationIndex,columnIndx);
						middleResArray[middleResultsCounter++]=executeFilter(column,data->predFilterArray[i].value,data->predFilterArray[i].typeOperation,relationIndex,temp_stats,relationId,columnIndx);
						middleResArray[middleResultsCounter-1].relation_id=data->predFilterArray[i].relColumn->rel;
						middleResArray[middleResultsCounter-1].team=teamCounter++;

						middleResArray[middleResultsCounter-1].fromArray=0;
						if(middleResArray[middleResultsCounter-1].relation==-1)
						{
							//free(middleResArray[middleResultsCounter-1].rowIds);
							middleResultsCounter--;
						}
						free(column->tuples);
						free(column);
					}

				}
				if(data->numPredJoinTwoRel>0)			//join 2 columns//
				{
					//kalw thn sun gia na dw thn seira
					int *seira;
					seira=JoinEnumeration(data,temp_stats);
					leftTeam=0;
					rightTeam=0;
					int indx=0;
					int leftColumnPosInMiddleArray=-1,rightColumnPosInMiddleArray=-1;

					for(i=0;i<data->numPredJoinTwoRel;i++){//
						indx=i;
						indx=seira[i];
						oneColumnRelation *leftColumn;
						oneColumnRelation *rightColumn;

						leftColumnPosInMiddleArray=-1;
						rightColumnPosInMiddleArray=-1;
						leftRelationId = data->twoRelationPredArray[indx].left->rel;					//pernoume ta stoixeia apo thn domh pou krataei ta predications data //
						leftColumnIndx =data->twoRelationPredArray[indx].left->col;
						rightRelationId = data->twoRelationPredArray[indx].right->rel;
						rightColumnIndx= data->twoRelationPredArray[indx].right->col;

						leftRelationIndx = data->QueryRelArray[leftRelationId];
						rightRelationIndx = data->QueryRelArray[rightRelationId];
						if(middleResultsCounter>0)			//exoume endiamesa apotelesmata
						{
							for(j=0;j<middleResultsCounter;j++)			//trexoume ton pinaka me ta relations kai psaxnoume an uparxei antistoixia//
							{
								if(middleResArray[j].relation==leftRelationIndx && middleResArray[j].relation_id==leftRelationId)
								{
									leftColumn=setColumnFromMiddleArray(middleResArray,leftRelationIndx,leftColumnIndx,j,relationArray);	//dhmiourgia aristerhs sthlhs apo to middle results
									leftColumnPosInMiddleArray=j;			//h thesh ths aristerhs sthlhs mesa ston middleArray
									middleResArray[j].fromArray=1;
									leftTeam=middleResArray[j].team;
								}
								if(middleResArray[j].relation==rightRelationIndx && middleResArray[j].relation_id==rightRelationId)
								{
									rightColumn=setColumnFromMiddleArray(middleResArray,rightRelationIndx,rightColumnIndx,j,relationArray);	//dhmiourgia deksias sthlhs apo to middle results
									rightColumnPosInMiddleArray=j;			//h thesh ths deksias sthlhs mesa ston middleArray
									middleResArray[j].fromArray=1;
									rightTeam=middleResArray[j].team;
								}
							}
						}
						if(leftColumnPosInMiddleArray==-1)
						{
							leftColumn = setColumnFromFirstArray(relationArray,leftRelationIndx,leftColumnIndx);		//dhmiourgia aristerhs sthlhs apo ton arxiko pinaka
							middleResArray[middleResultsCounter].fromArray=0;
						}

						if(rightColumnPosInMiddleArray==-1)		//an den uparxei mesa ston middle array pairnoume thn sthlh apo ton arxiko pinaka
						{
							rightColumn = setColumnFromFirstArray(relationArray,rightRelationIndx,rightColumnIndx);		//dhmiourgia deksias sthlhs apo ton arxiko pinaka
							middleResArray[middleResultsCounter].fromArray=0;
						}
						resultList *resultList1,*resultList2;
						if(leftRelationId==rightRelationId || (leftTeam==rightTeam && leftColumnPosInMiddleArray!=-1 && rightColumnPosInMiddleArray!=-1))			//JoinOneRelationArray
						{
							resultList1=sameRelationJoin(leftColumn,rightColumn);
							data->twoRelationPredArray[indx].selected=1;
							if(leftColumnPosInMiddleArray==-1)			//an den uparxei to left column ston middle array
							{
								middleResArray[middleResultsCounter].fromArray=0;
								setResultsToMiddleArray(resultList1,middleResArray,middleResultsCounter,0,middleResultsCounter);
								middleResArray[middleResultsCounter].relation=leftRelationIndx;

								middleResultsCounter++;
								middleResArray[middleResultsCounter-1].team=teamCounter++;
							}

							if(leftColumnPosInMiddleArray!=-1)			//an uparxei to left column ston middle array
							{
								setResultsToMiddleArray(resultList1,middleResArray,leftColumnPosInMiddleArray,0,middleResultsCounter);
								middleResArray[leftColumnPosInMiddleArray].relation=leftRelationIndx;
								changeRowIdNumOfTeam(middleResArray,middleResArray[leftColumnPosInMiddleArray].team,resultList1->numberOfResults,middleResultsCounter,middleResArray[leftColumnPosInMiddleArray].team);
							}
							deleteResultList(resultList1);
						}
						else// if(joinFlag==2)										//JointwoRelationArray
						{

							resultList2=RadixHashJoin(leftColumn,rightColumn);
							data->twoRelationPredArray[indx].selected=1;

							if(leftColumnPosInMiddleArray==-1 && rightColumnPosInMiddleArray==-1){	//ftiaxnoume neo team

								middleResArray[middleResultsCounter].relation=leftRelationIndx;
								middleResArray[middleResultsCounter].fromArray=0;
								setResultsToMiddleArray(resultList2,middleResArray,middleResultsCounter,0,middleResultsCounter);
								middleResArray[middleResultsCounter].relation_id=leftRelationId;
								middleResultsCounter++;

								middleResArray[middleResultsCounter].relation=rightRelationIndx;
								middleResArray[middleResultsCounter].fromArray=0;
								setResultsToMiddleArray(resultList2,middleResArray,middleResultsCounter,1,middleResultsCounter);
								middleResArray[middleResultsCounter].relation_id=rightRelationId;
								middleResultsCounter++;

								middleResArray[middleResultsCounter-1].team=teamCounter;
								middleResArray[middleResultsCounter-2].team=teamCounter++;
							}
							else if(leftColumnPosInMiddleArray==-1 && rightColumnPosInMiddleArray!=-1){ //an uparxei team gia right column
								middleResArray[middleResultsCounter].relation=leftRelationIndx;
								middleResArray[middleResultsCounter].fromArray=0;
								setResultsToMiddleArray(resultList2,middleResArray,middleResultsCounter,0,middleResultsCounter);
								middleResArray[middleResultsCounter].relation_id=leftRelationId;

								middleResultsCounter++;
								middleResArray[rightColumnPosInMiddleArray].relation=rightRelationIndx;
								setResultsToMiddleArray(resultList2,middleResArray,rightColumnPosInMiddleArray,1,middleResultsCounter);
								middleResArray[rightColumnPosInMiddleArray].relation_id=rightRelationId;

								int team;
								team=middleResArray[middleResultsCounter-1].team;
								middleResArray[middleResultsCounter-1].team = middleResArray[rightColumnPosInMiddleArray].team;
								changeRowIdNumOfTeam(middleResArray,team,resultList2->numberOfResults,middleResultsCounter,middleResArray[rightColumnPosInMiddleArray].team);
							}
							else if(leftColumnPosInMiddleArray!=-1 && rightColumnPosInMiddleArray==-1){ //an uparxei team gia left column

								middleResArray[leftColumnPosInMiddleArray].relation=leftRelationIndx;
								setResultsToMiddleArray(resultList2,middleResArray,leftColumnPosInMiddleArray,0,middleResultsCounter);
								middleResArray[leftColumnPosInMiddleArray].relation_id=leftRelationId;

								middleResArray[middleResultsCounter].relation=rightRelationIndx;
								middleResArray[middleResultsCounter].fromArray=0;
								setResultsToMiddleArray(resultList2,middleResArray,middleResultsCounter,1,middleResultsCounter);
								middleResArray[middleResultsCounter].relation_id=rightRelationId;

								middleResultsCounter++;
								int team=middleResArray[middleResultsCounter-1].team;
								middleResArray[middleResultsCounter-1].team = middleResArray[leftColumnPosInMiddleArray].team;
								changeRowIdNumOfTeam(middleResArray,team,resultList2->numberOfResults,middleResultsCounter,middleResArray[leftColumnPosInMiddleArray].team);

							}
							else{  //uparxei team kai gia ta 2 column --- psaxnoume gia mikrotero # of team
								middleResArray[leftColumnPosInMiddleArray].relation=leftRelationIndx;
								setResultsToMiddleArray(resultList2,middleResArray,leftColumnPosInMiddleArray,0,middleResultsCounter);
								middleResArray[leftColumnPosInMiddleArray].relation_id=leftRelationId;

								middleResArray[rightColumnPosInMiddleArray].relation=rightRelationIndx;
								setResultsToMiddleArray(resultList2,middleResArray,rightColumnPosInMiddleArray,1,middleResultsCounter);
								middleResArray[rightColumnPosInMiddleArray].relation_id=rightRelationId;

								int team;
								if(middleResArray[leftColumnPosInMiddleArray].team <= middleResArray[rightColumnPosInMiddleArray].team){
									for(j=0;j<middleResultsCounter;j++){
										if(middleResArray[j].team==middleResArray[rightColumnPosInMiddleArray].team){
											team=middleResArray[j].team;
											middleResArray[j].team=middleResArray[leftColumnPosInMiddleArray].team;
										}
									}
									changeRowIdNumOfTeam(middleResArray,team,resultList2->numberOfResults,middleResultsCounter,middleResArray[leftColumnPosInMiddleArray].team);
								}
								else{
									for(j=0;j<middleResultsCounter;j++){
										if(middleResArray[j].team==middleResArray[leftColumnPosInMiddleArray].team){
											team=middleResArray[j].team;
											middleResArray[j].team=middleResArray[rightColumnPosInMiddleArray].team;
										}
									}
									changeRowIdNumOfTeam(middleResArray,team,resultList2->numberOfResults,middleResultsCounter,middleResArray[rightColumnPosInMiddleArray].team);
								}
							}
							deleteResultList(resultList2);
						}
						free(leftColumn->tuples);
						free(rightColumn->tuples);
						free(leftColumn);
						free(rightColumn);
						leftColumnPosInMiddleArray=-1;
						rightColumnPosInMiddleArray=-1;
					}
					teamCounter=1;

					if(i==data->numPredJoinTwoRel)
					{
						if(data->numViewQuery>0)
						{
							char *answer=malloc(100*sizeof(char));
							memset(answer,0,100);
							for(i=0;i<data->numViewQuery;i++)
							{
								columnIndx = data->viewQueryArray[i].col;
								relationId =data->viewQueryArray[i].rel;
								relationIndex = data->QueryRelArray[relationId];
								for(j=0;j<middleResultsCounter;j++)
								{
									if(middleResArray[j].relation==relationIndex && middleResArray[j].relation_id==relationId)
									{

										int arrayIndx=j;
										long sum=0;
										for(int k=0;k<middleResArray[arrayIndx].rowIdsNum;k++)
										{
											int index=middleResArray[arrayIndx].rowIds[k];
											sum+=relationArray[relationIndex].columns[columnIndx].tuples[index].value;
										}
										if(sum==0)
										{
											if(write(STDOUT_FILENO,"NULL",strlen("NULL"))<=0)
											{
												perror("Write:");
												exit(0);
											}
										}
										else
										{
											char *str;
											str=malloc(100);
											sprintf(str,"%ld",sum);
											if(write(STDOUT_FILENO,str,strlen(str))<=0)
											{
												perror("Write:");
												exit(0);
											}
											memset(str,0,strlen(str)+1);
											free(str);

										}
										if(i!=data->numViewQuery-1)
										{
											if(write(STDOUT_FILENO," ",strlen(" "))<=0)
											{
												perror("Write:");
												exit(0);
											}
										}
									}
								}
							}
							if(write(STDOUT_FILENO,"\n",strlen("\n"))<=0)
							{
								perror("Write:");
								exit(0);
							}
							free(answer);
						}
					}
					for(j=0;j<middleResultsCounter;j++){
						if(middleResArray[j].relation!=-1)
							free(middleResArray[j].rowIds);
						middleResArray[j].relation=-1;
						middleResArray[j].rowIdsNum=0;
						middleResArray[j].team=0;
						middleResArray[j].fromArray=-1;
						middleResArray[j].relation_id=-1;
					}
					free(seira);
				}
				middleResultsCounter=0;
				for(int i=0;i<data->numPredJoinTwoRel;i++)
				{
					free(data->twoRelationPredArray[i].left);
					free(data->twoRelationPredArray[i].right);
				}
				free(data->twoRelationPredArray);
				free(data->QueryRelArray);
				free(data->viewQueryArray);
				for(int i=0;i<data->numPredFilter;i++)
				{
					free(data->predFilterArray[i].relColumn);
				}
				free(data->predFilterArray);
				free(data);
				for(i=0;i<temp_stats->rels;i++){
					free(temp_stats->array_with_stats[i]);
				}
				free(temp_stats->array_with_stats);
				free(temp_stats->cols);
				free(temp_stats);
		}
		free(queryString);
		free(middleResArray);
		for(cur=0;cur<numBatchQueries;cur++){
			free(execQueries[cur]);
		}
		free(execQueries);
	}
}

int checkIfOneRelationJoinExists(queryDataIndex* data,middleResults* middleResArray,int middleResultsCounter,int indx)
{
	int k,rightRelationId,leftRelationId,leftRelationIndx,rightRelationIndx,j,leftTeam=0,rightTeam=0,rightColumnPosInMiddleArray=-1,tempValue=-1,leftColumnPosInMiddleArray=-1;
	if(indx==data->numPredJoinTwoRel-1)
	{
		for(k=0;k<data->numPredJoinTwoRel;k++){
			if(data->twoRelationPredArray[k].selected==1)continue;
			else break;
		}
		return k;
	}
	for(k=0;k<data->numPredJoinTwoRel;k++){
		if(data->twoRelationPredArray[k].selected==1)continue;
		else if(tempValue==-1) tempValue=k;

		leftColumnPosInMiddleArray=-1;
		rightColumnPosInMiddleArray=-1;
		leftTeam=0;
		rightTeam=0;
		leftRelationId = data->twoRelationPredArray[k].left->rel;					//pernoume ta stoixeia apo thn domh pou krataei ta predications data //
		rightRelationId = data->twoRelationPredArray[k].right->rel;

		leftRelationIndx = data->QueryRelArray[leftRelationId];
		rightRelationIndx = data->QueryRelArray[rightRelationId];
		if(middleResultsCounter>0)			//exoume endiamesa apotelesmata
		{
			for(j=0;j<middleResultsCounter;j++)			//trexoume ton pinaka me ta relations kai psaxnoume an uparxei antistoixia//
			{
				if(middleResArray[j].relation==leftRelationIndx && middleResArray[j].relation_id==leftRelationId)
				{
					leftColumnPosInMiddleArray=j;			//h thesh ths aristerhs sthlhs mesa ston middleArray
					middleResArray[j].fromArray=1;
					leftTeam=middleResArray[j].team;
				}
				if(middleResArray[j].relation==rightRelationIndx && middleResArray[j].relation_id==rightRelationId)
				{
					rightColumnPosInMiddleArray=j;			//h thesh ths deksias sthlhs mesa ston middleArray
					middleResArray[j].fromArray=1;
					rightTeam=middleResArray[j].team;
				}
			}
		}
		if(leftRelationId==rightRelationId || (leftTeam==rightTeam && leftColumnPosInMiddleArray!=-1 && rightColumnPosInMiddleArray!=-1))
		{
			return k;
		}
	}
	return tempValue;
}
void changeRowIdNumOfTeam(middleResults *array,int team,int numRows,int countMiddleArray,int new_team){
	int i;
	for(i=0;i<countMiddleArray;i++){
		if(array[i].team==team){
			array[i].rowIdsNum=numRows;
			array[i].team=new_team;
		}
	}
}
void printMiddleArray(middleResults *array,int size)
{
	for(int i=0;i<size;i++)
	{
		printf("Relation[%d]-->%d  RelationID %d  Rows %d  Teams %d\n",i,array[i].relation,array[i].relation_id,array[i].rowIdsNum,array[i].team);
		for(int j=0;j<array[i].rowIdsNum;j++)
		{
			printf("Row[%d]=%d\n",j,array[i].rowIds[j]);
		}
		printf("\n");
	}
}
void setResultsToMiddleArray(resultList *list,middleResults *middleResultsArray,int index,int direction,int middleResultsCounter)
{
	resultNode *temp;
	int counter=0;
	temp =list->start;
	if(temp==NULL)
	{
		for(int j=0;j<middleResultsCounter;j++)
		{
			if(middleResultsArray[index].team==middleResultsArray[j].team)
			{
				middleResultsArray[j].rowIdsNum=0;
			}
		}
		if(middleResultsArray[index].relation_id!=-1)
		{
			free(middleResultsArray[index].rowIds);
		}
		middleResultsArray[index].rowIds=malloc(sizeof(int));
		return;
	}

	if(middleResultsArray[index].rowIdsNum==0){
		middleResultsArray[index].rowIds=malloc(list->numberOfResults*sizeof(int));
		middleResultsArray[index].rowIdsNum=list->numberOfResults;
	}
	if(middleResultsArray[index].rowIdsNum < list->numberOfResults){
		middleResultsArray[index].rowIds=realloc(middleResultsArray[index].rowIds,list->numberOfResults*sizeof(int));
	}
	middleResultsArray[index].rowIdsNum=list->numberOfResults;
	int teamSize=0;
	for(int j=0;j<middleResultsCounter;j++)
	{
		if(middleResultsArray[j].team==middleResultsArray[index].team)
		{
			teamSize++;
		}
	}
	int cnt=0,*tempArray;
	if(middleResultsArray[index].fromArray == 1)
		tempArray=malloc(list->numberOfResults*teamSize*sizeof(int));
	while(temp!=NULL){
		if(direction==0){
			for(int i=0;i<temp->rowSize;i++){
				if(middleResultsArray[index].fromArray==0)
					middleResultsArray[index].rowIds[counter]=temp->row_Array[i].idS;
				else if(middleResultsArray[index].fromArray == 1){
					for(int j=0;j<middleResultsCounter;j++)
					{
						if(middleResultsArray[j].team==middleResultsArray[index].team)
						{
							if(middleResultsArray[j].rowIdsNum < list->numberOfResults){

								middleResultsArray[j].rowIds=realloc(middleResultsArray[j].rowIds,list->numberOfResults*sizeof(int));
							}
							middleResultsArray[j].rowIdsNum=list->numberOfResults;
							tempArray[cnt]=middleResultsArray[j].rowIds[temp->row_Array[i].idS];
							cnt++;
						}
					}
				}
				counter++;
			}
		}
		else
		{
			for(int i=0;i<temp->rowSize;i++){
				if(middleResultsArray[index].fromArray==0)
					middleResultsArray[index].rowIds[counter]=temp->row_Array[i].idR;
				else if(middleResultsArray[index].fromArray == 1){
					for(int j=0;j<middleResultsCounter;j++)
					{
						if(middleResultsArray[j].team==middleResultsArray[index].team)
						{
							if(middleResultsArray[j].rowIdsNum < list->numberOfResults ){
								middleResultsArray[j].rowIds=realloc(middleResultsArray[j].rowIds,list->numberOfResults*sizeof(int));
							}
							middleResultsArray[j].rowIdsNum=list->numberOfResults;
							tempArray[cnt]=middleResultsArray[j].rowIds[temp->row_Array[i].idR];
							cnt++;
						}
					}
				}
				counter++;
			}
		}
		temp=temp->next;
	}
	int rowIndx=0;
	counter=0;
	if(middleResultsArray[index].fromArray==1){
		for(int k=0;k<list->numberOfResults;k++){
			for(int j=0;j<middleResultsCounter;j++){
				if(middleResultsArray[index].team==middleResultsArray[j].team){
					middleResultsArray[j].rowIds[rowIndx]=tempArray[counter];
					counter++;
				}
			}
			rowIndx++;
		}
		free(tempArray);
	}
}
oneColumnRelation* setColumnFromFirstArray(multiColumnRelation* relationArray,int relationIndx,int columnIndx){
	oneColumnRelation *temp;
	temp=malloc(sizeof(oneColumnRelation));
	temp->tuples=malloc(relationArray[relationIndx].rowCount*sizeof(tuple));
	temp->num_of_tuples=relationArray[relationIndx].rowCount;
	 memcpy(temp->tuples, relationArray[relationIndx].columns[columnIndx].tuples,relationArray[relationIndx].rowCount*sizeof(tuple));
	return temp;
}
oneColumnRelation* setColumnFromMiddleArray(middleResults* middleResArray,int relationIndx,int columnIndx,int arrayIndx,multiColumnRelation* relationArray){
	oneColumnRelation *temp;
	temp=malloc(sizeof(oneColumnRelation));

	if(middleResArray[arrayIndx].rowIdsNum==0){
		temp->tuples=malloc(sizeof(tuple));

	}else{
		temp->tuples=malloc(middleResArray[arrayIndx].rowIdsNum*sizeof(tuple));

	}
	temp->num_of_tuples=middleResArray[arrayIndx].rowIdsNum;
	for(int k=0;k<middleResArray[arrayIndx].rowIdsNum;k++)
	{
		int index=middleResArray[arrayIndx].rowIds[k];
		temp->tuples[k].value=relationArray[relationIndx].columns[columnIndx].tuples[index].value;
		temp->tuples[k].id=k;
	}
	return temp;
}
queryDataIndex* analyzeQuery(char *query)
{
	char *token,*token2,*token3,*temp,*tempToken;
	char *tempView,*tok,*bufRel,*bufCol;
	int i,j,k,index,numRel=0,numView=0;
	int numPredFilter=0,numPredJoinTwoRel=0;

	token=strtok(query,"|");
	token2=strtok(NULL,"|");
	token3=strtok(NULL,"|");

	tempToken=malloc(128);
	tempView=malloc(128);

	queryDataIndex *queryData;
	queryData=malloc(sizeof(queryDataIndex));

	for(i=0;i<3;i++){
		memset(tempToken,0,128);
		if(i==0){//1st part of query
			token=strtok(query,"|");
			strcat(tempToken,token);
			temp=strtok(token," ");
			while(temp!=NULL){
				numRel++;
				temp=strtok(NULL," ");
			}
			queryData->numRelQuery=numRel;
			queryData->QueryRelArray=malloc(numRel*sizeof(int));
			temp=strtok(tempToken," ");
			for(j=0;j<numRel;j++){
				queryData->QueryRelArray[j]=atoi(temp);
				temp=strtok(NULL," ");
			}
		}
		else if(i==1){//2nd part of query
			strcat(tempToken,token2);
			temp=strtok(token2,"&");
			while(temp!=NULL){
				memset(tempView,0,128);
				strcat(tempView,temp);
				temp=strtok(NULL,"&");
				if((tok=strchr(tempView,'>'))!=NULL ||(tok=strchr(tempView,'<'))!=NULL || ( (tok=strchr(tempView,'='))!=NULL && (tok=strstr(tok,"."))==NULL) ){
					numPredFilter++;
				}
				else if((tok=strchr(tempView,'='))!=NULL){//peritto else if mallon
						numPredJoinTwoRel++;
				}
				else{
					printf("wrong\n");
				}
			}
			queryData->numPredFilter=numPredFilter;
			queryData->predFilterArray=malloc(queryData->numPredFilter*sizeof(filterPredNode));
			for(k=0;k<queryData->numPredFilter;k++)
				queryData->predFilterArray[k].relColumn=malloc(sizeof(RelColNode));

			queryData->numPredJoinTwoRel=numPredJoinTwoRel;
			queryData->numPredJoinOneRel=0;
			queryData->twoRelationPredArray=malloc(queryData->numPredJoinTwoRel*sizeof(joinPredNode));
			for(k=0;k<queryData->numPredJoinTwoRel;k++){
				queryData->twoRelationPredArray[k].left=malloc(sizeof(RelColNode));
				queryData->twoRelationPredArray[k].right=malloc(sizeof(RelColNode));
				queryData->twoRelationPredArray[k].selected=0;
				queryData->twoRelationPredArray[k].score=-1;
			}


			int countF=0,countTwoR=0;
			char oper,*bufVal,*tok2;
			temp=strtok(tempToken,"&");
			while(temp!=NULL){
				memset(tempView,0,128);
				strcat(tempView,temp);
				temp=strtok(NULL,"&");
				if((tok=strchr(tempView,'>'))!=NULL ||(tok=strchr(tempView,'<'))!=NULL || ( (tok=strchr(tempView,'='))!=NULL && (tok2=strstr(tok,"."))==NULL) ){
					index = (int)(tok - tempView);
					oper=tempView[index];
					bufVal=malloc(128);
					memset(bufVal,0,128);
					for(k=index+1;k<strlen(tempView);k++){
						bufVal[k-index-1]=tempView[k];
					}
					bufVal[strlen(tempView)]='\0';

					tok=strchr(tempView,'.');
					index = (int)(tok - tempView);

					bufRel=malloc(index+1);
					bufCol=malloc(index+1);

					for(k=0;k<index;k++){
						bufRel[k]=tempView[k];
						bufCol[k]=tempView[k+index+1];
					}
					bufRel[index]='\0';
					bufCol[index]='\0';

					queryData->predFilterArray[countF].relColumn->rel=atoi(bufRel);
					queryData->predFilterArray[countF].relColumn->col=atoi(bufCol);
					queryData->predFilterArray[countF].typeOperation=oper;
					queryData->predFilterArray[countF].value=atoi(bufVal);

					countF++;
					free(bufVal);
					free(bufCol);
					free(bufRel);
				}
				else{//gia '='
					tok=strchr(tempView,'=');
					index = (int)(tok - tempView);
					int index_e=index;

					bufVal=malloc(index+1);
					for(k=0;k<index;k++){
						bufVal[k]=tempView[k];
					}
					bufVal[index]='\0';
					tok=strchr(bufVal,'.');
					index = (int)(tok - bufVal);

					bufRel=malloc(index+1);
					bufCol=malloc(index+1);
					for(k=0;k<index;k++){
						bufRel[k]=bufVal[k];
						bufCol[k]=bufVal[k+index+1];
					}
					bufRel[index]='\0';
					bufCol[index]='\0';

					queryData->twoRelationPredArray[countTwoR].left->rel=atoi(bufRel);
					queryData->twoRelationPredArray[countTwoR].left->col=atoi(bufCol);

					free(bufRel);
					free(bufCol);
					free(bufVal);
					////////////////////////////////////////////////////////////////
					bufVal=malloc(index_e+1);
					for(k=index_e+1;k<strlen(tempView);k++){
						bufVal[k-index_e-1]=tempView[k];
					}
					bufVal[index_e]='\0';
					tok=strchr(bufVal,'.');
					index = (int)(tok - bufVal);
					bufRel=malloc(index+1);
					bufCol=malloc(index+1);
					for(k=0;k<index;k++){
						bufRel[k]=bufVal[k];
						bufCol[k]=bufVal[k+index+1];
					}
					bufRel[index]='\0';
					bufCol[index]='\0';
					queryData->twoRelationPredArray[countTwoR].right->rel=atoi(bufRel);
					queryData->twoRelationPredArray[countTwoR].right->col=atoi(bufCol);
					//////////////////////////////////////////////////////////////
					countTwoR++;
					free(bufRel);
					free(bufCol);
					free(bufVal);

				}
			}
		}
		else{//3rd part of query
			strcat(tempToken,token3);
			temp=strtok(token3," ");
			while(temp!=NULL){
				numView++;
				temp=strtok(NULL," ");
			}
			queryData->numViewQuery=numView;
			queryData->viewQueryArray=malloc(numView*sizeof(RelColNode));
			temp=strtok(tempToken," ");
			for(j=0;j<numView;j++){
				memset(tempView,0,128);
				strcat(tempView,temp);
				temp=strtok(NULL," ");

				tok = strchr(tempView, '.');
				index = (int)(tok - tempView);

				bufRel=malloc(index+1);
				bufCol=malloc(index+1);

				for(k=0;k<index;k++){
					bufRel[k]=tempView[k];
					bufCol[k]=tempView[k+index+1];
				}
				bufRel[index]='\0';
				bufCol[index]='\0';
				queryData->viewQueryArray[j].rel=atoi(bufRel);
				queryData->viewQueryArray[j].col=atoi(bufCol);
				free(bufCol);
				free(bufRel);
			}
		}
	}
	free(tempToken);
	free(tempView);
	return queryData;
}
resultList* sameRelationJoin(oneColumnRelation *relR,oneColumnRelation *relS){//exoun to idio size
	int i;
	resultList *resList;
	resList=initializeResultList();
	for(i=0;i<relR->num_of_tuples;i++){
		if(relR->tuples[i].value==relS->tuples[i].value){//prepei ta id na einai ta idia
			insertResult(resList,relR->tuples[i].id,relR->tuples[i].id,1);			//2 sto teleutaio orisma shmainei oti exoume join apo idio relation
		}
	}
	return resList;
}
middleResults executeFilter(oneColumnRelation* column,int value,char typeOperation,int relation,all_stats *temp_stats,int relationId,int columnIndx)
{
	int i;
	middleResults results;
	results.rowIds=malloc(column->num_of_tuples*sizeof(int));				//isws einai megalos o xwros apothikeushs//
	results.rowIdsNum=0;
	results.relation=relation;
	results.fromArray=0;
	if(typeOperation=='=')
	{
		for(i=0;i<column->num_of_tuples;i++){
			if(column->tuples[i].value==value){
				results.rowIds[results.rowIdsNum]=column->tuples[i].id;
				results.relation = relation;
				results.rowIdsNum++;
			}
		}
		int prev_f=temp_stats->array_with_stats[relationId][columnIndx].f;
		//stats gia thn sthlh pou kaname to filter
		temp_stats->array_with_stats[relationId][columnIndx].l=value;
		temp_stats->array_with_stats[relationId][columnIndx].u=value;
		if(results.rowIdsNum==0){
			temp_stats->array_with_stats[relationId][columnIndx].d=0;
			temp_stats->array_with_stats[relationId][columnIndx].f=0;
		}
		else{
			temp_stats->array_with_stats[relationId][columnIndx].f=temp_stats->array_with_stats[relationId][columnIndx].f/temp_stats->array_with_stats[relationId][columnIndx].d;//edw mporw na balw kai results.rowIdsNum
			temp_stats->array_with_stats[relationId][columnIndx].d=1;
		}
		//stats gia tis upoloipes
		for(i=0;i<temp_stats->cols[relationId];i++){
			if(i!=columnIndx){
				double base,power;
				base=1-(1.0*temp_stats->array_with_stats[relationId][columnIndx].f)/prev_f;
				power=(1.0*temp_stats->array_with_stats[relationId][i].f)/temp_stats->array_with_stats[relationId][i].d;
				temp_stats->array_with_stats[relationId][i].d=temp_stats->array_with_stats[relationId][i].d*(1-pow(base,power));
				temp_stats->array_with_stats[relationId][i].f=temp_stats->array_with_stats[relationId][columnIndx].f;
				if(temp_stats->array_with_stats[relationId][i].f>0 && temp_stats->array_with_stats[relationId][i].d==0){
					temp_stats->array_with_stats[relationId][i].d=1;
				}
			}
		}
	}
	else if(typeOperation=='>')
	{
		for(i=0;i<column->num_of_tuples;i++){
			if(column->tuples[i].value>value){
				results.rowIds[results.rowIdsNum]=column->tuples[i].id;
				results.relation = relation;
				results.rowIdsNum++;
			}
		}
		int prev_f=temp_stats->array_with_stats[relationId][columnIndx].f;
		//stats
		if(value<temp_stats->array_with_stats[relationId][columnIndx].l){
			value=temp_stats->array_with_stats[relationId][columnIndx].l;
		}
		double suntelesths;
		suntelesths=(1.0*temp_stats->array_with_stats[relationId][columnIndx].u-value)/(1.0*temp_stats->array_with_stats[relationId][columnIndx].u-temp_stats->array_with_stats[relationId][columnIndx].l);

		temp_stats->array_with_stats[relationId][columnIndx].l=value;
		temp_stats->array_with_stats[relationId][columnIndx].d=suntelesths*temp_stats->array_with_stats[relationId][columnIndx].d;
		temp_stats->array_with_stats[relationId][columnIndx].f=suntelesths*temp_stats->array_with_stats[relationId][columnIndx].f;

		//stats gia tis upoloipes
		for(i=0;i<temp_stats->cols[relationId];i++){
			if(i!=columnIndx){
				double base,power;
				base=1-(1.0*temp_stats->array_with_stats[relationId][columnIndx].f)/prev_f;
				power=(1.0*temp_stats->array_with_stats[relationId][i].f)/(1.0*temp_stats->array_with_stats[relationId][i].d);
				temp_stats->array_with_stats[relationId][i].d=temp_stats->array_with_stats[relationId][i].d*(1-pow(base,power));
				temp_stats->array_with_stats[relationId][i].f=temp_stats->array_with_stats[relationId][columnIndx].f;

				if(temp_stats->array_with_stats[relationId][i].f>0 && temp_stats->array_with_stats[relationId][i].d==0){
					temp_stats->array_with_stats[relationId][i].d=1;
				}
			}
		}
	}
	else if(typeOperation=='<')
	{
		for(i=0;i<column->num_of_tuples;i++){
			if(column->tuples[i].value<value){
				results.rowIds[results.rowIdsNum]=column->tuples[i].id;
				results.relation = relation;
				results.rowIdsNum++;
			}
		}
		int prev_f=temp_stats->array_with_stats[relationId][columnIndx].f;
		//stats
		if(value>temp_stats->array_with_stats[relationId][columnIndx].u){
			value=temp_stats->array_with_stats[relationId][columnIndx].u;
		}
		double suntelesths;
		suntelesths=(1.0*value-temp_stats->array_with_stats[relationId][columnIndx].l)/(1.0*temp_stats->array_with_stats[relationId][columnIndx].u-temp_stats->array_with_stats[relationId][columnIndx].l);
		temp_stats->array_with_stats[relationId][columnIndx].u=value;
		temp_stats->array_with_stats[relationId][columnIndx].d=suntelesths*temp_stats->array_with_stats[relationId][columnIndx].d;
		temp_stats->array_with_stats[relationId][columnIndx].f=suntelesths*temp_stats->array_with_stats[relationId][columnIndx].f;

		//stats gia tis upoloipes
		for(i=0;i<temp_stats->cols[relationId];i++){
			if(i!=columnIndx){
				double base,power;
				base=1-(1.0*temp_stats->array_with_stats[relationId][columnIndx].f)/prev_f;
				power=(1.0*temp_stats->array_with_stats[relationId][i].f)/(1.0*temp_stats->array_with_stats[relationId][i].d);
				temp_stats->array_with_stats[relationId][i].d=temp_stats->array_with_stats[relationId][i].d*(1-pow(base,power));
				temp_stats->array_with_stats[relationId][i].f=temp_stats->array_with_stats[relationId][columnIndx].f;
				if(temp_stats->array_with_stats[relationId][i].f>0 && temp_stats->array_with_stats[relationId][i].d==0){
					temp_stats->array_with_stats[relationId][i].d=1;
				}
			}
		}
	}
	return results;
}
int *JoinEnumeration(queryDataIndex *data,all_stats *before_joins_stats){
	int i,j,k;
	int **graph;//an exw kathgorhma ths morfhs A.x=A.y tote 8a balw sthn diagwnio 1
	bestTree *btree;
	btree=NULL;
	//initiliaze graph
	graph=malloc(data->numRelQuery*sizeof(int *));
	for(i=0;i<data->numRelQuery;i++){
		graph[i]=malloc(data->numRelQuery*sizeof(int));
		for(j=0;j<data->numRelQuery;j++){
			graph[i][j]=0;
		}
	}
	//fill graph
	for(i=0;i<data->numPredJoinTwoRel;i++){
		graph[data->twoRelationPredArray[i].left->rel][data->twoRelationPredArray[i].right->rel]=1;
		graph[data->twoRelationPredArray[i].right->rel][data->twoRelationPredArray[i].left->rel]=1;
	}

	//ta filter ta exw treksei me thn seira pou mou erxontai, opote edw 8a dw mono ta join
	//ta stats exoun allaksei sthn executeFilter
	int leftRelationId,leftColumnIndx,rightRelationId,rightColumnIndx;
	if(data->numPredJoinTwoRel>0){
		//initialize btree
		btree=malloc(data->numRelQuery*sizeof(bestTree));
		for(i=0;i<data->numRelQuery;i++){
			btree[i].startlist=NULL;
			btree[i].endlist=NULL;
			btree[i].BestNode=NULL;
		}

		for(i=0;i<data->numRelQuery;i++){//to prwto for apo thn ekfwnhsh
			int *set,cost;
			set=malloc(data->numRelQuery*sizeof(int));//ftiaxnw to set , 8a einai ola "0" ektos apo 1 "1" ka8ws exw set mege8ous 1
			for(j=0;j<data->numRelQuery;j++){
				if(i==j){
					set[j]=1;
				}
				else{
					set[j]=0;
				}
			}

			int *teams,teamCount=0;//arxika den exoun sxhmatistei omades opote ola "0"
			teams=malloc(data->numRelQuery*sizeof(int));
			for(j=0;j<data->numRelQuery;j++){
				teams[j]=0;
			}

			int *seira;
			int seira_point=0;//kai arxikopoiw thn seira se -1 ka8ws pairnei times {0,...,data->numPredJoinTwoRel-1}
			seira=malloc(data->numPredJoinTwoRel*sizeof(int));
			for(j=0;j<data->numPredJoinTwoRel;j++){
				seira[j]=-1;
			}

			//antigrafw to stats wste ka8e diaforatikh seira rels na exei ta dika dika toy endiamesa apotelesmata
			all_stats *temp_stats;
			temp_stats=malloc(sizeof(all_stats));
			temp_stats->rels=before_joins_stats->rels;
			temp_stats->cols=malloc(temp_stats->rels*sizeof(uint64_t));
			temp_stats->array_with_stats=malloc(temp_stats->rels*sizeof(stats *));
			for(j=0;j<temp_stats->rels;j++){
				temp_stats->cols[j]=before_joins_stats->cols[j];
				temp_stats->array_with_stats[j]=malloc(temp_stats->cols[j]*sizeof(stats));
				for(k=0;k<temp_stats->cols[j];k++){
					temp_stats->array_with_stats[j][k].l=before_joins_stats->array_with_stats[j][k].l;
					temp_stats->array_with_stats[j][k].u=before_joins_stats->array_with_stats[j][k].u;
					temp_stats->array_with_stats[j][k].f=before_joins_stats->array_with_stats[j][k].f;
					temp_stats->array_with_stats[j][k].d=before_joins_stats->array_with_stats[j][k].d;
				}
			}


			if(graph[i][i]==1){//exoume join se sthles tou idiou pinaka (sameJoin)
				for(k=0;k<data->numPredJoinTwoRel;k++){
					if(data->twoRelationPredArray[k].left->rel==i && data->twoRelationPredArray[k].right->rel==i){
						seira[seira_point]=k;//bazw ta kathgorhmata sthn seira
						seira_point++;

						int indx=k;
						leftRelationId = data->twoRelationPredArray[indx].left->rel;
						leftColumnIndx =data->twoRelationPredArray[indx].left->col;
						rightRelationId = data->twoRelationPredArray[indx].right->rel;
						rightColumnIndx= data->twoRelationPredArray[indx].right->col;
						sameJoinStatsCalculator(data,temp_stats,teams,leftRelationId,leftColumnIndx,rightRelationId,rightColumnIndx);
					}

					cost=temp_stats->array_with_stats[rightRelationId][rightColumnIndx].f;//8a agnow to cost gia thn prwth fora
				}
				insertList(&(btree[0].startlist),cost,set,data->numRelQuery,teams,teamCount,temp_stats,seira,data->numPredJoinTwoRel,seira_point);
			}
			else{
				cost=0;
				//printf("bazw sthn lista 2\n");
				insertList(&(btree[0].startlist),cost,set,data->numRelQuery,teams,teamCount,temp_stats,seira,data->numPredJoinTwoRel,seira_point);
			}
			free(set);
			free(teams);
			free(seira);
		}

		//to spaw se 2 kommatia sto prwto 8a kanw opws kai panw alla gia 2 sxeseis(wste na ftiaksw kai ena best tree) kai sto allo 8a pairnw to best kai 8a to sugkrinw
		listnode *temp;
		temp=btree[0].startlist;
		while(temp!=NULL){
			for(j=0;j<data->numRelQuery;j++){//to for me to R pou den anhkei sto S
				if(temp->set[j]==0){//den to exoume xrhsimopoihsei st0 sugkekrimeno set , ara to 8eloume
					//opote paw sto graph kai elegxw an ennonetai me kapoio
					for(k=j+1;k<data->numRelQuery;k++){//den 8elw ta k opou einai mikrotera apo to j giati exw ftiaksei autes tis omades
						if( k!=j && temp->set[k]==1 && graph[j][k]==1 ){//an to k einai sto set kai sundaietai me to j tote to 8eloume
							//pairnw ta dedomena apo thn lista
							int *set,*teams,teamCount,cost;
							all_stats *local_stats;
							int *seira,seira_point;

							set=malloc(data->numRelQuery*sizeof(int));
							teams=malloc(data->numRelQuery*sizeof(int));
							teamCount=temp->teamCount;
							for(i=0;i<data->numRelQuery;i++){
								set[i]=temp->set[i];
								teams[i]=temp->teams[i];
							}

							seira=malloc(data->numPredJoinTwoRel*sizeof(int));
							seira_point=temp->seira_point;
							for(i=0;i<data->numPredJoinTwoRel;i++){
								seira[i]=temp->seira[i];
							}


							local_stats=malloc(sizeof(all_stats));
							local_stats->rels=temp->local_stats->rels;
							local_stats->cols=malloc(local_stats->rels*sizeof(uint64_t));
							local_stats->array_with_stats=malloc(local_stats->rels*sizeof(stats *));
							for(i=0;i<local_stats->rels;i++){
								local_stats->cols[i]=temp->local_stats->cols[i];
								local_stats->array_with_stats[i]=malloc(local_stats->cols[i]*sizeof(stats));
								for(int q=0;q<local_stats->cols[i];q++){
									local_stats->array_with_stats[i][q].l=temp->local_stats->array_with_stats[i][q].l;
									local_stats->array_with_stats[i][q].u=temp->local_stats->array_with_stats[i][q].u;
									local_stats->array_with_stats[i][q].f=temp->local_stats->array_with_stats[i][q].f;
									local_stats->array_with_stats[i][q].d=temp->local_stats->array_with_stats[i][q].d;
								}
							}

							//upologizw to cost ,
							for(i=0;i<data->numPredJoinTwoRel;i++){//koitazw ola ta kathgorhmata na dw ti prakseis
								if( (data->twoRelationPredArray[i].left->rel==j && data->twoRelationPredArray[i].right->rel==k) || (data->twoRelationPredArray[i].left->rel==k && data->twoRelationPredArray[i].right->rel==j) ){//koitazw an ta rel uparxoun sta kathgorhmata
									seira[seira_point]=i;
									seira_point++;
									if(teams[j]==teams[k] && teams[j]!=0 ){//elegxw tis omades , dld an 8a kanw radix h samejoin
										int indx=i;//exw samejoin
										leftRelationId = data->twoRelationPredArray[indx].left->rel;
										leftColumnIndx =data->twoRelationPredArray[indx].left->col;
										rightRelationId = data->twoRelationPredArray[indx].right->rel;
										rightColumnIndx= data->twoRelationPredArray[indx].right->col;

										sameJoinStatsCalculator(data,local_stats,teams,leftRelationId,leftColumnIndx,rightRelationId,rightColumnIndx);
										cost=local_stats->array_with_stats[rightRelationId][rightColumnIndx].f;
										//edw den xreiazetai na allaksw omades
									}
									else{//exw radix
//printf("QWERTY4\n");
										int indx=i;
										leftRelationId = data->twoRelationPredArray[indx].left->rel;
										leftColumnIndx = data->twoRelationPredArray[indx].left->col;
										rightRelationId = data->twoRelationPredArray[indx].right->rel;
										rightColumnIndx= data->twoRelationPredArray[indx].right->col;

										RadixStatsCalculator(data,local_stats,teams,leftRelationId,leftColumnIndx,rightRelationId,rightColumnIndx);
										cost=local_stats->array_with_stats[rightRelationId][rightColumnIndx].f;

										//allazw ta teams
										int newteam;
										if(teams[rightRelationId]==0 && teams[leftRelationId]==0){//an kai ta 2 den anhkoun se kapoia omada tote ta bazw mazi se nea omada
											teamCount++;
											teams[rightRelationId]=teamCount;
											teams[leftRelationId]=teamCount;
										}
										else if(teams[rightRelationId]==0){//edw opws kai katw an ena den exei omada mpainei sthn omada toy allou
											teams[rightRelationId]=teams[leftRelationId];
										}
										else if(teams[leftRelationId]==0){
											teams[leftRelationId]=teams[rightRelationId];
										}
										else{//an kai ta 2 anhkoun se omada tote ola mpainoun sthn omada ths "mikroterhs omadas"
											if(teams[rightRelationId]<teams[leftRelationId]){
												newteam=teams[rightRelationId];
											}
											else{
												newteam=teams[leftRelationId];
											}
											for(int q=0;q<data->numRelQuery;q++){
												if(teams[q]==teams[rightRelationId] || teams[q]==teams[leftRelationId]){
													teams[q]=newteam;
												}
											}
										}
										//bazw oti pleon phra kai to j sto set
										set[j]=1;
									}
								}
							}
							insertList(&(btree[1].startlist),cost,set,data->numRelQuery,teams,teamCount,local_stats,seira,data->numPredJoinTwoRel,seira_point);
							//kai na kanw to besttree
							if(btree[1].BestNode==NULL){//an einai o prwtos kombos tote automata autos einai o kaluteros kombos
								btree[1].BestNode=btree[1].startlist;
							}
							if(btree[1].BestNode->cost>cost){//alliws elegxw an o kombos pou molis ebala exei pio mikro kostos apo ton BestNode wste na ton allaksw
								listnode *temp2;
								temp2=btree[1].startlist;
								while(temp2->next!=NULL){
									temp2=temp2->next;
								}
								btree[1].BestNode=temp2;//o kaluteros kombos einai o teleutaios pou molis ebala
							}

							free(set);
							free(teams);
							free(seira);
						}
					}
				}
			}
			temp=temp->next;
		}
		//to 2o kommati
		for(int subteam=2;subteam<data->numRelQuery;subteam++){
			listnode *temp;
			temp=btree[subteam-1].BestNode;
			for(j=0;j<data->numRelQuery;j++){//to for me to R pou den anhkei sto S
				if(temp->set[j]==0){//den to exoume xrhsimopoihsei st0 sugkekrimeno set , ara to 8eloume
					//opote paw sto graph kai elegxw an ennonetai me kapoio
					for(k=0;k<data->numRelQuery;k++){//edw elegxw ola ta k kai oxi mono ta >j
						if( k!=j && temp->set[k]==1 && graph[j][k]==1 ){//an to k einai sto set kai sundaietai me to j tote to 8eloume
							//pairnw ta dedomena apo thn lista
							int *set,*teams,teamCount,cost;
							all_stats *local_stats;
							int *seira,seira_point;

							set=malloc(data->numRelQuery*sizeof(int));
							teams=malloc(data->numRelQuery*sizeof(int));
							teamCount=temp->teamCount;
							for(i=0;i<data->numRelQuery;i++){
								set[i]=temp->set[i];
								teams[i]=temp->teams[i];
							}

							seira=malloc(data->numPredJoinTwoRel*sizeof(int));
							seira_point=temp->seira_point;
							for(i=0;i<data->numPredJoinTwoRel;i++){
								seira[i]=temp->seira[i];
							}


							local_stats=malloc(sizeof(all_stats));
							local_stats->rels=temp->local_stats->rels;
							local_stats->cols=malloc(local_stats->rels*sizeof(uint64_t));
							local_stats->array_with_stats=malloc(local_stats->rels*sizeof(stats*));
							for(i=0;i<local_stats->rels;i++){
								local_stats->cols[i]=temp->local_stats->cols[i];
								local_stats->array_with_stats[i]=malloc(local_stats->cols[i]*sizeof(stats));
								for(int q=0;q<local_stats->cols[i];q++){
									local_stats->array_with_stats[i][q].l=temp->local_stats->array_with_stats[i][q].l;
									local_stats->array_with_stats[i][q].u=temp->local_stats->array_with_stats[i][q].u;
									local_stats->array_with_stats[i][q].f=temp->local_stats->array_with_stats[i][q].f;
									local_stats->array_with_stats[i][q].d=temp->local_stats->array_with_stats[i][q].d;
								}
							}

							//upologizw to cost ,
							for(i=0;i<data->numPredJoinTwoRel;i++){//koitazw ola ta kathgorhmata na dw ti prakseis
								if( (data->twoRelationPredArray[i].left->rel==j && data->twoRelationPredArray[i].right->rel==k) || (data->twoRelationPredArray[i].left->rel==k && data->twoRelationPredArray[i].right->rel==j) ){//koitazw an ta rel uparxoun sta kathgorhmata
									seira[seira_point]=i;
									seira_point++;
									if(teams[j]==teams[k] && teams[j]!=0 ){//elegxw tis omades , dld an 8a kanw radix h samejoin
										int indx=i;//exw sameJoin
										leftRelationId = data->twoRelationPredArray[indx].left->rel;
										leftColumnIndx =data->twoRelationPredArray[indx].left->col;
										rightRelationId = data->twoRelationPredArray[indx].right->rel;
										rightColumnIndx= data->twoRelationPredArray[indx].right->col;

										sameJoinStatsCalculator(data,local_stats,teams,leftRelationId,leftColumnIndx,rightRelationId,rightColumnIndx);
										cost=local_stats->array_with_stats[rightRelationId][rightColumnIndx].f;
										//edw den xreiazetai na allaksw omades
									}
									else{//exw radix
//printf("QWERTY4\n");
										int indx=i;
										leftRelationId = data->twoRelationPredArray[indx].left->rel;
										leftColumnIndx = data->twoRelationPredArray[indx].left->col;
										rightRelationId = data->twoRelationPredArray[indx].right->rel;
										rightColumnIndx= data->twoRelationPredArray[indx].right->col;

										RadixStatsCalculator(data,local_stats,teams,leftRelationId,leftColumnIndx,rightRelationId,rightColumnIndx);
										cost=local_stats->array_with_stats[rightRelationId][rightColumnIndx].f;

										//allazw ta teams
										int newteam;
										if(teams[rightRelationId]==0 && teams[leftRelationId]==0){//omoia me panw
											teamCount++;
											teams[rightRelationId]=teamCount;
											teams[leftRelationId]=teamCount;
										}
										else if(teams[rightRelationId]==0){
											teams[rightRelationId]=teams[leftRelationId];
										}
										else if(teams[leftRelationId]==0){
											teams[leftRelationId]=teams[rightRelationId];
										}
										else{
											if(teams[rightRelationId]<teams[leftRelationId]){
												newteam=teams[rightRelationId];
											}
											else{
												newteam=teams[leftRelationId];
											}
											for(int q=0;q<data->numRelQuery;q++){
												if(teams[q]==teams[rightRelationId] || teams[q]==teams[leftRelationId]){
													teams[q]=newteam;
												}
											}
										}
										//bazw oti pleon phra kai to j sto set
										set[j]=1;
									}
								}
							}

							insertList(&(btree[subteam].startlist),cost,set,data->numRelQuery,teams,teamCount,local_stats,seira,data->numPredJoinTwoRel,seira_point);
							//kai na kanw to besttree
							if(btree[subteam].BestNode==NULL){
								btree[subteam].BestNode=btree[subteam].startlist;
							}
							if(btree[subteam].BestNode->cost>cost){
								listnode *temp2;
								temp2=btree[subteam].startlist;
								while(temp2->next!=NULL){
									temp2=temp2->next;
								}
								btree[subteam].BestNode=temp2;//o kaluteros kombos einai o teleutaios pou molis ebala
							}

							free(set);
							free(teams);
							free(seira);
						}
					}
				}
			}
		}
		//free graph,btree and create ret_seira
		int *ret_seira;//einai h seira tou bestNode tou monadikou kombou ths teleutaias seiras tou pinaka
		ret_seira=malloc(data->numPredJoinTwoRel*sizeof(int));
		for(j=0;j<data->numPredJoinTwoRel;j++){
			ret_seira[j]=btree[data->numRelQuery-1].BestNode->seira[j];
		}

		for(j=0;j<data->numRelQuery;j++){
			free(graph[j]);
		}
		free(graph);

		listnode *temp_node,*next_node;
		for(j=0;j<data->numRelQuery;j++){
			temp_node=btree[j].startlist;
			while(temp_node!=NULL){
				next_node=temp_node->next;

				free(temp_node->set);
				free(temp_node->teams);
				free(temp_node->seira);
				///stats
				for(k=0;k<temp_node->local_stats->rels;k++){
					free(temp_node->local_stats->array_with_stats[k]);
				}
				free(temp_node->local_stats->array_with_stats);
				free(temp_node->local_stats->cols);
				free(temp_node->local_stats);

				free(temp_node);
				temp_node=next_node;
			}
		}
		free(btree);

		return ret_seira;
	}
	else{
		return NULL;
	}
}

void greaterThanFilterStatsCalculator(queryDataIndex *data,all_stats *statsArray,int *teams,int relationId,int columnIndx,int value){// R.A>value

	int prev_f=statsArray->array_with_stats[relationId][columnIndx].f;
	//stats
	if(value<statsArray->array_with_stats[relationId][columnIndx].l){//ta stats den allazoun , gt ta pairnw ola
		value=statsArray->array_with_stats[relationId][columnIndx].l;
		return;
	}

	//gia thn sthlh tou filter
	double suntelesths;
	suntelesths=(1.0*statsArray->array_with_stats[relationId][columnIndx].u-value)/(1.0*statsArray->array_with_stats[relationId][columnIndx].u-statsArray->array_with_stats[relationId][columnIndx].l);

	statsArray->array_with_stats[relationId][columnIndx].l=value;
	statsArray->array_with_stats[relationId][columnIndx].d=suntelesths*statsArray->array_with_stats[relationId][columnIndx].d;
	statsArray->array_with_stats[relationId][columnIndx].f=suntelesths*statsArray->array_with_stats[relationId][columnIndx].f;

	//stats gia tis upoloipes sthles tou pinaka
	for(int i=0;i<statsArray->cols[relationId];i++){
		if(i!=columnIndx){
			double base,power;
			if(prev_f==0 || statsArray->array_with_stats[relationId][i].d==0 || statsArray->array_with_stats[relationId][i].f==0){
				statsArray->array_with_stats[relationId][i].d=0;
				statsArray->array_with_stats[relationId][i].f=0;
			}
			else{
				base=1-(1.0*statsArray->array_with_stats[relationId][columnIndx].f)/prev_f;
				power=(1.0*statsArray->array_with_stats[relationId][i].f)/(1.0*statsArray->array_with_stats[relationId][i].d);
				statsArray->array_with_stats[relationId][i].d=statsArray->array_with_stats[relationId][i].d*(1-pow(base,power));
				statsArray->array_with_stats[relationId][i].f=statsArray->array_with_stats[relationId][columnIndx].f;
			}

			if(statsArray->array_with_stats[relationId][i].f>0 && statsArray->array_with_stats[relationId][i].d==0){
				statsArray->array_with_stats[relationId][i].d=1;
			}
		}
	}

	//stats gia tous upoloipous pinakes ths omadas pou anhkei h sthlh
	for(int j=0;j<statsArray->rels;j++){
		if(j!=relationId && teams[j]==teams[relationId] && teams[j]!=0){
			for(int i=0;i<statsArray->cols[j];i++){
				double base,power;
				if(prev_f==0 || statsArray->array_with_stats[j][i].d==0 || statsArray->array_with_stats[j][i].f==0){
					statsArray->array_with_stats[j][i].d=0;
					statsArray->array_with_stats[j][i].f=0;
				}
				else{
					base=1-(1.0*statsArray->array_with_stats[relationId][columnIndx].f)/prev_f;
					power=(1.0*statsArray->array_with_stats[j][i].f)/(1.0*statsArray->array_with_stats[j][i].d);
					statsArray->array_with_stats[j][i].d=statsArray->array_with_stats[j][i].d*(1-pow(base,power));
					statsArray->array_with_stats[j][i].f=statsArray->array_with_stats[relationId][columnIndx].f;

					if(statsArray->array_with_stats[j][i].f>0 && statsArray->array_with_stats[j][i].d==0){
						statsArray->array_with_stats[j][i].d=1;
					}
				}
			}
		}
	}
}

void lessThanFilterStatsCalculator(queryDataIndex *data,all_stats *statsArray,int *teams,int relationId,int columnIndx,int value){// R.A<value

	int prev_f=statsArray->array_with_stats[relationId][columnIndx].f;
	//stats
	if(value>statsArray->array_with_stats[relationId][columnIndx].u){//ta stats den allazoun , gt ta pairnw ola
		value=statsArray->array_with_stats[relationId][columnIndx].u;
		return;
	}

	//gia thn sthlh tou filter
	double suntelesths;
	suntelesths=(1.0*value-statsArray->array_with_stats[relationId][columnIndx].l)/(1.0*statsArray->array_with_stats[relationId][columnIndx].u-statsArray->array_with_stats[relationId][columnIndx].l);

	statsArray->array_with_stats[relationId][columnIndx].u=value;
	statsArray->array_with_stats[relationId][columnIndx].d=suntelesths*statsArray->array_with_stats[relationId][columnIndx].d;
	statsArray->array_with_stats[relationId][columnIndx].f=suntelesths*statsArray->array_with_stats[relationId][columnIndx].f;

	//stats gia tis upoloipes sthles tou pinaka
	for(int i=0;i<statsArray->cols[relationId];i++){
		if(i!=columnIndx){
			double base,power;
			if(prev_f==0 || statsArray->array_with_stats[relationId][i].d==0 || statsArray->array_with_stats[relationId][i].f==0){
				statsArray->array_with_stats[relationId][i].d=0;
				statsArray->array_with_stats[relationId][i].f=0;
			}
			else{
				base=1-(1.0*statsArray->array_with_stats[relationId][columnIndx].f)/prev_f;
				power=(1.0*statsArray->array_with_stats[relationId][i].f)/(1.0*statsArray->array_with_stats[relationId][i].d);
				statsArray->array_with_stats[relationId][i].d=statsArray->array_with_stats[relationId][i].d*(1-pow(base,power));
				statsArray->array_with_stats[relationId][i].f=statsArray->array_with_stats[relationId][columnIndx].f;
			}

			if(statsArray->array_with_stats[relationId][i].f>0 && statsArray->array_with_stats[relationId][i].d==0){
				statsArray->array_with_stats[relationId][i].d=1;
			}
		}
	}

	//stats gia tous upoloipous pinakes ths omadas pou anhkei h sthlh
	for(int j=0;j<statsArray->rels;j++){
		if(j!=relationId && teams[j]==teams[relationId] && teams[j]!=0){
			for(int i=0;i<statsArray->cols[j];i++){
				double base,power;
				if(prev_f==0 || statsArray->array_with_stats[j][i].d==0 || statsArray->array_with_stats[j][i].f==0){
					statsArray->array_with_stats[j][i].d=0;
					statsArray->array_with_stats[j][i].f=0;
				}
				else{
					base=1-(1.0*statsArray->array_with_stats[relationId][columnIndx].f)/prev_f;
					power=(1.0*statsArray->array_with_stats[j][i].f)/(1.0*statsArray->array_with_stats[j][i].d);
					statsArray->array_with_stats[j][i].d=statsArray->array_with_stats[j][i].d*(1-pow(base,power));
					statsArray->array_with_stats[j][i].f=statsArray->array_with_stats[relationId][columnIndx].f;
				}

				if(statsArray->array_with_stats[j][i].f>0 && statsArray->array_with_stats[j][i].d==0){
					statsArray->array_with_stats[j][i].d=1;
				}
			}
		}
	}
}

void sameJoinStatsCalculator(queryDataIndex *data,all_stats *statsArray,int *teams,int leftRelationId,int leftColumnIndx,int rightRelationId,int rightColumnIndx){

	//gia tis sthles tou join
	if(statsArray->array_with_stats[leftRelationId][leftColumnIndx].l > statsArray->array_with_stats[rightRelationId][rightColumnIndx].l){
		statsArray->array_with_stats[rightRelationId][rightColumnIndx].l=statsArray->array_with_stats[leftRelationId][leftColumnIndx].l;
	}
	else{
		statsArray->array_with_stats[leftRelationId][leftColumnIndx].l=statsArray->array_with_stats[rightRelationId][rightColumnIndx].l;
	}

	if(statsArray->array_with_stats[leftRelationId][leftColumnIndx].u < statsArray->array_with_stats[rightRelationId][rightColumnIndx].u){
		statsArray->array_with_stats[rightRelationId][rightColumnIndx].u=statsArray->array_with_stats[leftRelationId][leftColumnIndx].u;
	}
	else{
		statsArray->array_with_stats[leftRelationId][leftColumnIndx].u=statsArray->array_with_stats[rightRelationId][rightColumnIndx].u;
	}

	int prev_f=statsArray->array_with_stats[leftRelationId][leftColumnIndx].f;
	int n=statsArray->array_with_stats[leftRelationId][leftColumnIndx].u-statsArray->array_with_stats[rightRelationId][rightColumnIndx].l+1;
	statsArray->array_with_stats[leftRelationId][leftColumnIndx].f=statsArray->array_with_stats[leftRelationId][leftColumnIndx].f/n;
	statsArray->array_with_stats[rightRelationId][rightColumnIndx].f=statsArray->array_with_stats[leftRelationId][leftColumnIndx].f;

	double base,power;
	if(prev_f==0 || statsArray->array_with_stats[leftRelationId][leftColumnIndx].d==0){
		statsArray->array_with_stats[leftRelationId][leftColumnIndx].d=0;
		statsArray->array_with_stats[rightRelationId][rightColumnIndx].d=0;
	}
	else{
		base=1-(1.0*statsArray->array_with_stats[leftRelationId][leftColumnIndx].f)/prev_f;
		power=(1.0*statsArray->array_with_stats[leftRelationId][leftColumnIndx].f)/statsArray->array_with_stats[leftRelationId][leftColumnIndx].d;
		statsArray->array_with_stats[leftRelationId][leftColumnIndx].d=statsArray->array_with_stats[leftRelationId][leftColumnIndx].d*(1-pow(base,power));
		statsArray->array_with_stats[rightRelationId][rightColumnIndx].d=statsArray->array_with_stats[leftRelationId][leftColumnIndx].d;
	}

	//gia tis upoloipes sthles twn 2 relation tou query
	//exoume 2 periptwseis
	//a)oi sthles na einai ston idio pinaka , auto 8a ginei prin arxisoun na sxhmatizontai omades opote den xreiazetai na allaksw allo pinaka
	if(leftRelationId==rightRelationId){
		for(int i=0;i<statsArray->cols[leftRelationId];i++){
			if(i!=leftColumnIndx && i!=rightColumnIndx){
				if(prev_f==0 || statsArray->array_with_stats[leftRelationId][i].d==0){
					statsArray->array_with_stats[leftRelationId][i].d=0;
					statsArray->array_with_stats[leftRelationId][i].f=0;
				}
				else{
					base=1-(1.0*statsArray->array_with_stats[leftRelationId][leftColumnIndx].f)/prev_f;
					power=(1.0*statsArray->array_with_stats[leftRelationId][i].f)/statsArray->array_with_stats[leftRelationId][i].d;
					statsArray->array_with_stats[leftRelationId][i].d=statsArray->array_with_stats[leftRelationId][i].d*(1-pow(base,power));
					statsArray->array_with_stats[leftRelationId][i].f=statsArray->array_with_stats[leftRelationId][leftColumnIndx].f;
				}
			}
		}
	}
	//b)oi sthles na einai se diaforetikous pinakes ths idias omadas
	else{
		//allazw tis upoloipes sthles tou left pinaka
		for(int i=0;i<statsArray->cols[leftRelationId];i++){
			if(i!=leftColumnIndx){
				if(prev_f==0 || statsArray->array_with_stats[leftRelationId][i].d==0){
					statsArray->array_with_stats[leftRelationId][i].d=0;
					statsArray->array_with_stats[leftRelationId][i].f=0;
				}
				else{
					base=1-(1.0*statsArray->array_with_stats[leftRelationId][leftColumnIndx].f)/prev_f;
					power=(1.0*statsArray->array_with_stats[leftRelationId][i].f)/statsArray->array_with_stats[leftRelationId][i].d;
					statsArray->array_with_stats[leftRelationId][i].d=statsArray->array_with_stats[leftRelationId][i].d*(1-pow(base,power));
					statsArray->array_with_stats[leftRelationId][i].f=statsArray->array_with_stats[leftRelationId][leftColumnIndx].f;
				}
			}
		}

		//allazw tis upoloipes sthles tou right pinaka
		for(int i=0;i<statsArray->cols[rightRelationId];i++){
			if(i!=rightColumnIndx){
				if(prev_f==0 || statsArray->array_with_stats[rightRelationId][i].d==0){
					statsArray->array_with_stats[rightRelationId][i].d=0;
					statsArray->array_with_stats[rightRelationId][i].f=0;
				}
				else{
					base=1-(1.0*statsArray->array_with_stats[rightRelationId][rightColumnIndx].f)/prev_f;
					power=(1.0*statsArray->array_with_stats[rightRelationId][i].f)/statsArray->array_with_stats[rightRelationId][i].d;
					statsArray->array_with_stats[rightRelationId][i].d=statsArray->array_with_stats[rightRelationId][i].d*(1-pow(base,power));
					statsArray->array_with_stats[rightRelationId][i].f=statsArray->array_with_stats[rightRelationId][rightColumnIndx].f;
				}
			}
		}

		//allazw olous tous pinakes sto team
		for(int j=0;j<statsArray->rels;j++){
			if(j!=leftRelationId && j!=rightRelationId && teams[j]==teams[rightRelationId] && teams[j]!=0){
				for(int i=0;i<statsArray->cols[j];i++){
					if(prev_f==0 || statsArray->array_with_stats[j][i].d==0){
						statsArray->array_with_stats[j][i].d=0;
						statsArray->array_with_stats[j][i].f=0;
					}
					else{
						base=1-(1.0*statsArray->array_with_stats[j][rightColumnIndx].f)/prev_f;
						power=(1.0*statsArray->array_with_stats[j][i].f)/statsArray->array_with_stats[j][i].d;
						statsArray->array_with_stats[j][i].d=statsArray->array_with_stats[j][i].d*(1-pow(base,power));
						statsArray->array_with_stats[j][i].f=statsArray->array_with_stats[j][rightColumnIndx].f;
					}
				}
			}
		}
	}
}

void RadixStatsCalculator(queryDataIndex *data,all_stats *statsArray,int *teams,int leftRelationId,int leftColumnIndx,int rightRelationId,int rightColumnIndx){

	//arxika kanw thn proetimasia me ta filters
	if(statsArray->array_with_stats[leftRelationId][leftColumnIndx].l > statsArray->array_with_stats[rightRelationId][rightColumnIndx].l){
		greaterThanFilterStatsCalculator(data,statsArray,teams,rightRelationId,rightColumnIndx,statsArray->array_with_stats[leftRelationId][leftColumnIndx].l);
	}
	else{
		greaterThanFilterStatsCalculator(data,statsArray,teams,leftRelationId,leftColumnIndx,statsArray->array_with_stats[rightRelationId][rightColumnIndx].l);
	}

	if(statsArray->array_with_stats[leftRelationId][leftColumnIndx].u < statsArray->array_with_stats[rightRelationId][rightColumnIndx].u){
		lessThanFilterStatsCalculator(data,statsArray,teams,rightRelationId,rightColumnIndx,statsArray->array_with_stats[leftRelationId][leftColumnIndx].u);
	}
	else{
		lessThanFilterStatsCalculator(data,statsArray,teams,leftRelationId,leftColumnIndx,statsArray->array_with_stats[rightRelationId][rightColumnIndx].u);
	}

	//gia tis sthles ths radix
	//ta u,l einai etoima
	int prev_d_A=statsArray->array_with_stats[leftRelationId][leftColumnIndx].d;
	int prev_d_B=statsArray->array_with_stats[rightRelationId][rightColumnIndx].d;
	int n=statsArray->array_with_stats[leftRelationId][leftColumnIndx].u-statsArray->array_with_stats[rightRelationId][rightColumnIndx].l+1;

	statsArray->array_with_stats[leftRelationId][leftColumnIndx].f=(statsArray->array_with_stats[leftRelationId][leftColumnIndx].f*statsArray->array_with_stats[rightRelationId][rightColumnIndx].f)/n;
	statsArray->array_with_stats[rightRelationId][rightColumnIndx].f=statsArray->array_with_stats[leftRelationId][leftColumnIndx].f;

	statsArray->array_with_stats[leftRelationId][leftColumnIndx].d=(statsArray->array_with_stats[leftRelationId][leftColumnIndx].d*statsArray->array_with_stats[rightRelationId][rightColumnIndx].d)/n;
	statsArray->array_with_stats[rightRelationId][rightColumnIndx].d=statsArray->array_with_stats[leftRelationId][leftColumnIndx].d;

	double base,power;
	//allazw tis upoloipes sthles tou left pinaka
	for(int i=0;i<statsArray->cols[leftRelationId];i++){
		if(i!=leftColumnIndx){
			if(prev_d_A==0 || statsArray->array_with_stats[leftRelationId][i].d==0 || statsArray->array_with_stats[leftRelationId][i].f==0){
				statsArray->array_with_stats[leftRelationId][i].d=0;
				statsArray->array_with_stats[leftRelationId][i].f=0;
			}
			else{
				base=1-(1.0*statsArray->array_with_stats[leftRelationId][leftColumnIndx].d)/prev_d_A;
				power=(1.0*statsArray->array_with_stats[leftRelationId][i].f)/statsArray->array_with_stats[leftRelationId][i].d;
				statsArray->array_with_stats[leftRelationId][i].d=statsArray->array_with_stats[leftRelationId][i].d*(1-pow(base,power));
				statsArray->array_with_stats[leftRelationId][i].f=statsArray->array_with_stats[leftRelationId][leftColumnIndx].f;
			}
		}
	}
	//allazw olous tous pinakes sto team tou left
	for(int j=0;j<statsArray->rels;j++){
		if(j!=leftRelationId && teams[j]==teams[leftRelationId] && teams[j]!=0){
			for(int i=0;i<statsArray->cols[j];i++){
				if(i!=leftColumnIndx){
					if(prev_d_A==0 || statsArray->array_with_stats[j][i].d==0){
						statsArray->array_with_stats[j][i].d=0;
						statsArray->array_with_stats[j][i].f=0;
					}
					else{
						base=1-(1.0*statsArray->array_with_stats[leftRelationId][leftColumnIndx].d)/prev_d_A;
						power=(1.0*statsArray->array_with_stats[j][i].f)/statsArray->array_with_stats[j][i].d;
						statsArray->array_with_stats[j][i].d=statsArray->array_with_stats[j][i].d*(1-pow(base,power));
						statsArray->array_with_stats[j][i].f=statsArray->array_with_stats[j][leftColumnIndx].f;
					}
				}
			}
		}
	}
	//allazw tis upoloipes sthles tou right pinaka
	for(int i=0;i<statsArray->cols[rightRelationId];i++){
		if(i!=rightColumnIndx){
			if(prev_d_B==0 || statsArray->array_with_stats[rightRelationId][i].d==0 || statsArray->array_with_stats[rightRelationId][i].f==0){
				statsArray->array_with_stats[rightRelationId][i].d=0;
				statsArray->array_with_stats[rightRelationId][i].f=0;
			}
			else{
				base=1-(1.0*statsArray->array_with_stats[rightRelationId][rightColumnIndx].d)/prev_d_B;
				power=(1.0*statsArray->array_with_stats[rightRelationId][i].f)/statsArray->array_with_stats[rightRelationId][i].d;
				statsArray->array_with_stats[rightRelationId][i].d=statsArray->array_with_stats[rightRelationId][i].d*(1-pow(base,power));
				statsArray->array_with_stats[rightRelationId][i].f=statsArray->array_with_stats[rightRelationId][rightColumnIndx].f;
			}
		}
	}
	//allazw olous tous pinakes sto team tou right
	for(int j=0;j<statsArray->rels;j++){
		if(j!=rightRelationId && teams[j]==teams[rightRelationId] && teams[j]!=0){
			for(int i=0;i<statsArray->cols[j];i++){
				if(i!=rightColumnIndx){
					if(prev_d_B==0 || statsArray->array_with_stats[j][i].d==0){
						statsArray->array_with_stats[j][i].d=0;
						statsArray->array_with_stats[j][i].f=0;
					}
					else{
						base=1-(1.0*statsArray->array_with_stats[rightRelationId][rightColumnIndx].d)/prev_d_B;
						power=(1.0*statsArray->array_with_stats[j][i].f)/statsArray->array_with_stats[j][i].d;
						statsArray->array_with_stats[j][i].d=statsArray->array_with_stats[j][i].d*(1-pow(base,power));
						statsArray->array_with_stats[j][i].f=statsArray->array_with_stats[j][rightColumnIndx].f;
					}
				}
			}
		}
	}
}

void insertList(listnode **list,int cost,int *set,int size,int *teams,int teamCount,all_stats *stats,int *seira,int seira_size,int seira_point){
	if((*list)==NULL){
		(*list)=malloc(sizeof(listnode));
		(*list)->cost=cost;
		(*list)->set=malloc(size*sizeof(int));
		(*list)->teams=malloc(size*sizeof(int));
		
		(*list)->seira=malloc(seira_size*sizeof(int));
		for(int i=0;i<seira_size;i++){
			(*list)->seira[i]=seira[i];
		}
		(*list)->seira_point=seira_point;
		
		for(int i=0;i<size;i++){
			(*list)->set[i]=set[i];
			(*list)->teams[i]=teams[i];
		}
		(*list)->teamCount=teamCount;
		(*list)->local_stats=stats;
		(*list)->next=NULL;
	}
	else{
		listnode *temp;
		temp=(*list);
		while(temp->next!=NULL){
			temp=temp->next;
		}
		temp->next=malloc(sizeof(listnode));
		temp->next->cost=cost;
		temp->next->set=malloc(size*sizeof(int));
		temp->next->teams=malloc(size*sizeof(int));
		
		temp->next->seira=malloc(seira_size*sizeof(int));
		for(int i=0;i<seira_size;i++){
			temp->next->seira[i]=seira[i];
		}
		temp->next->seira_point=seira_point;
		
		for(int i=0;i<size;i++){
			temp->next->set[i]=set[i];
			temp->next->teams[i]=teams[i];
		}
		temp->next->teamCount=teamCount;
		temp->next->local_stats=stats;
		temp->next->next=NULL;
	}
}

void printList(listnode *list,int max,int max_seira){
	listnode *temp;
	temp=list;
	printf("LIST ITEMS:\n");
	while(temp!=NULL){
		printf("set : ");
		for(int i=0;i<max;i++){
			printf(" (%d) %d |",i,temp->set[i]);
		}
		printf("\ncost = %d\n",temp->cost);
		printf("\nteams : ");
		for(int i=0;i<max;i++){
			printf(" (%d) %d |",i,temp->teams[i]);
		}
		printf("\nteamCount = %d\n",temp->teamCount);
		printf("\nseira : ");
		for(int i=0;i<max_seira;i++){
			printf(" (%d) %d |",i,temp->seira[i]);
		}
		printf("\nseira_point = %d\n",temp->seira_point);
		printf("\nstats :");
		for(int i=0;i<temp->local_stats->rels;i++){
			printf("\nrel %d",i);
			for(int j=0;j<temp->local_stats->cols[i];j++){
				printf("\n\tcol %d l=%ld u=%ld f=%ld d=%ld",j,temp->local_stats->array_with_stats[i][j].l,temp->local_stats->array_with_stats[i][j].u,temp->local_stats->array_with_stats[i][j].f,temp->local_stats->array_with_stats[i][j].d);
			}
			printf("\n");
		}
		printf("-----------------------\n");

		temp=temp->next;
	}
	printf("\n");
}

void deleteBestTree(bestTree *tree){

}

void print_stats_function(all_stats *statsArray){
	for(int i=0;i<statsArray->rels;i++){
		printf("rel %d\n",i);
		for(int j=0;j<statsArray->cols[i];j++){
			printf("col %d l=%ld u=%ld f=%ld d=%ld\n",j,statsArray->array_with_stats[i][j].l,statsArray->array_with_stats[i][j].u,statsArray->array_with_stats[i][j].f,statsArray->array_with_stats[i][j].d);
		}
		printf("\n\n");
	}
	printf("--------------\n");
}



