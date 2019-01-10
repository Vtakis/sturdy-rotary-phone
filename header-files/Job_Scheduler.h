#ifndef JOB_SCHEDULER_H_
#define JOB_SCHEDULER_H_
#include <pthread.h>
#include <stdbool.h>
#include "../header-files/functions.h"

typedef struct Job Job;
typedef struct HistJob HistJob;
typedef struct PartitionJob PartitionJob;
typedef struct JoinJob JoinJob;
typedef struct Job_Scheduler Job_Scheduler;
typedef struct Queue Queue;
typedef struct thread_param thread_param;
typedef struct data data;

struct Queue
{
	Job* jobs;
	int size;
	int start;
	int end;
	int jobs_counter;
};

struct data
{
	hist **histArrayR;
	hist **histArrayS;
	oneColumnRelation **RR;
	oneColumnRelation **RS;
	resultList **resList;
};

struct Job_Scheduler
{
	int execution_threads;
	Queue* q;
	pthread_t* tids;
	data shared_data;
};

struct HistJob
{
	//oneColumnRelation *relSegment;
	int start;
	int end;
	char rel;
	hist* (*createHistArray)(oneColumnRelation **,int ,int );
	//int id;
};
struct PartitionJob
{
	int start;
	int end;
	char rel;
	oneColumnRelation* (*createReOrderedArray)(oneColumnRelation *array,hist *sumArray,int ,int,oneColumnRelation*);
};
struct JoinJob
{
	char rel;
	int32_t startR,endR;
	int32_t startS,endS;
	indexHT* (*createHashTable)(oneColumnRelation* reOrderedArray,int32_t start,int32_t end);
	void (*compareRelations)(indexHT *ht,oneColumnRelation *array,int32_t start,int32_t end,oneColumnRelation *hashedArray,resultList *resList,int32_t );
	void (*deleteHashTable)(indexHT **);
};

struct Job
{
	HistJob histjob;
	PartitionJob partitionjob;
	JoinJob joinjob;
	bool isEmpty;
	bool histFlag;
	bool partitionFlag;
	bool joinFlag;
};

struct thread_param
{
	Job_Scheduler* sch;
	int id;
	oneColumnRelation *R;
	oneColumnRelation *S;
	hist *HistR;
	hist *HistS;
};
struct thread_hist_param
{

};
struct thread_partition_param
{

};
struct thread_join_param
{

};
void sleep_producer(Job_Scheduler *job_scheduler,int);
void submit_Job(Job_Scheduler* schedule,Job Job);
Job initializeJob(char *type_of_job);		// type_of_job : "hist" , "partition" ,"join"//
void printjobs(Job_Scheduler* schedule);
Job_Scheduler* initialize_scheduler(int,oneColumnRelation*,oneColumnRelation*);
void *HistWorker(void* i);
void *PartitionWorker(void* i);
void *JoinWorker(void* i);
void submit_job(Job_Scheduler* schedule,Job* job,int join_id ,int partition_id ,int hist_id,oneColumnRelation *reOrderedArray , oneColumnRelation* array,
		int32_t start,int32_t end,int32_t num ,indexHT *ht,oneColumnRelation *relSegment,oneColumnRelation *relation,hist* histSum);
void execute_all_jobs(Job_Scheduler*,thread_param *,char **);
void wait_all_tasks_finish(Job_Scheduler* schedule);
void destroy_scheduler(Job_Scheduler* schedule);
void reset_queue(Queue *);
void execute_job(thread_param *,int);
void delete_threads(Job_Scheduler **);
#endif /* JOB_SCHEDULER_H_ */
