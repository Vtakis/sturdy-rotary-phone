#ifndef JOB_SCHEDULER_H_
#define JOB_SCHEDULER_H_
#include <pthread.h>
#include <stdbool.h>

typedef struct Job Job;
typedef struct HistJob HistJob;
typedef struct PartitionJob PartitionJob;
typedef struct JoinJob JoinJob;
typedef struct Job_Scheduler Job_Scheduler;
typedef struct Queue Queue;
typedef struct thread_param thread_param;
struct Queue
{
	Job* jobs;
	int size;
	int start;
	int end;
};
struct Job_Scheduler
{
	int execution_threads;
	Queue* q;
	pthread_t* tids;
};
struct Job
{
	HistJob histjob;
	PartitionJob partitionjob;
	JoinJob joinjob;
	bool histFlag;
	bool partitionFlag;
	bool joinFlag;
};
struct HistJob
{
	oneColumnRelation *relSegment;
	int id;
};
struct PartitionJob
{
	//oneColumnRelation* (createReOrderedArray)(oneColumnRelation *array,hist *sumArray);
	oneColumnRelation *array;
	hist* histSum;
};
struct JoinJob
{
	int id;
	oneColumnRelation *reOrderedArray,*array;
	int32_t start,end,num;
	indexHT *ht;
	indexHT* (*createHashTable)(oneColumnRelation* reOrderedArray,int32_t start,int32_t end);
	void (*compareRelations)(indexHT *ht,oneColumnRelation *array,int32_t start,int32_t end,oneColumnRelation *hashedArray,resultList *resList,int32_t );
	void (*deleteHashTable)(indexHT **);
};
struct thread_param
{
	Job_Scheduler* sch;
	int id;
	hash_trie* indx;
	hash_keeper* hash;
    char **buffer;
    int flagslen;
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
void initializeJob(char *type_of_job,Job *job);		// type_of_job : "hist" , "partition" ,"join"//
void printjobs(Job_Scheduler* schedule);
Job_Scheduler* initialize_scheduler(int,int,thread_param **);
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
void delete_threads(Job_Scheduler **,thread_param **);
#endif /* JOB_SCHEDULER_H_ */
