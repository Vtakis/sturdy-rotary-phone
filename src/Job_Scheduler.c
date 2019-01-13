#include <pthread.h>
#include <stdio.h>
#include "../header-files/Job_Scheduler.h"
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>


pthread_mutex_t start_mutex= PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t *worker_mutex;
pthread_cond_t producer_wait_cond= PTHREAD_COND_INITIALIZER;
pthread_mutex_t job_counter_mutex= PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  worker_wait_cond=PTHREAD_COND_INITIALIZER;
pthread_mutex_t resList_counter_mutex= PTHREAD_MUTEX_INITIALIZER;

int ending;
int resList_counter=0;

void *Worker(void* j)
{
    int thread_mutex_place=0;
	thread_param* x=(thread_param*)j;
	int start;
	while(1)
	{
		pthread_mutex_lock( &start_mutex );//pairnei tin thesi tou buffer apo tin opoia tha parei douleia
		start=x->sch->q->start;
		thread_mutex_place=start;
		start=(start+1)%(x->sch->q->size);
		x->sch->q->start=start;
		pthread_mutex_unlock(&start_mutex);

		pthread_mutex_lock(&worker_mutex[thread_mutex_place]);
		while(x->sch->q->jobs[thread_mutex_place].isEmpty==true){			//oso h thesh pou exei parei ena thread sthn oura einai adeia tote perimenei sto condition variable//
			if(ending==1)
			{
				pthread_mutex_unlock(&worker_mutex[thread_mutex_place]);
				pthread_cond_signal(&worker_wait_cond);
				pthread_exit(0);
			}
			pthread_cond_wait(&worker_wait_cond,&worker_mutex[thread_mutex_place]);

		}
		pthread_mutex_unlock(&worker_mutex[thread_mutex_place]);
		//an h douleia sthn thesh thread_mutex_place einai tupou hist
		if(x->sch->q->jobs[thread_mutex_place].histFlag==true)
		{
			int start = x->sch->q->jobs[thread_mutex_place].histjob.start;
			int end = x->sch->q->jobs[thread_mutex_place].histjob.end;
			if(x->sch->q->jobs[thread_mutex_place].histjob.rel=='R')
			{
				x->sch->shared_data.histArrayR[thread_mutex_place%x->sch->execution_threads]=x->sch->q->jobs[thread_mutex_place].histjob.createHistArray(&(x->R),start,end);
			}
			else
			{
				x->sch->shared_data.histArrayS[thread_mutex_place%x->sch->execution_threads]=x->sch->q->jobs[thread_mutex_place].histjob.createHistArray(&(x->S),start,end);
			}
		}
		else if(x->sch->q->jobs[thread_mutex_place].partitionFlag==true)		//an h douleia sthn thesh thread_mutex_place einai tupou partition
		{
			int start = x->sch->q->jobs[thread_mutex_place].partitionjob.start;
			int end = x->sch->q->jobs[thread_mutex_place].partitionjob.end;
			//kaloume thn createReOrderedArray gia na mas ftiaksei ton R' kai S'
			if(x->sch->q->jobs[thread_mutex_place].partitionjob.rel=='R')
			{
				x->sch->q->jobs[thread_mutex_place].partitionjob.createReOrderedArray(x->R,*(x->sch->shared_data.histArrayR),start,end,*(x->sch->shared_data.RR));
			}
			else
			{
				x->sch->q->jobs[thread_mutex_place].partitionjob.createReOrderedArray(x->S,*(x->sch->shared_data.histArrayS),start,end,*(x->sch->shared_data.RS));
			}
		}
		else if(x->sch->q->jobs[thread_mutex_place].joinFlag==true)		//an h douleia sthn thesh thread_mutex_place einai tupou join
		{
			indexHT* ht;
			resultList *resList=initializeResultList();
			int startR = x->sch->q->jobs[thread_mutex_place].joinjob.startR;
			int endR = x->sch->q->jobs[thread_mutex_place].joinjob.endR;
			int startS = x->sch->q->jobs[thread_mutex_place].joinjob.startS;
			int endS = x->sch->q->jobs[thread_mutex_place].joinjob.endS;

			//dhmiourgia hashtable gia to katallhlo bucket ,sugkrish twn 2 bucket kai ustera eisagwgh sthn lista
			if(x->sch->q->jobs[thread_mutex_place].joinjob.rel=='R')
			{
				ht=createHashTable(*(x->sch->shared_data.RR),startR,endR);
				compareRelations(ht,*(x->sch->shared_data.RS),startS,endS,*(x->sch->shared_data.RR),resList,1);		// 1 -> hashedRr
				deleteHashTable(&ht);
			}
			else
			{
				ht=createHashTable(*(x->sch->shared_data.RS),startS,endS);
				compareRelations(ht,*(x->sch->shared_data.RR),startR,endR,*(x->sch->shared_data.RS),resList,0);		// 0 -> hashedRs
				deleteHashTable(&ht);
			}
			pthread_mutex_lock(&resList_counter_mutex);
			x->sch->shared_data.resList[resList_counter] = resList;		//pername ston pinaka me tis listes thn lista pou ftiaxthke parapanw kai meta tha tis enwsoume oles mazi
			resList_counter++;
			pthread_mutex_unlock(&resList_counter_mutex);
		}
		pthread_mutex_lock( &job_counter_mutex);
		x->sch->q->jobs_counter--;
		x->sch->q->jobs[thread_mutex_place].isEmpty=true;
		pthread_mutex_unlock(&job_counter_mutex);

		pthread_cond_signal(&producer_wait_cond);
	}
}
void sleep_producer(Job_Scheduler *job_scheduler,int end)		//sunarthsh pou kaleitai apo thn radix kai blockarei thn leitourgia ths kentrikhs diergasias ews otou teleiwsoun oles oi douleies
{
	pthread_mutex_lock(&job_counter_mutex);
		while(job_scheduler->q->jobs_counter>0)
		{
			pthread_cond_wait(&producer_wait_cond,&job_counter_mutex);
		}
	pthread_mutex_unlock(&job_counter_mutex);
	//an to end einai iso me 1 tote shmainei oti h teleutaia douleia pou dothike htan join kai den exoume alles douleies .ara termatizoume ta threads
	if(end==1)
	{
		ending=1;
		delete_threads(&job_scheduler);
	}
	resList_counter=0;
}
void delete_threads(Job_Scheduler** schedule)		//katharizei ton jobscheduler
{
    int i;
	int err;
	for (i =0 ; i <(*schedule)->execution_threads ; i++)		//perimenoume na teleiwsoun ola ta threads thn leitourgia tous
	{
		pthread_cond_broadcast(&worker_wait_cond);				//ksupname gia teleutaia fora osa threads exoun meinei ston condition variable
		if ( (err = pthread_join (((*schedule)->tids[i]) ,NULL)))
		{
			perror (" pthread_join " );
			exit (1) ;
		}
	}
	for(i=0;i<5000;i++)			//katastrofh tou mutex ths ouras
	{
		pthread_mutex_destroy(&worker_mutex[i]);
	}

	free((*schedule)->tids);		//diagrafh tou job scheduler
	free((*schedule)->q->jobs);
	free((*schedule)->q);
	free((*schedule));
	free(worker_mutex);
}

void submit_Job(Job_Scheduler* schedule,Job Job){			//upovolh douleias ston job scheduler

	pthread_mutex_lock(&worker_mutex[schedule->q->end]);
		if((Job).histFlag==true)							//upovolh hist douleias ston job scheduler
		{
			schedule->q->jobs[schedule->q->end]=Job;
			schedule->q->jobs[schedule->q->end].histFlag=true;
			schedule->q->jobs[schedule->q->end].isEmpty=false;
			schedule->q->jobs[schedule->q->end].joinFlag=false;
			schedule->q->jobs[schedule->q->end].partitionFlag=false;
		}
		else if((Job).partitionFlag==true)					//upovolh partition douleias ston job scheduler
		{
			schedule->q->jobs[schedule->q->end]=Job;
			schedule->q->jobs[schedule->q->end].histFlag=false;
			schedule->q->jobs[schedule->q->end].isEmpty=false;
			schedule->q->jobs[schedule->q->end].joinFlag=false;
			schedule->q->jobs[schedule->q->end].partitionFlag=true;
		}
		else if((Job).joinFlag==true)						//upovolh join douleias ston job scheduler
		{
			schedule->q->jobs[schedule->q->end]=Job;
			schedule->q->jobs[schedule->q->end].histFlag=false;
			schedule->q->jobs[schedule->q->end].isEmpty=false;
			schedule->q->jobs[schedule->q->end].joinFlag=true;
			schedule->q->jobs[schedule->q->end].partitionFlag=false;
		}
		pthread_mutex_lock( &job_counter_mutex );			//mutex gia ton metrhth twn douleiwn
			schedule->q->jobs_counter++;					//aukshsh metrhth douleiwn tou job scheduler
		pthread_mutex_unlock( &job_counter_mutex );
	pthread_mutex_unlock(&worker_mutex[schedule->q->end]);

	schedule->q->end=(schedule->q->end+1)%(schedule->q->size);		//allagh tou deikth end sthn teleutaia thesh ths ouras pou uparxei douleia
	pthread_cond_broadcast(&worker_wait_cond);						//ksupname ola ta threads pou perimenoun na paroun kapoia douleia
}
Job_Scheduler* initialize_scheduler(int execution_threads,oneColumnRelation *R,oneColumnRelation *S)//arxikopoihsh tou JobScheduler
{
	Job_Scheduler* scheduler=malloc(sizeof(Job_Scheduler));
	pthread_t *workers;
	Queue* queue=malloc(sizeof(Queue)); //dimiourgeia buffer ouras
	queue->start=0;
	queue->end=0;
	queue->size=5000;
	queue->jobs_counter=0;
	queue->jobs=malloc(queue->size*sizeof(Job));
	ending=0;
	worker_mutex=malloc(queue->size*sizeof(pthread_mutex_t));//dimiourgeia worker_mutex gia kathe thesi tou pinaka
	int j;
	for(j=0;j<queue->size;j++)
	{
		pthread_mutex_init(&(worker_mutex[j]),NULL);//arxikopoiisi ton worker_mutex gia kathe thesi tou pinaka
	}
	for(j=0;j<queue->size;j++)
		queue->jobs[j].isEmpty=true;
	scheduler->execution_threads=execution_threads;

	if (( workers = malloc ( execution_threads * sizeof ( pthread_t ))) == NULL )
	{
		perror ( " malloc ") ;
		exit (1) ;
	}
	scheduler->shared_data.histArrayR=malloc(execution_threads*sizeof(hist*));
	scheduler->shared_data.histArrayS=malloc(execution_threads*sizeof(hist*));
	int i;
	scheduler->q=queue;
	scheduler->tids=workers;
	int err;
	void *Worker(void*) ;
	thread_param *temp1;
	temp1=malloc(scheduler->execution_threads*sizeof(thread_param));		//proswrinh metavlhth sthn opoia kratame ta dedomena pou xreiazontai
																			//ta threads gia na ektelesoun tis douleies tous
    for(i=0;i<scheduler->execution_threads;i++)//dimiourgia ton thread
	{
		temp1[i].sch=scheduler;
		temp1[i].id=i;
		temp1[i].R=R;
		temp1[i].S=S;
		if ((err=pthread_create( (scheduler->tids)+i  , NULL , Worker ,(void*)(temp1+i))))		/*ftiaxnw ta threads workers*/
		{
			perror (" pthread_create " );
			exit (1) ;
		}
	}
	return scheduler;
}
Job initializeJob(char *type_of_job)		// type_of_job : "hist" , "partition" ,"join"//
{
	Job job;
	if(!strcmp(type_of_job,"hist"))
	{
        job.histFlag=true;
        job.joinFlag=false;
        job.partitionFlag=false;
        job.histjob.createHistArray = createHistArray;
	}
	else if(!strcmp(type_of_job,"partition"))
	{
        job.histFlag=false;
        job.joinFlag=false;
        job.partitionFlag=true;
        job.partitionjob.createReOrderedArray=createReOrderedArray;
	}
	else if(!strcmp(type_of_job,"join"))
	{
        job.histFlag=false;
        job.joinFlag=true;
        job.partitionFlag=false;
        job.joinjob.compareRelations = compareRelations;
        job.joinjob.createHashTable = createHashTable;
        job.joinjob.deleteHashTable = deleteHashTable;
	}
	return job;
}
