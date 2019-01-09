#include <pthread.h>
#include <stdio.h>
#include "../header-files/Job_Scheduler.h"
//#include "../header-files/functions.h"
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>


pthread_mutex_t start_mutex= PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t id_counter_mutex= PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t *read_mutex;
pthread_mutex_t *mutex;
pthread_cond_t *mutex_cond;
pthread_cond_t  write_cond=PTHREAD_COND_INITIALIZER;
pthread_cond_t  *read_cond;
pthread_cond_t wait_hist_cond= PTHREAD_COND_INITIALIZER;
pthread_mutex_t wait_hist_mutex= PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t job_counter_mutex= PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t write_to_histR= PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t write_to_histS= PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  new_cond=PTHREAD_COND_INITIALIZER;
pthread_mutex_t resList_counter_mutex= PTHREAD_MUTEX_INITIALIZER;

int ending;
int id_counter=0;
int resList_counter=0;
int threads_exited=0;
int beginning=0;
int broadcast_start=0;
pthread_mutex_t threads_exited_mutex=PTHREAD_MUTEX_INITIALIZER;
int threads_reached=0;
pthread_mutex_t trm= PTHREAD_MUTEX_INITIALIZER;
void *Worker(void* j)//sunartisi ton thread akolouthei to montelo tou katanoloti paragogou
{
    int thread_mutex_place=0;
	thread_param* x=(thread_param*)j;
	int start;
	ending=0;
	//printf("Eimai to thread %d or %ld kai ksekinaw\n",x->id,pthread_self());
	//x->sch->shared_data.histArrayR=malloc(x->sch->execution_threads*sizeof(hist*));
	//x->sch->shared_data.histArrayS=malloc(x->sch->execution_threads*sizeof(hist*));

	while(1)
	{
		pthread_mutex_lock( &start_mutex );//pairnei tin thesi tou buffer apo tin opoia tha parei douleia
		start=x->sch->q->start;
		thread_mutex_place=start;
		//printf("I am thread %d and get pos %d jobs=%d\n",x->id,thread_mutex_place,x->sch->q->jobs_counter);
		start=(start+1)%(x->sch->q->size);
		x->sch->q->start=start;
		pthread_mutex_unlock(&start_mutex);

		pthread_mutex_lock(&mutex[thread_mutex_place]);
		//printf("I am thread %d and leave pos %d\n",x->id,thread_mutex_place);
		while(x->sch->q->jobs[thread_mutex_place].isEmpty==true){			//oso h thesh pou exei parei ena thread sthn oura einai adeia tote perimenei sot condition variable//
			//printf("I am thread %d and waiting with position %d   ending=%d  and jobs %d\n",x->id,thread_mutex_place,ending,x->sch->q->jobs_counter);

			if(ending==1){
				//printf("Eimai to thread %d or %ld kai teleiwnw me thesh %d\n",x->id,pthread_self(),thread_mutex_place);
				/*pthread_mutex_lock(&threads_exited_mutex);
				threads_exited++;
				//pthread_cond_signal(&wait_hist_cond);
				pthread_mutex_unlock(&threads_exited_mutex);
				pthread_mutex_unlock(&mutex[thread_mutex_place]);*/
				//sleep(10);
				//return NULL;
				//exit(x->id);
				pthread_exit(&x->id);
			}
			pthread_cond_wait(&new_cond,&mutex[thread_mutex_place]);

			//printf("I am thread %d and woke up with position %d\n",x->id,thread_mutex_place);
			//sleep(10);
		}
		//printf("I am thread %d and i start work at pos %d\n",x->id,thread_mutex_place);
		//sleep(2);
		//x->sch->q->jobs_counter--;
		pthread_mutex_unlock(&mutex[thread_mutex_place]);

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
		else if(x->sch->q->jobs[thread_mutex_place].partitionFlag==true)
		{
			int start = x->sch->q->jobs[thread_mutex_place].partitionjob.start;
			int end = x->sch->q->jobs[thread_mutex_place].partitionjob.end;
			if(x->sch->q->jobs[thread_mutex_place].partitionjob.rel=='R')
			{
				x->sch->q->jobs[thread_mutex_place].partitionjob.createReOrderedArray(x->R,*(x->sch->shared_data.histArrayR),start,end,*(x->sch->shared_data.RR));
			}
			else
			{
				x->sch->q->jobs[thread_mutex_place].partitionjob.createReOrderedArray(x->S,*(x->sch->shared_data.histArrayS),start,end,*(x->sch->shared_data.RS));
			}
		}
		else if(x->sch->q->jobs[thread_mutex_place].joinFlag==true)
		{
			//printf("I Begin join work %d\n",thread_mutex_place);
			indexHT* ht;
			resultList *resList=initializeResultList();
			int startR = x->sch->q->jobs[thread_mutex_place].joinjob.startR;
			int endR = x->sch->q->jobs[thread_mutex_place].joinjob.endR;
			int startS = x->sch->q->jobs[thread_mutex_place].joinjob.startS;
			int endS = x->sch->q->jobs[thread_mutex_place].joinjob.endS;
			if(x->sch->q->jobs[thread_mutex_place].joinjob.rel=='R')
			{
				ht=createHashTable(*(x->sch->shared_data.RR),startR,endR);
				compareRelations(ht,*(x->sch->shared_data.RS),startS,endS,*(x->sch->shared_data.RR),resList,1);		// 1 -> hashedRr
				deleteHashTable(&ht);
				//printResults(resList);
			}
			else
			{
				ht=createHashTable(*(x->sch->shared_data.RS),startS,endS);
				compareRelations(ht,*(x->sch->shared_data.RR),startR,endR,*(x->sch->shared_data.RS),resList,0);		// 0 -> hashedRs
				deleteHashTable(&ht);
				//printResults(resList);
			}
			pthread_mutex_lock(&resList_counter_mutex);
			x->sch->shared_data.resList[resList_counter] = resList;
			resList_counter++;
			//printf("Listcounter=%d\n",resList_counter);
			pthread_mutex_unlock(&resList_counter_mutex);
		}
		//printf("1)Thread %d -- jobs %d\n",x->id,x->sch->q->jobs_counter);
		pthread_mutex_lock( &job_counter_mutex);
		x->sch->q->jobs_counter--;
		x->sch->q->jobs[thread_mutex_place].isEmpty=true;
		//printf("Thread %d wakes up central process with job counter %d\n",x->id,x->sch->q->jobs_counter);
		//pthread_cond_signal(&wait_hist_cond);
		//printf("Thread %d Reduce Jobs=%d\n",x->id,x->sch->q->jobs_counter);
		pthread_mutex_unlock(&job_counter_mutex);
		//printf("2)Thread %d -- jobs %d\n",x->id,x->sch->q->jobs_counter);
		//printf("Id=%d Counter=%d\n",x->id,x->sch->q->jobs_counter);
		//if(x->sch->q->jobs_counter==0)
		//sleep(2);
		if(x->sch->q->jobs_counter==0)
			pthread_cond_signal(&wait_hist_cond);



	}
}
void sleep_producer(Job_Scheduler *job_scheduler,int end)
{
	//printf("Mpika gia upno\n");
	pthread_mutex_lock(&job_counter_mutex);
	while(job_scheduler->q->jobs_counter>0)
	{
		//printf("Eimai h kentrikh diergasia kai perimenw %d\n",job_scheduler->q->jobs_counter);
		//printf("%d\n",job_scheduler->q->jobs_counter);
		pthread_cond_wait(&wait_hist_cond,&job_counter_mutex);
		//printf("Eimai h kentrikh diergasia kai ksupnhsa me %d\n",job_scheduler->q->jobs_counter);
	}
	pthread_mutex_unlock(&job_counter_mutex);
	//
	/*if(end==1)
	{
		ending=1;
		pthread_cond_broadcast(&new_cond);
		while(threads_exited!=job_scheduler->execution_threads)
		{
			//printf("threads exited=%d\n",threads_exited);
			ending=1;
			//pthread_cond_wait(&wait_hist_cond,&wait_hist_mutex);
			pthread_cond_broadcast(&new_cond);
		}
	}*/
	if(end==1)
	{
		//printf("Job Counter %d\n",job_scheduler->q->jobs_counter);
		//delete_threads(job_scheduler);
		ending=1;
		//printf("Ksupnaw ola ta threads\n");
		pthread_cond_broadcast(&new_cond);
		//printf("Ta ksuphsa\n");
		/*while(threads_exited!=job_scheduler->execution_threads)
		{
		}*/
		//delete_threads(&job_scheduler);
	}
	else
	{
		//printf("Job Finished\n");
	}
	resList_counter=0;

}
void delete_threads(Job_Scheduler** schedule)//katharizei ton jobscheduler
{
    int i;
	int err;
	//printf("deleted\n");
	//free((*schedule)->tids);
	//free((*schedule)->q->jobs);
	//free((*schedule)->q);
	/*for (i =0 ; i <(*schedule)->execution_threads ; i++)
	{
		if ( (err = pthread_join (((*schedule)->tids[i]) ,NULL)))
		{
			perror (" pthread_join " );
			exit (1) ;
		}
	}*/
	for(i=0;i<5000;i++)
	{
		pthread_mutex_destroy(&mutex[i]);
	}
	//free((*schedule));

	free(mutex);
	/*pthread_cond_destroy(&new_cond);
	pthread_cond_destroy(&wait_hist_cond);
	pthread_mutex_destroy(&start_mutex);
	pthread_mutex_destroy(&job_counter_mutex);
	pthread_mutex_destroy(&resList_counter_mutex);*/
}

void submit_Job(Job_Scheduler* schedule,Job *Job){
	pthread_mutex_lock(&mutex[schedule->q->end]);
	if((*Job).histFlag==true)
	{
		schedule->q->jobs[schedule->q->end]=*Job;
		schedule->q->jobs[schedule->q->end].histFlag=true;
		schedule->q->jobs[schedule->q->end].isEmpty=false;
		schedule->q->jobs[schedule->q->end].joinFlag=false;
		schedule->q->jobs[schedule->q->end].partitionFlag=false;
	}
	else if((*Job).partitionFlag==true)
	{
		schedule->q->jobs[schedule->q->end]=*Job;
		schedule->q->jobs[schedule->q->end].histFlag=false;
		schedule->q->jobs[schedule->q->end].isEmpty=false;
		schedule->q->jobs[schedule->q->end].joinFlag=false;
		schedule->q->jobs[schedule->q->end].partitionFlag=true;
	}
	else if((*Job).joinFlag==true)
	{
		schedule->q->jobs[schedule->q->end]=*Job;
		schedule->q->jobs[schedule->q->end].histFlag=false;
		schedule->q->jobs[schedule->q->end].isEmpty=false;
		schedule->q->jobs[schedule->q->end].joinFlag=true;
		schedule->q->jobs[schedule->q->end].partitionFlag=false;
	}
	pthread_mutex_lock( &job_counter_mutex );
	schedule->q->jobs_counter++;
	//printf("Increase Jobs=%d\n",schedule->q->jobs_counter);
	pthread_mutex_unlock( &job_counter_mutex );
	pthread_mutex_unlock(&mutex[schedule->q->end]);
	schedule->q->end=(schedule->q->end+1)%(schedule->q->size);
	//printf("Increase Jobs=%d\n",schedule->q->jobs_counter);
	//sleep(15);
	pthread_cond_broadcast(&new_cond);
	//printf("Ksupnhma gia douleia STHN THESH %d\n",schedule->q->end-1);
	//sleep(1);

}
void execute_job(thread_param *x,int thread_mutex_place)//voithitiki tou worker gia na ektelesei tin ergasia
{
  /*  char* result;
    int question_id;
    question_id=x->sch->q->jobs[thread_mutex_place].id;
    if(x->sch->q->jobs[thread_mutex_place].job_fun_st!=NULL)//an einai static
    {
        result=x->sch->q->jobs[thread_mutex_place].job_fun_st(x->indx,x->sch->q->jobs[thread_mutex_place].job_work,x->hash,x->id);
    }
    else//an einai dynamic
    {
        result=x->sch->q->jobs[thread_mutex_place].job_fun_dy(x->indx,x->sch->q->jobs[thread_mutex_place].job_work,x->hash,x->sch->q->jobs[thread_mutex_place].current_version);
    }
    if(strcmp(result,"-1"))
    {
        //free(result);
        x->buffer[question_id]=result;
    }
    else if(!strcmp(result,"-1"))
    {
        x->buffer[question_id]=malloc(3);
        strcpy(x->buffer[question_id],result);
    }
    free(x->sch->q->jobs[thread_mutex_place].job_work);
    strcpy(x->sch->q->jobs[thread_mutex_place].job_name,"-1");*/
}

Job_Scheduler* initialize_scheduler(int execution_threads,oneColumnRelation *R,oneColumnRelation *S)//arxikopoiisi JobScheduler
{
	//printf("Kanw INITIALIZE\n");
	Job_Scheduler* scheduler=malloc(sizeof(Job_Scheduler));
	pthread_t *workers;
	Queue* queue=malloc(sizeof(Queue));//dimiourgeia buffer
	queue->start=0;
	queue->end=0;
	queue->size=5000;
	queue->jobs_counter=0;
	queue->jobs=malloc(queue->size*sizeof(Job));
	ending=0;
	threads_exited=0;
	mutex=malloc(queue->size*sizeof(pthread_mutex_t));//dimiourgeia mutex gia kathe thesi tou pinaka
	//read_cond=malloc(queue->size*sizeof(pthread_cond_t));//kai ena condition variable gia kathe thesi tou pinaka
	int j;
	for(j=0;j<queue->size;j++)
	{
		pthread_mutex_init(&(mutex[j]),NULL);//arxikopoiisi ton mutex gia kathe thesi tou pinaka
		//pthread_cond_init(&(read_cond[j]),NULL);//arxikopoiisi ton condition variables gia kathe thesi tou pinaka
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
	temp1=malloc(scheduler->execution_threads*sizeof(thread_param));
	//temp1=*temp;

    for(i=0;i<scheduler->execution_threads;i++)//dimiourgia ton thread
	{
		temp1[i].sch=scheduler;
		temp1[i].id=i;
		temp1[i].R=R;
		temp1[i].S=S;
		temp1[i].HistR=NULL;
		temp1[i].HistS=NULL;
		if ((err=pthread_create( (scheduler->tids)+i  , NULL , Worker ,(void*)(temp1+i))))		/*ftiaxnw ta threads workers*/
		{
			perror (" pthread_create " );
			exit (1) ;
		}
	}
	//printf("EPISTREFW\n");
	return scheduler;
}
Job* initializeJob(char *type_of_job)		// type_of_job : "hist" , "partition" ,"join"//
{
	Job *job;
	job=malloc(sizeof(Job));
	if(!strcmp(type_of_job,"hist"))
	{
        job->histFlag=true;
        job->joinFlag=false;
        job->partitionFlag=false;
        job->histjob.createHistArray = createHistArray;
	}
	else if(!strcmp(type_of_job,"partition"))
	{
        job->histFlag=false;
        job->joinFlag=false;
        job->partitionFlag=true;
        job->partitionjob.createReOrderedArray=createReOrderedArray;
	}
	else if(!strcmp(type_of_job,"join"))
	{
        job->histFlag=false;
        job->joinFlag=true;
        job->partitionFlag=false;
        job->joinjob.compareRelations = compareRelations;
        job->joinjob.createHashTable = createHashTable;
        job->joinjob.deleteHashTable = deleteHashTable;
	}

	return job;
}

void execute_all_jobs(Job_Scheduler* schedule,thread_param *temp,char **buf_res)
{
    int i;
    threads_exited=0;
    if(!beginning)//an vriskomaste stin arxi tou programmatos kai einai i proti fora pou exei kleithei i sunartisi perimenoume mexri na dimiourgithoun ta thread kai na koimithoun giaproti fora
    {

        int flag1;
        flag1=0;
        while(1)
        {
            pthread_mutex_lock( &(trm));
            if(threads_reached==schedule->execution_threads)
            {
                flag1=1;
            }
            pthread_mutex_unlock( &(trm));
            if(flag1)
            {
                break;
            }
        }
    }
    beginning=beginning+1;
    i=0;
    for(i=0;i<5000;i++)//ta afupnoume
    {
        pthread_cond_broadcast(&(read_cond[i]));
    }
	return;
}

/*void wait_all_tasks_finish(Job_Scheduler* schedule)//perimenei na teleiosoun ta thread kai na tou steiloun sima na sunexisei
{
    pthread_mutex_lock(&init);
    pthread_cond_wait(&end_cond,&init);
    pthread_mutex_unlock(&init);
}*/
void destroy_scheduler(Job_Scheduler* schedule)
{
	free(schedule->q->jobs);
	free(schedule->tids);
}


void reset_queue(Queue *q)//arxikopoiei tin oura
{
	broadcast_start=q->end;
	q->start=0;
	q->end=0;
	id_counter=0;
}
/*submit_job(Job_Scheduler* schedule,Job* job,int join_id ,int partition_id ,int hist_id,oneColumnRelation *reOrderedArray , oneColumnRelation* array,
		int32_t start,int32_t end,int32_t num ,indexHT *ht,oneColumnRelation *relSegment,oneColumnRelation *relation,hist* histSum)//upovoli mias ergasias ston buffer
{
	int i;
    if(schedule->q->end==schedule->q->size)//an gemisei o buffer tou diplasiazoume to megethos
    {
        schedule->q->size=schedule->q->size*2;
        schedule->q->jobs=realloc(schedule->q->jobs,schedule->q->size*sizeof(Job));
        int j;
        for(j=schedule->q->end;j<schedule->q->size;j++)
        {
            schedule->q->jobs[j].histFlag=false;
            schedule->q->jobs[j].partitionFlag=false;
            schedule->q->jobs[j].joinFlag=false;
            schedule->q->jobs[j].histjob=NULL;
            schedule->q->jobs[j].partitionjob=NULL;
            schedule->q->jobs[j].joinjob=NULL;
        }
    }
    if(job->histFlag==true)
    {
    	//to do
    }
    else if(job->partitionFlag==true)
    {
    	//to do
	schedule->q->jobs[schedule->q->end]=job;
	schedule->q->jobs[schedule->q->end].joinjob.reOrderedArray = reOrderedArray;//
        schedule->q->jobs[schedule->q->end].joinjob.array = array;
	//schedule->q->jobs[schedule->q->end].joinjob.histSum=histSum;

    }
    else if(job->joinFlag==true)
    {
        schedule->q->jobs[schedule->q->end]=job;
        schedule->q->jobs[schedule->q->end].joinjob.start = start;
        schedule->q->jobs[schedule->q->end].joinjob.end = end;
        schedule->q->jobs[schedule->q->end].joinjob.num = num;
        schedule->q->jobs[schedule->q->end].joinjob.reOrderedArray = reOrderedArray;
        schedule->q->jobs[schedule->q->end].joinjob.ht = ht;
        schedule->q->jobs[schedule->q->end].joinjob.array = array;
        schedule->q->jobs[schedule->q->end].joinjob.id=join_id;
    	schedule->q->jobs[schedule->q->end].joinjob.compareRelations = compareRelations;
    	schedule->q->jobs[schedule->q->end].joinjob.createHashTable = createHashTable;
    	schedule->q->jobs[schedule->q->end].joinjob.deleteHashTable = deleteHashTable;
    }
    schedule->q->end=schedule->q->end+1;
}*/
