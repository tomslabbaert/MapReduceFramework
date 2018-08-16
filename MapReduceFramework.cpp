#include <pthread.h>
#include <cstdio>
#include <stdlib.h>
#include <map>
#include <thread>
#include <algorithm>
#include <iostream>
#include <semaphore.h>
#include <fstream>
#include <iomanip>
#include <sys/time.h>
#include "MapReduceFramework.h"
#define LOG_NAME ".MapReduceFramework.log"


sem_t shuff_sem;
pthread_mutex_t my_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t my_mutex2 = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond;


typedef std::pair<k2Base*, v2Base*> map_pair;
typedef std::vector<map_pair>* mapVec;
std::map<std::thread::id ,mapVec> multiMap; // a map for the ExecMap threads
// contains a thread ID as key, and a container for each thread to use.

typedef std::vector<std::pair<k3Base*, v3Base*>>* emit3Container;
std::map<std::thread::id ,emit3Container> emit3Map; // a map of containers
// for the reduce phase, contains thread ID as key and a private container
// for each of them.
std::map<std::thread::id ,pthread_mutex_t> mutexMap;

// a map for the shuffler to storage all data post shuffle.
std::map<k2Base*,V2_VEC*> shuffle_map;
// an iterator.
std::map<k2Base*,V2_VEC*>::iterator reduceIt;

MapReduceBase* mapReduce;
OUT_ITEMS_VEC output;
IN_ITEMS_VEC items;

//threads count that will follow how many current threads are alive.
int reduceThreadCount, execMapRunningThreads, MultiThreadlvl;
bool flag, flag2, toDelete;
size_t var_index;
timeval tim;

static std::ofstream log;
char buff[20];
struct tm *sTm;

void logTimedAction(std::string s){
	time_t now = time(0);
	sTm = gmtime (&now);
	strftime (buff, sizeof(buff), "%Y-%m-%d %H:%M:%S", sTm);
	log << (s);
	log << buff;
	log << (std::string("]\n"));
}

/**
 * this function receives an int, prints an error message and exits the process
 * if the result is negative.
 * ## to be used by system calls only through this program.
 * @param res an int number.
 */
void checkRes(int res){
	if(res < 0){
		printf("ERROR\n");
		log << std::string("MapReducedFrameWork finished\n");
		log.close();
		exit(-1);
	}
}



/**
 * this functions runs the "map" part of the mapReduce process
 * it runs until their are no more items to map.
 */
void execMap() {
	while(true){ // looping while their are still more items to map.
		checkRes(pthread_mutex_lock(&my_mutex));
		if(var_index >= items.size()){	// if theres no more items to map
			checkRes(pthread_mutex_unlock(&my_mutex));	// break the loop
			break;
		}
		IN_ITEM pair = items[var_index]; // else take the item at the curr
		var_index++;	// index and increase the counter by 1.
		checkRes(pthread_mutex_unlock(&my_mutex));
		mapReduce->Map(pair.first, pair.second); // send the pair to be mapped.
	}
	// once there are no more items are to be mapped.
	checkRes(pthread_mutex_lock(&my_mutex2));
	execMapRunningThreads--; // reduce the running thread counter by 1.
	if(!execMapRunningThreads){ // if its the last thread thats running
		flag = true;			// change the flag to true.
		checkRes(sem_post(&shuff_sem));
	}
	checkRes(pthread_mutex_unlock(&my_mutex2));
	logTimedAction(std::string("thread execMap terminated ["));
	pthread_exit(NULL); // exit the process.
}

/**
 * this function set ups the exec maps threads personal containers (vectors)
 * before they start their process in order to assure no errors are caused.
 * @return NULL.
 */
void* setup_ExecMap_threads(void*)
{
	checkRes(pthread_mutex_lock(&my_mutex2));
	std::thread::id  currID = std::this_thread::get_id();
	// creates a personal mutex for each thread and maps it, to be used later
	// by the shuffler to avoid reading from the same unlocked container.
	pthread_mutex_t tempMutex = PTHREAD_MUTEX_INITIALIZER;
	mutexMap[currID] = tempMutex;
	// creating a new empty vector and mapping it to the threads ID.
	std::vector<map_pair>* tempVec = new std::vector<map_pair>();
	multiMap[currID] = tempVec;
	checkRes(pthread_mutex_unlock(&my_mutex2));
	// locking the mutex by all threads to ensure that all initialize
	// successfully before their run starts.
	checkRes(pthread_mutex_lock(&my_mutex));
	checkRes(pthread_mutex_unlock(&my_mutex));
	execMap(); // calls the execMap function.
	return NULL;

}

/**
 * this function runs the shuffler part from the MapReduce concept
 * @return  null;
 */
void* shuffle(void*){
	bool isFound;
	// while Execmaps threads are still running or a flag has been raised
	// signaling the final shuffler run.
	while(execMapRunningThreads > 0 or flag) {
		checkRes(sem_wait(&shuff_sem));
		// iterate over the execmap map, shuffling each threads container
		// at a time.
		for (std::map<std::thread::id, std::vector<map_pair> *>::iterator it
				= multiMap.begin(); it != multiMap.end(); it++) {
			std::thread::id currID = it->first;
			checkRes(pthread_mutex_lock(&(mutexMap.find(currID)->second)));
			std::vector<map_pair> *v = multiMap[currID];
			if(v->size() ==0 ){
				checkRes(pthread_mutex_unlock(&(mutexMap.
						find(currID)->second)));
				continue;
			}
			// iterate over the threads container,containing key and value pairs
			for (unsigned long i = 0; i < v->size(); i++) {
				k2Base *currKey = v->at(i).first;
				v2Base *currVal = v->at(i).second;
				isFound = false;
				// searches the shuffle map to see if the key has already
				// been added in the past.
				for (std::map<k2Base *, std::vector<v2Base *>*>::iterator it2
						= shuffle_map.begin();
					 it2 != shuffle_map.end(); it2++) {
					// if the key was found, we add the val to the key's vector.
					if (!(*(it2->first) < *currKey) &&
						!(*currKey < *(it2->first))) {
						it2->second->push_back(currVal);
						isFound = true;
						if (toDelete) { // if the user asked to delete the k2
							// objects we delete the currKey as it will not
							// be used and tracking him will become difficult.
							delete currKey;
						}
						break;
					}
				}
				// if the key was not found then a new vector is created for it.
				if (!isFound) { // and the key,val pair pushed in.
					std::vector<v2Base *>* baseVec= new std::vector<v2Base *>();
					baseVec->push_back(currVal);
					shuffle_map[currKey] = baseVec;
				}
			}
			v->clear();//we clear the current container and we release the lock.
			checkRes(pthread_mutex_unlock(&(mutexMap.find(currID)->second)));
		}
		if(flag) // if a flag was signaled meaning the last exec map thread
		{			// finished then the shuffler goes on one last run.
			if(flag2) {
				flag2 = false;
				sem_post(&shuff_sem);
				continue;
			}
			break; // finally breaks the loop after its last run
		}
	}
	logTimedAction(std::string("thread Shuffle terminated ["));
	pthread_exit(NULL); // exits the process
}

/**
 * this function does the reduce action of the MapReduce concept.
 */
void execReduce(){
	while(true){ // while their are more items to reduce we loop.
		if(reduceIt == shuffle_map.end()){
			break;
		}
		pthread_mutex_lock(&my_mutex);
		std::vector<k2Base*> tempKeyVec;
		std::vector<std::vector<v2Base*>*> tempValVec;
		int counter = 0;
		// we take 10 items from the shuffle map at a time
		while(reduceIt != shuffle_map.end() && counter < 10) {
			tempKeyVec.push_back(reduceIt->first);
			tempValVec.push_back(reduceIt->second);
			counter++;
			reduceIt++;
		}
		pthread_mutex_unlock(&my_mutex);
		// then send those items to be Reduced by the reduce function.
		for(unsigned long i = 0; i < tempKeyVec.size(); i++){
			mapReduce->Reduce(tempKeyVec.at(i),*tempValVec.at(i));
		}
	}
	// once the loop ends the threads goes to terminate itself.
	checkRes(pthread_mutex_lock(&my_mutex));
	reduceThreadCount--;	// reducing the curr running thread count by 1.
	if(!reduceThreadCount){	// if it was the last reduce thread then a signal
		pthread_cond_signal(&cond);	// is sent to the main process to proceed.
	}
	checkRes(pthread_mutex_unlock(&my_mutex));
	logTimedAction(std::string("thread execReduce terminated ["));
	pthread_exit(NULL); // exits the process.
}


/**
 * this function set ups the reduce threads personal containers (vectors)
 * before they start their process in order to assure no errors are caused.
 * @return NULL.
 */
void* setup_reduce_threads(void *){
	checkRes(pthread_mutex_lock(&my_mutex2));
	std::thread::id  currID = std::this_thread::get_id();
	// sets up a new empty vector for each of the threads.
	std::vector<std::pair<k3Base*, v3Base*>>* tempVec =
			new std::vector<std::pair<k3Base*, v3Base*>>();
	emit3Map[currID] = tempVec;
	checkRes(pthread_mutex_unlock(&my_mutex2));
	checkRes(pthread_mutex_lock(&my_mutex)); // waits for all threads to finish.
	checkRes(pthread_mutex_unlock(&my_mutex));
	execReduce(); // starts the reduce process.
	return NULL;
}

/**
 * this function is called at the end of the program, ensures all the memory is
 * cleaned.
 */
void clearMemory(){
	// cleans the multi map used by the exec maps.
	for (std::map<std::thread::id, std::vector<map_pair> *>::iterator it
			= multiMap.begin();
		 it != multiMap.end(); it++) {
		delete it->second;
	}
	multiMap.clear();
	// cleans the cond,semaphore and mutex's used throughout the program.
	checkRes(sem_destroy(&shuff_sem));
	checkRes(pthread_mutex_destroy(&my_mutex));
	checkRes(pthread_mutex_destroy(&my_mutex2));
	checkRes(pthread_cond_destroy(&cond));
	for(std::map<std::thread::id ,pthread_mutex_t>::iterator it =
			mutexMap.begin(); it != mutexMap.end(); it++) {
		checkRes(pthread_mutex_destroy(&it->second));
	}
	mutexMap.clear();
	// if the user asked the K2 and V2 objects to be deleted by us
	// we take care of this here.
	if(toDelete){
		for (std::map<k2Base *, std::vector<v2Base *>*>::iterator it =
				shuffle_map.begin();
			 it != shuffle_map.end(); it++) {
			for(unsigned long i = 0; i < it->second->size(); i++){
				delete it->second->at(i);
			}
			it->second->clear();
			delete it->first;
		}
	}
	// lastly we clear 2 more containers.
	for (std::map<k2Base *, std::vector<v2Base *>*>::iterator it =
			shuffle_map.begin();
		 it != shuffle_map.end(); it++) {
		delete it->second;
		}
	shuffle_map.clear();
	for (std::map<std::thread::id ,emit3Container>::iterator it =
			emit3Map.begin();
		 it != emit3Map.end(); it++) {
		delete it->second;
	}
	emit3Map.clear();
}

/**
 * this is the main function that runs the entire MapReduce program.
 * @param base an objects with map and reduce functions.
 * @param itemsVec the input of pairs to be mapped.
 * @param multiThreadLevel the amount of threads to run simultaneously.
 * @param autoDeleteV2K2 a bool to signal if to delete k2 and v2 objects.
 * @return output, the map&reduced output after the run.
 */
OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& base, IN_ITEMS_VEC&
itemsVec,int multiThreadLevel, bool autoDeleteV2K2)
{
	log.open(LOG_NAME, std::ios::app | std::ios::out);
	log << std::string("RunMapReduceFramework created with ");
	log << multiThreadLevel;
	log << std::string(" threads\n");

	if(itemsVec.size() == 0){ // if their are no items to map just return
		log << std::string("MapReducedFrameWork finished\n");
		log.close();
		return output;
	}
	// initialize flags,counters and objects in need for the program.
	toDelete = autoDeleteV2K2;
	MultiThreadlvl = multiThreadLevel;
	flag = false;
	flag2 = true;
	var_index = 0;
	execMapRunningThreads = multiThreadLevel;
	reduceThreadCount = multiThreadLevel;
	pthread_t threads[multiThreadLevel + 1];
	items = itemsVec;
	checkRes(sem_init(&shuff_sem,0,0));
	mapReduce = &base;
	// locks a mutex in order to ensure the threads that are about to be created
	// wont start their run before they are all initialized correctly.
	checkRes(pthread_mutex_lock(&my_mutex));
	// create all the exec map threads.
	for(int t = 0; t < multiThreadLevel; t++)
	{
		logTimedAction(std::string("thread execMap created ["));
		checkRes(pthread_create(&threads[t], NULL, setup_ExecMap_threads,NULL));
	}
	// create the shuffle thread.
	logTimedAction(std::string("thread shuffler created ["));
	checkRes(pthread_create(&threads[multiThreadLevel], NULL, shuffle, NULL));
	// unlocks the mutex allowing all threads to start their run.
	checkRes(pthread_mutex_unlock(&my_mutex));
	gettimeofday(&tim,NULL);
	double t1 = tim.tv_usec;

	// main process waits for the shuffle thread to finish its run.
	checkRes(pthread_join(threads[multiThreadLevel], NULL));
	gettimeofday(&tim,NULL);
	double t2 = tim.tv_usec;
	double result = t2-t1;
	log << ("map and reduce took");
	log << result;
	log << ("ns\n");
	// once this happens we start the reduce process.
	reduceIt = shuffle_map.begin();
	pthread_cond_init(&cond,NULL);
	checkRes(pthread_mutex_lock(&my_mutex));
	// create all the reduce threads.
	for(int t = 0; t < multiThreadLevel; t++)
	{
		logTimedAction(std::string("thread execReduce created ["));
		checkRes(pthread_create(&threads[t], NULL, setup_reduce_threads, NULL ));
	}
	gettimeofday(&tim,NULL);
	t1 = tim.tv_usec;
	checkRes(pthread_mutex_unlock(&my_mutex));
	// main process waits for a signal (that would come from the last reduce
			// threads that's running just before it terminates).
	pthread_cond_wait(&cond, &my_mutex2);
	gettimeofday(&tim,NULL);
	t2 = tim.tv_usec;
	t2 = tim.tv_usec;
	log << ("map and reduce took ");
	 result = t2-t1;
	log << result;
	log << ("ns\n");
	// then we iterate over the reduced result and push the pairs
	// into the output vector.
	for(std::map<std::thread::id ,std::vector<std::pair<k3Base*,
			v3Base*>>*>::iterator it = emit3Map.begin(); it != emit3Map.end();
		it++) {
		for(unsigned int i = 0; i < it->second->size(); i++){
			output.push_back(it->second->at(i));
		}
	}
	// lastly some sugar syntax to sort the output vector as required.
	std::sort(output.begin(), output.end(),
			  [](const std::pair<k3Base*,v3Base*> &left,
				 const std::pair<k3Base*,v3Base*> &right) {
		return *left.first < *right.first;
	});
	clearMemory(); // cleans the memory used by the program.
	log << std::string("MapReducedFrameWork finished\n");
	log.close();
	return output;
}

/**
 * function used by the users map function to add items after the map act.
 * @param key a key.
 * @param val a value.
 */
void Emit2 (k2Base* key, v2Base* val){
	std::thread::id  currID = std::this_thread::get_id();
	checkRes(pthread_mutex_lock(&(mutexMap.find(currID)->second)));
	// adds the pair to the threads container.
	(multiMap.find(currID)->second)->push_back(std::pair<k2Base*,
			v2Base*>(key,val));
	checkRes(pthread_mutex_unlock(&(mutexMap.find(currID)->second)));
	checkRes(sem_post(&shuff_sem)); // lets the shuffler know theres work to do.
}

/**
 * function used by the users reduce to add pairs afters the reduce act.
 * @param key a key.
 * @param val a value.
 */
void Emit3 (k3Base* key, v3Base* val){
	std::thread::id  currID = std::this_thread::get_id();
	(emit3Map.find(currID)->second)->push_back(std::pair<k3Base*,
			v3Base*>(key,val));
}


