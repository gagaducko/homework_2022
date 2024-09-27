#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string>
#include <vector>
#include <mutex>

#include "mr_protocol.h"
#include "rpc.h"

using namespace std;

struct Task {
	int type_task;     // should be either Mapper or Reducer
	bool isAssigned;  // has been assigned to a worker
	bool isCompleted; // has been finised by a worker
	int index;        // index to the file
};

class Coordinator {
public:
	Coordinator(const vector<string> &files, int nReduce);
	mr_protocol::status askTask(int, mr_protocol::AskTaskResponse &reply);
	mr_protocol::status submitTask(int type_task, int index, bool &success);
	bool isFinishedMap();
	bool isFinishedReduce();
	bool Done();

	// Your code is here
	bool task_assign(Task &task);
	bool compCount_less_mapTasksSize(Task &task);
	bool compCount_more_mapTasksSize(Task &task);


private:
	vector<string> files;
	vector<Task> mapTasks;
	vector<Task> reduceTasks;

	mutex mtx;

	long completedMapCount;
	long completedReduceCount;
	bool isFinished;
	
	string getFile(int index);
};


bool Coordinator::compCount_less_mapTasksSize(Task &task) {
	bool found = false;
	int size_mapTasks = (int) this->mapTasks.size();
    for (int i = 0; i < size_mapTasks; i++) {
        Task &thisTask = this->mapTasks[i];
        if (!thisTask.isCompleted && !thisTask.isAssigned) {
            task = thisTask;
            thisTask.isAssigned = true;
            found = true;
            break;
        }
    }
	return found;
}


bool Coordinator::compCount_more_mapTasksSize(Task &task) {
	bool found = false;
	int size_reduceTaks = (int) this->reduceTasks.size();
    for (int i = 0; i < size_reduceTaks; i++) {
        Task &thisTask = this->reduceTasks[i];
        if (!thisTask.isCompleted && !thisTask.isAssigned) {
            task = thisTask;
            thisTask.isAssigned = true;
            found = true;
            break;
        }
    }
	return found;
}


// Your code here -- RPC handlers for the worker to call.


bool Coordinator::task_assign(Task &task) {
    task.type_task = NONE;
	long size = long(this->mapTasks.size());
    bool found = false;
    this->mtx.lock();


    if (this->completedMapCount < size) {
		found = compCount_less_mapTasksSize(task);
    } else {
		found = compCount_more_mapTasksSize(task);
    }

    this->mtx.unlock();
    return found;
}


mr_protocol::status Coordinator::askTask(int, mr_protocol::AskTaskResponse &reply) {
	// Lab4 : Your code goes here.
	Task availableTask;
    if (task_assign(availableTask)) {
        reply.index = availableTask.index;
        reply.type_task = (mr_type_task) availableTask.type_task;
        reply.filename = getFile(reply.index);
        reply.nfiles = files.size();
    } else {
        reply.index = -1;
        reply.type_task = NONE;
        reply.filename = "";
    }
	return mr_protocol::OK;
}

mr_protocol::status Coordinator::submitTask(int type_task, int index, bool &success) {
	// Lab4 : Your code goes here.
	mtx.lock();
	long mapsize = (long) mapTasks.size();
	long reducesize = (long) reduceTasks.size();
	if(type_task == MAP) {
		mapTasks[index].isCompleted = true;
        mapTasks[index].isAssigned = false;
        this->completedMapCount++;
	}
	if(type_task == REDUCE) {
		reduceTasks[index].isCompleted = true;
        reduceTasks[index].isAssigned = false;
        this->completedReduceCount++;
	}
    if (this->completedMapCount >= mapsize && this->completedReduceCount >= reducesize) {
        this->isFinished = true;
    }
    mtx.unlock();
    success = true;
	return mr_protocol::OK;
}


string Coordinator::getFile(int index) {
	this->mtx.lock();
	string file = this->files[index];
	this->mtx.unlock();
	return file;
}

bool Coordinator::isFinishedMap() {
	bool isFinished = false;
	this->mtx.lock();
	if (this->completedMapCount >= long(this->mapTasks.size())) {
		isFinished = true;
	}
	this->mtx.unlock();
	return isFinished;
}

bool Coordinator::isFinishedReduce() {
	bool isFinished = false;
	this->mtx.lock();
	if (this->completedReduceCount >= long(this->reduceTasks.size())) {
		isFinished = true;
	}
	this->mtx.unlock();
	return isFinished;
}

//
// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
//
bool Coordinator::Done() {
	bool r = false;
	this->mtx.lock();
	r = this->isFinished;
	this->mtx.unlock();
	return r;
}

//
// create a Coordinator.
// nReduce is the number of reduce tasks to use.
//
Coordinator::Coordinator(const vector<string> &files, int nReduce)
{
	this->files = files;
	this->isFinished = false;
	this->completedMapCount = 0;
	this->completedReduceCount = 0;

	int filesize = files.size();
	for (int i = 0; i < filesize; i++) {
		this->mapTasks.push_back(Task{mr_type_task::MAP, false, false, i});
	}
	for (int i = 0; i < nReduce; i++) {
		this->reduceTasks.push_back(Task{mr_type_task::REDUCE, false, false, i});
	}
}

int main(int argc, char *argv[])
{
	int count = 0;

	if(argc < 3){
		fprintf(stderr, "Usage: %s <port-listen> <inputfiles>...\n", argv[0]);
		exit(1);
	}
	char *port_listen = argv[1];
	
	setvbuf(stdout, NULL, _IONBF, 0);

	char *count_env = getenv("RPC_COUNT");
	if(count_env != NULL){
		count = atoi(count_env);
	}

	vector<string> files;
	char **p = &argv[2];
	while (*p) {
		files.push_back(string(*p));
		++p;
	}

	rpcs server(atoi(port_listen), count);

	Coordinator c(files, REDUCER_COUNT);
	
	//
	// Lab4: Your code here.
	// Hints: Register "askTask" and "submitTask" as RPC handlers here
	// 
	server.reg(mr_protocol::asktask, &c, &Coordinator::askTask);
    server.reg(mr_protocol::submittask, &c, &Coordinator::submitTask);

	while(!c.Done()) {
		sleep(1);
	}

	return 0;
}


