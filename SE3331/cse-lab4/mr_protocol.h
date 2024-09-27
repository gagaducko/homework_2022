#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>
#include <sys/types.h>
#include <dirent.h>
#include <stdio.h>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4


enum mr_type_task {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

int scanDir(string base, vector<string> &ret) {
    DIR * dir = opendir(base.c_str());
    struct dirent * rent;
    char str[100];
    while (rent = readdir(dir)) {
        strcpy(str, rent->d_name);
        if (str[0] != '.')
            ret.push_back(str);
    }
    closedir(dir);
    return ret.size();
}

int strHash(const string &str) {
    unsigned int hashVal = 0;
    for (char ch : str)
        hashVal = hashVal * 131 + (int) ch;
    return hashVal % REDUCER_COUNT;  // one reducer is responsible for this particular key
}

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		// Lab4: Your definition here.
		int type_task;
		string filename;
        int index;
        int nfiles;
	};

	friend marshall &operator<<(marshall &m, const AskTaskResponse &res) {
        return m << res.type_task << res.filename << res.index << res.nfiles;
    }

    friend unmarshall &operator>>(unmarshall &u, AskTaskResponse &res) {
        return u >> res.type_task >> res.filename >> res.index >> res.nfiles;
    }

	struct AskTaskRequest {
		// Lab4: Your definition here.
		int id;
	};

	struct SubmitTaskResponse {
		// Lab4: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab4: Your definition here.
	};

};

#endif

