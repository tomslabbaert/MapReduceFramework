#include <cstdio>
#include <stdlib.h>
#include <dirent.h>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <iostream>
#include <fstream>

#define NO_ARG_ERR "Usage: <substring to search> <folders, separated by space>"
#define THREADS_AMOUNT 1
#define SEARCH_KEY_INDEX 1

/**
 * a wrapper classes to wrap all the key types (a string in our program)
 */
class ClientKey : public k1Base, public k2Base, public k3Base
{
public:
	std::string fileName;
	ClientKey(std::string name){
		fileName = name;
	}
	bool operator < (const k1Base &other) const {
		ClientKey *k = (ClientKey*) &other;
		return ((ClientKey*)this)->fileName < k->fileName;
	}
	bool operator < (const k2Base &other) const {
		ClientKey *k = (ClientKey*) &other;
		return ((ClientKey*)this)->fileName < k->fileName;
	}
	bool operator < (const k3Base &other) const {
		ClientKey *k = (ClientKey*) &other;
		return ((ClientKey*)this)->fileName < k->fileName;
	}
};

/**
 * a wrapper classes to wrap v1base whice is a string
 */
class ClientValue : public v1Base
{
public:
	std::string searchWord;
	ClientValue(std::string toSearch){
		searchWord = toSearch;
	}
};

/**
 * a wrapper classes to wrap v2 and v3 base whice are an int number.
 * (found substring or not, 1 or 0).
 */
class ProgValue : public v2Base,public v3Base
{
public:
	unsigned long amount;
	ProgValue(unsigned long num){
		amount = num;
	};
};

/**
 * mapReduce class - overides the map and reduce function as required.
 */
class MY_MAP : public MapReduceBase {
	void Map(const k1Base *const key, const v1Base *const val) const {
		ClientKey *k = (ClientKey *) key;
		ClientValue *v = (ClientValue *) val;
		// if the filename contains the substring we send the pair to emit2.
		if (k->fileName.find(v->searchWord) != std::string::npos) {
			ProgValue *result = new ProgValue(1);
			ClientKey *tempKey = new ClientKey(k->fileName);
			Emit2(tempKey, result);
		}
	}

	void Reduce(const k2Base *const key, const V2_VEC &values) const {
		ClientKey *k = (ClientKey *) key;
		ProgValue *result = new ProgValue(values.size());
		ClientKey *tempKey = new ClientKey(k->fileName);
		Emit3(tempKey, result);
	}
};


/**
 * main, starts the entire Map Reduce process.
 */
int main(int argc, char* argv[]) {
	IN_ITEMS_VEC in_items_vec;
	OUT_ITEMS_VEC out_items_vec;
	MY_MAP MapReduce;
	ClientValue* tempVal;
	ClientKey* curFolder;
	if (argc <= 1) { // if invalid amount of args.
		fprintf(stderr, NO_ARG_ERR);
		exit(1);
		// iterates over all the directories and creates a v1 k1 vector
		// to be sent to the map reduce framework.
	} else if (argc > 2) {
		std::string toSearch = argv[SEARCH_KEY_INDEX];
		for (int i = 2; i < argc; i++) {
			DIR *dir;
			struct dirent *dirStream;
			if ((dir = opendir (argv[i])) != NULL) {
				while ((dirStream = readdir (dir)) != NULL) {
					curFolder = new ClientKey(dirStream->d_name);
					tempVal = new ClientValue(toSearch);
					in_items_vec.push_back(std::pair<k1Base*,v1Base*>(curFolder,
																	  tempVal));
				}
				closedir (dir);
			}
		}
		// calls the map reduce process, and saves the result in an out_item_vec
		out_items_vec = RunMapReduceFramework(MapReduce, in_items_vec,
											  THREADS_AMOUNT, true);
		// iterates over the result and prints them out as requested.
		for (unsigned int i = 0; i < out_items_vec.size(); i++) {
			for (unsigned int j = 0;
				 j < ((ProgValue*)((out_items_vec.at(i)).second))->amount; j++){
				if ((i == out_items_vec.size() - 1) &&
					(j == ((ProgValue*)
							((out_items_vec.at(i)).second))->amount) - 1)
				{
					std::cout << ((ClientKey*)
							((out_items_vec.at(i)).first))->fileName;
				}
				else {
					std::cout << ((ClientKey*)
							((out_items_vec.at(i)).first))->fileName << ' ';
				}
			}
		}
		std::cout << "" << std::endl;
		// deletes the K1V1K3V3 objects that were used through the program.
		for (unsigned int i = 0; i < in_items_vec.size(); i++) {
			delete(in_items_vec.at(i).first);
			delete(in_items_vec.at(i).second);
		}
		for (unsigned int i = 0; i < out_items_vec.size(); i++) {
			delete(out_items_vec.at(i).first);
			delete(out_items_vec.at(i).second);
		}
	}
}