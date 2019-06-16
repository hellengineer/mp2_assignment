/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

const int SP_MSG = -1; // indicates message is from stabilization protocol (running in  background) and not client operation


// need a structure which tracks all pending CRUD transactions for the current node.
// As any client side operation will make the client the coordinator as well, so I assume
// that the client will not fail during a transaction. 
// So we need to keep track of the state of in-flight transactions, and associate the 
// incoming messages from the servers with the right transactions.
// So introduce a class which handles the transaction status, add a transaction when a 
// client sends a message, and finish a transaction when enough servers have replied.

class TxStat {
	int id;
	int timestamp;
public:
	
	int repCnt;
	int sucCnt;
	string key;
	string val;
	MessageType mT;
	// constructor
	TxStat(int id, int timestamp, MessageType mT, string key, string val){
		this->id = id;
		this->timestamp = timestamp;
		this->repCnt = 0;
		this->sucCnt = 0;
		this->mT = mT;
		this->key = key;
		this->val = val;
	}
};
/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
	// in-flight transactions
	//<tx_id, tx_obj>
	map<int, TxStat*> txMap;
	//<tx_id, is_complete?>
	map<int, bool> txDn;


public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica, int txId);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

    // Destructor
	~MP2Node();

	//Own functions
	//reply from server to client
	void sendreply(string key, MessageType mT, bool success, Address* fromaddr, int txID, string content = ""); 
    Message makeMsg(MessageType mT, string key, string value);
    void makeTx(int txId, MessageType mT, string key, string value = "");
};

#endif /* MP2NODE_H_ */
