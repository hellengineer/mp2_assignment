/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
	// clean up transaction map
	auto it = txMap.begin();
	while(it != txMap.end()) {
		delete it->second;
		it++;
	}
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	//check ring against curMemList and decide if anything has changed
	if (ring.size() != curMemList.size())
		change = true;
	else if (ring.size() > 0){
		for (auto i = 0; i < ring.size(); i++ ){
			if (curMemList[i].getHashCode() != ring[i].getHashCode()){
				change = true; // atleast some difference between ring and the current membership list.
				break;
			}
		}
	}
	ring = curMemList; // update ring 

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if (change)
		stabilizationProtocol();
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}


//-----------------------------------------------------------------------
//   Helper and wrapper functions
// makes a transaction object and adds it to the txMap map at this node
void MP2Node::makeTx(int txId, MessageType mT, string key, string value){
	int timestamp = this->par->getcurrtime();
	TxStat* trans = new TxStat(txId, timestamp, mT, key, value);
	this->txMap.emplace(txId, trans);
}

// returns a message object based on the message type and key value pair
// also adds a transaction object onto the transaction status map for this node
// basically a wrapper for Message constructor + transaction maker.

Message MP2Node::makeMsg(MessageType mT, string key, string value=""){
	int txId = g_transID; // get global transaction id
	makeTx(txId, mT, key, value);
	if(mT == CREATE || mT == UPDATE){
		Message msg(txId, this->memberNode->addr, mT, key, value);
		return msg;
	}
	else if(mT == READ || mT == DELETE){
		Message msg(txId, this->memberNode->addr, mT, key);
		return msg;
	}
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
	// make message
	Message msg = makeMsg(MessageType::CREATE, key, value);
	string message = msg.toString();

	auto replicas = findNodes(key); // get replicas for this key 
	// send a message to each replica
	for (auto it = replicas.begin(); it != replicas.end(); it ++){
		emulNet->ENsend(&memberNode->addr, it->getAddress(), message);
	}
	g_transID++; // increment global transaction count for simulation
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
	// make message
	Message msg = makeMsg(MessageType::READ, key);
	string message = msg.toString();

	auto replicas = findNodes(key); // get replicas for this key 
	// send a message to each replica
	for (auto it = replicas.begin(); it != replicas.end(); it ++){
		emulNet->ENsend(&memberNode->addr, it->getAddress(), message);
	}
	g_transID++; // increment global transaction count for simulation
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
	// make message
	Message msg = makeMsg(MessageType::UPDATE, key, value);
	string message = msg.toString();

	auto replicas = findNodes(key); // get replicas for this key 
	// send a message to each replica
	for (auto it = replicas.begin(); it != replicas.end(); it ++){
		emulNet->ENsend(&memberNode->addr, it->getAddress(), message);
	}
	g_transID++; // increment global transaction count for simulation
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	// make message
	Message msg = makeMsg(MessageType::DELETE, key);
	string message = msg.toString();

	auto replicas = findNodes(key); // get replicas for this key 
	// send a message to each replica
	for (auto it = replicas.begin(); it != replicas.end(); it ++){
		emulNet->ENsend(&memberNode->addr, it->getAddress(), message);
	}
	g_transID++; // increment global transaction count for simulation
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int txId) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	// if txId == SP
	bool success = false;

}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
		// convert message to Message object, easier to access fields.
		Message msg(message);

		// different actions based on message type, and have to take care of stabilization messages.
		switch(msg.type){
			case MessageType::CREATE:{

			}
			case MessageType::READ:{

			}
			case MessageType::UPDATE:{

			}
			case MessageType::DELETE:{

			}
			case MessageType::REPLY:{
				
			}
			case MessageType::READREPLY:{
				
			}
		}

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
	// for each replica transfer over the entire hashtable. May not be as efficient as possible.
	// call this function after assigning the new current membership list to the ring vector
	// TODO: update hasReplicas vector as well.
	for (auto it = this->ht->hashTable.begin(); it != this->ht->hashTable.end(); it++){
		string key = it->first;
		string value = it->second;
		auto replicas = findNodes(key); // get the new replica nodes
		for (auto i = 0; i < replicas.size(); i++){
			// create stabilization protocol message which will not be confused with transaction messages
			Message msg(SP_MSG, this->memberNode->addr, MessageType::CREATE, key, value);
			string message = msg.toString(); // convert to string
			// send over network
			emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), message);
		}
	}
}
