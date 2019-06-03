/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
    MPROT, // membership protocol message
    DUMMYLASTMSGTYPE
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr { // the whole msg
	enum MsgTypes msgType;
	Address addr; // sender address
	long heartbeat = 0; // sender heartbeat
	//int nMem = 0; // sender member count
	vector<MemberListEntry> msgList {}; // the member list for the sender node
}MessageHdr;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[6];

public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
	virtual ~MP1Node();
	void addMember(MessageHdr *msg);
	void addMember(MemberListEntry *entry);
	MemberListEntry* checkMember(int id, short port);
	MemberListEntry* checkMember(Address *addr);
	void sendMsg (Address* addr, MsgTypes t);
	Address getAddr(const MemberListEntry * entry);
	Address getAddr(int id, short port);
	Address* readAddr(MessageHdr *msg);
	void handleProt(MessageHdr *msg);
};

#endif /* _MP1NODE_H_ */
