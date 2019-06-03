/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <memory>
#include <iterator>
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();
    //printAddress(&joinaddr);

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        //size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        //msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        msg = new MessageHdr();
        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        msg->heartbeat = memberNode->heartbeat;
        msg->msgList = {};
        //memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        //memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
        memcpy(&msg->addr.addr, &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        std::cout<<"introducing using address: ";
        printAddress(&memberNode->addr);
        //std::cout <<memberNode->memberList.size()<<std::endl;

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, sizeof(MessageHdr));

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    // env is the memberNode of the callback, ptr is the message, size is size of the message
	/*
	 * Your code goes here
	 */
    // cast to header
    MessageHdr *msg = (MessageHdr*) data;
    // reply to JOINREQ message by sending over your membership list
    if ((msg->msgType) == MsgTypes::JOINREQ) {
        //add to your membership list
        addMember(msg);
        Address *dstAddr = &(msg->addr);
        sendMsg(dstAddr,MsgTypes::JOINREP);
        //MessageHdr *msgReply = makeMsg(MsgTypes::JOINREP);
        //printAddress(&msg->addr);
        //printAddress(&msgReply->addr);
        //emulNet->ENsend(&memberNode->addr, &msg->addr, (char *) msgReply, sizeof(MessageHdr));
        //std::cout << "send [" << par->getcurrtime() << "] JOINREP [" << memberNode->addr.getAddress() << "] to " << msg->addr.getAddress() << std::endl;
        //delete msgReply;
    } else if (msg->msgType == MsgTypes::JOINREP){
        addMember(msg);
        // merge membership list with current list and
        memberNode->inGroup = true; // member is now in group
        memberNode->nnb = msg->msgList.size();
        // printAddress(&msg->addr);
        // for (auto iter = msg->msgList.begin(); iter != msg->msgList.end(); iter++){            
        //     std::cout<<"above member  has list entry " << iter->id <<" "<< std::endl;
        // }
        // handleJoinRep(msg);
    } else if (msg->msgType == MsgTypes::MPROT){
        handleProt(msg);

    } else // not a message of any supported type
        return false;
    return true; // exit function
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    //if (memberNode->nnb == 9){
    //    // print for debug
    //    std::cout<<"this node is "<<memberNode->addr.getAddress()<<std::endl;
    //    std::cout<< "and these are the members for this node: "<<std::endl;
    //    for (auto x: memberNode->memberList){
    //        std::cout<<" "<< x.id<<std::endl;
    //    }
    //}
    if (memberNode-> bFailed == false){
        memberNode->heartbeat += 1;
        // delete member
        auto it = remove_if(memberNode->memberList.begin(), memberNode->memberList.end(),
            [&](const MemberListEntry & val){
                if(par->getcurrtime() - val.timestamp >= TREMOVE){
                    auto addr = getAddr(&val); 
                    log->logNodeRemove(&memberNode->addr, &addr);
                    memberNode->nnb--;
                    std::cout<< memberNode->addr.getAddress() << " removed "<< val.id <<":"<<val.port <<" at time "<< par->getcurrtime()<<std::endl;
                    return true;            
                } else
                    return false;            
            });
        memberNode->memberList.erase(it, memberNode->memberList.end());

        // all to all heartbeat for now
        for(auto mem: memberNode->memberList) {
            Address addr = getAddr(&mem);
            //std::cout << "send [" << par->getcurrtime() << "] PING [" << memberNode->addr.getAddress() << "] to " << address.getAddress() << std::endl;
            sendMsg(&addr, MsgTypes::MPROT);
        }
    }

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

// helper functions added

// getAddr() gets addr from MemberListEntry int id and short port fields, kind of a hack
Address MP1Node::getAddr(const MemberListEntry * entry) {
    Address joinaddr;
    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = entry->id;
    *(short *)(&joinaddr.addr[4]) = entry->port;
    return joinaddr;
}

Address MP1Node::getAddr(int id, short port) {
    Address addr;
    memcpy(&addr.addr[0], &id, sizeof(int));
    memcpy(&addr.addr[4], &port, sizeof(short));
    return addr;
}


//sendMsg() creates a new message from the memberNode structure to send to a given target node
void MP1Node::sendMsg (Address* addr, MsgTypes t){
    MessageHdr *msg = new MessageHdr();
    msg->msgType = t;
    msg->addr = memberNode->addr;
    msg->heartbeat = memberNode->heartbeat;
    msg->msgList = memberNode->memberList;
    // remove nodes which have been marked failed from the message
    // auto it = remove_if(msg->msgList.begin(), msg->msgList.end(),
    //     [&](const MemberListEntry & val){
    //         //if (t == JOINREP){
    //         //    std::cout<<memberNode->addr.getAddress() <<" message list contains "<<val.id<<std::endl;
    //         //}
    //         if(par->getcurrtime()-val.timestamp >= TFAIL)
    //             return true; 
    //         return false;
    //     });
    // msg->msgList.erase(it, msg->msgList.end());
    //msg->nMem = memberListSize;
    //msg->heartbeat = memberNode->heartbeat;
    emulNet->ENsend( &memberNode->addr, addr, (char*)msg, sizeof(MessageHdr));
    
}

//checkMember() checks if node with address is already in member list or not, if it is returns pointer to 
// the member list entry
MemberListEntry* MP1Node::checkMember(int id, short port) {
    for (auto iter = memberNode->memberList.begin(); iter != memberNode->memberList.end(); iter++) {
         
        if (iter->id  ==  id && iter->port == port)
            return &(*iter); 
    }
    return nullptr; // address not found in list
}

// for working with Address object
MemberListEntry* MP1Node::checkMember(Address *addr) {
    
    for (auto iter = memberNode->memberList.begin(); iter != memberNode->memberList.end(); iter++) {
        int id = 0;
        short port = 0;
        memcpy(&id, &addr->addr[0], sizeof(int));
        memcpy(&port, &addr->addr[4], sizeof(short)); 
        if (iter->id  ==  id && iter->port == port)
            return &(*iter); 
    }
    return nullptr; // address not found in list
}

// addMember() adds a member from a message to the node's member list. used when we receive a JOINREQ message 
void MP1Node::addMember(MessageHdr *msg){
    // existing hacky way to get id and port
    int id = *(int*)(&msg->addr.addr);
    short port = *(short*)(&msg->addr.addr[4]);
    if (checkMember(id,port) != nullptr) { 
        return; // node with this address already in Member List
    }
    //std::cout<<"adding: ";
    //printAddress(&msg->addr);
    MemberListEntry newMemLE(id, port, 1, par->getcurrtime()); 
    memberNode->memberList.push_back(newMemLE);
    log->logNodeAdd(&memberNode->addr, &msg->addr); // log new node  added

}

//same as above but uses MemberListEntry and checks for freshness
void MP1Node::addMember(MemberListEntry *entry){
    Address addr = getAddr(entry);
    if (addr == memberNode->addr){
        return;
    }
    if (par->getcurrtime() - entry->timestamp < TREMOVE){
        log->logNodeAdd(&memberNode->addr, &addr);
        memberNode->memberList.push_back(*entry);
    }
}


// handle MPROT messages
void MP1Node::handleProt(MessageHdr *msg){
    // source of this message
    int id = *(int*)(&msg->addr.addr);
    short port = *(short*)(&msg->addr.addr[4]);
    int found = 0; 
    for(int i = 0; i < memberNode->memberList.size(); i++) {
        MemberListEntry *chk = memberNode->memberList.data() + i;
        if (chk->id == id && chk->port == port) {
            found = 1;
            if (chk->heartbeat < msg->heartbeat)
                chk->heartbeat = msg->heartbeat;
            else
                 chk->heartbeat++;
            chk->timestamp = par->getcurrtime();
            
        } 
    }
    if (found == 0){ // message from node not in memberList, so add it
        addMember(msg);
    }
    // do the same for the rest of the nodes sent over in the message list
    found = 0;
    for (auto j = msg->msgList.begin(); j != msg->msgList.end(); j++){ // for each member list entry in msg check to see if in memberNode and update it or add it
        // get port and id from msg entry
        auto ind = (*j);
        id = ind.id;
        port = ind.port;
        found = 0;
        MemberListEntry *chk = checkMember(id,port);
        if (chk != nullptr){
            if (chk->heartbeat < ind.heartbeat){
                    chk->heartbeat = ind.heartbeat;
                    chk->timestamp = par->getcurrtime();
                }
        } else {
            addMember(&(*j));
        }
    }
}

