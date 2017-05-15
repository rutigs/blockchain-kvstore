//==========================================================================================
// ========================================= Types =========================================
//==========================================================================================
/*
	Types that are not large enough to justify their own file go here.
*/

package types

import (
	"net/rpc"
	"sync"
)

type Node struct {
	NodeIP     string
	NodeClient *rpc.Client
	Alive      bool
}

type Nodes struct {
	sync.RWMutex
	List []Node // List of nodes in the system, where List[0] will be the master
}

// the NewTX args struct the kvservice sents to the node
type NewTXArgs struct {
	UUID string
}

// the NewTX response struct the node sends to a kvservice
type NewTXReply struct {
	Success bool
}

// the Get args struct the kvservice sents to the node
type GetArgs struct {
	UUID string
	Key  string
}

// the Get response struct the node sends to a kvservice
type GetReply struct {
	Success bool
	Value   string //Value
}

// the Get args struct the kvservice sents to the node
type PutArgs struct {
	UUID  string
	Key   string
	Value string
}

// the Get response struct the node sends to a kvservice
type PutReply struct {
	Success bool
}

// the Get args struct the kvservice sents to the node
type CommitArgs struct {
	UUID        string
	ValidateNum int
}

// the Get response struct the node sends to a kvservice
type CommitReply struct {
	Success bool
	TxID    int //Value
}

// the Abort args struct for service -> node
type AbortArgs struct {
	UUID string
}

// the GetChildren args struct the kvservice sends to a node
type GetChildrenArgs struct {
	Node       string
	ParentHash string
}

// the GetChildren response struct the node sends to a kvservice
type GetChildrenReply struct {
	Success  bool
	Children []string
}
