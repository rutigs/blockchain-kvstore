/*

This package specifies the application's interface to the key-value
service library to be used in assignment 7 of UBC CS 416 2016 W2.

*/

package kvservice

import (
	"../tools"
	"../types"
	//"encoding/json"
	"log"
	//"math/rand"
	"errors"
	"net"
	"net/rpc"
	"sort"
	//"sync"
	"time"
)

// Represents a key in the system.
type Key string

// Represent a value in the system.
type Value string

// Unique id for this client
var UUID string

// Logger for this client
var logger *log.Logger

//==========================================================================================
// ===================================== RPC Connection ====================================
//==========================================================================================

// An interface representing a connection to the key-value store. To
// create a new connection use the NewConnection() method.
type connection interface {
	// The 'constructor' for a new logical transaction object. This is the
	// only way to create a new transaction. The returned transaction must
	// correspond to a specific, reachable, node in the k-v service. If
	// none of the nodes are reachable then tx must be nil and error must
	// be set (non-nil).
	NewTX() (newTX tx, err error)

	// Used by a client to ask a node for information about the
	// block-chain. Node is an IP:port string of one of the nodes that
	// was used to create the connection.  parentHash is either an
	// empty string to indicate that the client wants to retrieve the
	// SHA 256 hash of the genesis block. Or, parentHash is a string
	// identifying the hexadecimal SHA 256 hash of one of the blocks
	// in the block-chain. In this case the return value should be the
	// string representations of SHA 256 hash values of all of the
	// children blocks that have the block identified by parentHash as
	// their prev-hash value.
	GetChildren(node string, parentHash string) (children []string)

	// Close the connection.
	Close()
}

// Concrete implementation of a connection interface.
type KVConn struct {
	clients []*rpc.Client
	nodes   []string
}

// The 'constructor' for a new logical connection object. This is the
// only way to create a new connection. Takes a set of k-v service
// node ip:port strings.
func NewConnection(nodes []string) connection {
	_, logger = tools.NewTools(0)

	logger.Println("Establish NewConnection to nodes:", nodes)

	// Create RPC client for contacting the server.
	var clients []*rpc.Client
	sort.Strings(nodes)

	for _, node := range nodes {
        if newClient := createRPCClient(node); newClient != nil {
            clients = append(clients, newClient)
        }
	}

	return &KVConn{clients: clients, nodes: nodes}
}

// Create RPC client for contacting the server.
func createRPCClient(serverIPPort string) *rpc.Client {
	// parse given string address
	raddr, err := net.ResolveTCPAddr("tcp", serverIPPort)
	if err != nil {
		logger.Println(err)
        return nil
	}
	// dial rpc address
	conn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		logger.Println(err)
        return nil
	}
	// instantiate rpc client
	client := rpc.NewClient(conn)

	return client
}

// Create a new transaction.
func (kvConn *KVConn) NewTX() (newTX tx, err error) {
	UUID, _ = tools.GetUUID()
    newTX = &KVtx{kvConn}
	err = nil

	// create the args and replies
	args := &types.NewTXArgs{UUID: UUID}
	var replies []interface{}
	for range kvConn.clients {
		replies = append(replies, &types.NewTXReply{Success: false})
	}

	messageAllNodes(kvConn.clients, "ClientServer.NewTX", args, replies, false)

	// Check to see if any of the nodes completed
	var any bool
	for _, reply := range replies {
		if reply.(*types.NewTXReply).Success {
			any = true
			break
		}
	}

	if !any {
        time.Sleep(time.Second)
        messageAllNodes(kvConn.clients, "ClientServer.NewTX", args, replies, false)
        for _, reply := range replies {
		    if reply.(*types.NewTXReply).Success {
			    any = true
			    break
		    }
	    }

        if !any {
		err = errors.New("Unable to make a new transaction on any nodes")
	    }
    }

	return
}

func messageAllNodes(clients []*rpc.Client, rpcFunc string, args interface{}, replies []interface{}, commit bool) {
	var responses int
	if commit {
		responses = 1
	} else {
		responses = len(clients)
	}
	responseCh := make(chan bool, responses)

	for i, _ := range clients {
		go func(c *rpc.Client, r interface{}) {
			// commit will block entirely
			if commit {
                err := c.Call(rpcFunc, args, r)
                if err == nil {
                    responseCh <- true
                }
			} else {
				call := c.Go(rpcFunc, args, r, nil)
				select {
				case <-call.Done:
					break
				case <-time.After(time.Second * 2):
					break
				}
				responseCh <- true
			}
		}(clients[i], replies[i])
	}

	for i := 0; i < responses; i++ {
		<-responseCh
	}
}

// Close the connection.
func (kvConn *KVConn) Close() {
	for _, client := range kvConn.clients {
		if err := client.Close(); err != nil {
			logger.Printf("Error closing connection: %v\n", err)
		}
	}
}

// Get the children of the parent hash
func (kvConn *KVConn) GetChildren(node string, parentHash string) (children []string) {
	// TODO
	logger.Printf("GetChildren - Node %v ParentHash %v\n", node, parentHash)
	args := &types.GetChildrenArgs{Node: node, ParentHash: parentHash}
	var reply types.GetChildrenReply
	for i, n := range kvConn.nodes {
		if n == node {
			call := kvConn.clients[i].Go("ClientServer.GetChildren", args, &reply, nil)
			select {
			case <-call.Done:
				return reply.Children
			case <-time.After(2 * time.Second):
				break
			}

		}
	}

	return []string{""}
}

//==========================================================================================
// ====================================== Transaction ======================================
//==========================================================================================

// An interface representing a client's transaction. To create a new
// transaction use the connection.NewTX() method.
type tx interface {
	// Retrieves a value v associated with a key k as part of this
	// transaction. If success is true then v contains the value
	// associated with k and err is nil. If success is false then the
	// tx has aborted, v is an empty string, and err is non-nil. If
	// success is false, then all future calls on this transaction
	// must immediately return success = false (indicating an earlier
	// abort).
	Get(k Key) (success bool, v Value, err error)

	// Associates a value v with a key k as part of this
	// transaction. If success is true, then put was recoded
	// successfully, otherwise the transaction has aborted (see
	// above).
	Put(k Key, v Value) (success bool, err error)

	// Commits this transaction. If success is true then commit
	// succeeded, otherwise the transaction has aborted (see above).
	// The validateNum argument indicates the number of blocks that
	// must follow this transaction's block in the block-chain along
	// the longest path before the commit returns with a success.
	// txID represents the transactions's global sequence number
	// (which determines this transaction's position in the serialized
	// sequence of all the other transactions executed by the
	// service).
	Commit(validateNum int) (success bool, txID int, err error)

	// Aborts this transaction. This call always succeeds.
	Abort()
}

// Concrete implementation of a tx interface.
type KVtx struct {
	//GUID  string
	//Alive bool

	// implementation details
	//messages chan types.Msg
	connection *KVConn
}

// makes an rpc Get call
func (kvtx *KVtx) Get(k Key) (success bool, v Value, err error) {
	args := &types.GetArgs{UUID: UUID, Key: string(k)}
	var replies []interface{}
	for range kvtx.connection.clients {
		replies = append(replies, &types.GetReply{Success: false})
	}

	messageAllNodes(kvtx.connection.clients, "ClientServer.Get", args, replies, false)

	for _, reply := range replies {
		r := reply.(*types.GetReply)
		if r.Success {
			success = r.Success
			v = Value(r.Value)
			err = nil
			return
		}
	}

	return false, Value(""), errors.New("Unable to Get value for given key")
}

// makes an rpc Put call
func (kvtx *KVtx) Put(k Key, v Value) (success bool, err error) {
	args := &types.PutArgs{UUID: UUID, Key: string(k), Value: string(v)}
	var replies []interface{}
	for range kvtx.connection.clients {
		replies = append(replies, &types.PutReply{Success: false})
	}

	messageAllNodes(kvtx.connection.clients, "ClientServer.Put", args, replies, false)

	for _, reply := range replies {
		r := reply.(*types.PutReply)
		if r.Success {
			success = r.Success
			err = nil
			return
		}
	}

	return false, errors.New("Unable to Put value for given key")
}

// makes an rpc Commit Call
func (kvtx *KVtx) Commit(validateNum int) (success bool, txID int, err error) {
	args := &types.CommitArgs{UUID: UUID, ValidateNum: validateNum}
	var replies []interface{}
	for range kvtx.connection.clients {
		replies = append(replies, &types.CommitReply{Success: false})
	}

	messageAllNodes(kvtx.connection.clients, "ClientServer.Commit", args, replies, true)

	for _, reply := range replies {
		r := reply.(*types.CommitReply)
		if r.Success {
			success = r.Success
			txID = r.TxID
			err = nil
			return
		}
	}

	return false, -1, errors.New("Unable to commit")
}

// makes an rpc Abort
func (kvtx *KVtx) Abort() {
	args := &types.AbortArgs{UUID: UUID}
	var replies []interface{}
	for range kvtx.connection.clients {
		reply := false
		replies = append(replies, &reply)
	}

	messageAllNodes(kvtx.connection.clients, "ClientServer.Abort", args, replies, false)
}
