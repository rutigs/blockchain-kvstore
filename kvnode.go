/*
Implements the kvnode solution in assignment 7 for UBC CS 416 2016 W2.

Usage:

Start node:
go run kvnode.go [ghash] [num-zeroes] [nodesFile] [nodeID] [listen-node-in IP:port] [listen-client-in IP:port]

Example:
go run kvnode.go       nodes.txt 1 192.999.999.999:1337 192.999.999.999:1234

*/

package main

import (
	"./tools"
	"./types"
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Resource server type.
type ClientServer int

// Node to Node server type
type NodeServer int

// Block Type
type Block struct {
	Hash         string            // hex encoded string sha256 hash of this block
	PrevHash     string            // PrevHash computed by this block
	Depth        int               // depth of this block in the Block Chain
	TxID         string            // transaction/order id in the chain
	UUID         string            // unique id for the client
	TxnUpdates   map[string]string // Map of k-v puts
	NodeID       int               // ID of the Node that computes this
	Nonce        uint32            // Nonce to compute the hash
	Links        []*Block          // Links to the next block(s) in the chain
	ValidateNum  int               // number of blocks needed to follow to validate
	Validated    bool              // flag when block is validated
	Aborted      bool              // in case this block is aborted
	NoOp         bool              // flag for if this block is a no-op block
	AttemptTrans bool              // flag for when this block has been transplanted to another branch
}

// temporary store for uncommited txns
type TempStore struct {
	sync.RWMutex
	Transactions map[string][]MicroTransaction // map of Client UUID to array of micro transactions
}

// either a single GET or PUT
type MicroTransaction struct {
	Verb  string // "PUT" || "GET"
	Key   string
	Value string
	Tip   *Block
}

// Main Block Chain
type BlockChain struct {
	sync.RWMutex
	Genesis Block
	Tip     *Block
}

type TxnTracker struct {
	sync.RWMutex
	ValidatedMap map[string]int  // map[UUID] = txID
	AbortedMap   map[string]bool // map[UUID] = bool
}

//==========================================================================================
// ====================================== Global Vars ======================================
//==========================================================================================

// constants
const (
	//NUM_HASH_ATTEMPTS = 5
	MAX_BLOCKS_BEHIND = 4 // TODO: pick a reasonable number of blocks for this
)

// loggers
var (
	verboseLog *log.Logger
	logger     *log.Logger
)

// Node list
var nodes types.Nodes

// this node's external ip
var externalIP string

// this node's Block Chain
var blockChain BlockChain

// Channel for notify we have received a new block
var receivedBlockCh chan *Block

// Channel to notify we have a new block to compute
var unhashedBlockCh chan *Block

// Temporary store for uncommitted txns
var store TempStore

// this node's ID
var nodeID int

// num zeroes for hashing requirements
var numZeroes int

// for tracking which txns are validated or aborted
var txnTracker TxnTracker

//==========================================================================================
// ======================================= Functions =======================================
//==========================================================================================

// http://imgur.com/a/38VII
func main() {
	// fetch args
	ghash, nodesFilePath, listenNodeAddr, listenClientAddr := parseArgs()

	// tools 	tools.NewTools(nodeID)
	verboseLog, logger = tools.NewTools(nodeID)

	verboseLog.Printf("ghash: %v\n", ghash)
	verboseLog.Printf("numZeroes: %v\n", numZeroes)
	verboseLog.Printf("listenNodeAddr: %v\n", listenNodeAddr)
	verboseLog.Printf("listenClientAddr: %v\n", listenClientAddr)

	// these will be used to asynchronously communicate with computeBlocks
	receivedBlockCh = make(chan *Block, 10)
	unhashedBlockCh = make(chan *Block, 10)

	txnTracker = TxnTracker{
		ValidatedMap: make(map[string]int),
		AbortedMap:   make(map[string]bool),
	}

	nodeList, err := readLines(nodesFilePath)
	tools.CheckError("Failed to parse nodes file", err, true)
	verboseLog.Printf("nodes: %v\n", nodes)

	externalIP = nodeList[nodeID-1]
	sort.Strings(nodeList)

	go func() {
		nServer := rpc.NewServer()
		n := new(NodeServer)
		nServer.Register(n)

		l, err := net.Listen("tcp", listenNodeAddr)
		tools.CheckError("Error starting rpc server for nodes", err, true)
		logger.Println("NodeRPC server started")
		for {
			conn, err := l.Accept()
			tools.CheckError("Error accpecting node rpc requests", err, false)
			go nServer.ServeConn(conn)
		}
	}()
	startup(nodeList)

	// Construct the block chain
	blockChain = BlockChain{
		Genesis: Block{
			Hash:  ghash,
			Depth: 0,
			NoOp:  true,
		},
		Tip: &blockChain.Genesis,
	}

	// Start computing no-ops or txn blocks
	go computeBlocks(nodeID, numZeroes)

	// create the temp store for ongoing txns
	store = TempStore{Transactions: make(map[string][]MicroTransaction)}

	cServer := rpc.NewServer()
	c := new(ClientServer)
	cServer.Register(c)

	l, err := net.Listen("tcp", listenClientAddr)
	tools.CheckError("Error starting rpc server for clients", err, true)
	logger.Println("ClientRPC server started")
	for {
		conn, err := l.Accept()
		tools.CheckError("Error accepting client rpc requests", err, false)
		go cServer.ServeConn(conn)
	}
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func startup(nodeList []string) {
	nodes = types.Nodes{}
	//clients = Clients{Map: make(map[int]Transaction)}

	var wg sync.WaitGroup

	for _, ip := range nodeList {

		if ip != externalIP {
			wg.Add(1)
			go func(newNodeIP string) {
				client := connectServer(newNodeIP)
				if client != nil {
					nodes.Lock()
					nodes.List = append(nodes.List, types.Node{
						NodeIP:     ip,
						NodeClient: client,
					})
					nodes.Unlock()
				}
				wg.Done()
			}(ip)

		}
	}
	wg.Wait()
}

// super basic func to grab args
func parseArgs() (ghash string, nodesFile string, nodeIPPort string, clientIPPort string) {
	ghash = os.Args[1]
	numZeroes, _ = strconv.Atoi(os.Args[2])
	nodesFile = os.Args[3]
	nodeID, _ = strconv.Atoi(os.Args[4])
	nodeIPPort = os.Args[5]
	clientIPPort = os.Args[6]
	return
}

// dial and connect to server
func connectServer(serverIPPort string) *rpc.Client {
	client, err := rpc.Dial("tcp", serverIPPort)
	tools.CheckError("connecting to server", err, false)

	if err != nil {
		time.Sleep(2 * time.Second)
		err = nil
	}

	logger.Println("Retrying connection")
	time.Sleep(time.Second)
	client, err = rpc.Dial("tcp", serverIPPort)
	tools.CheckError("connecting to server", err, false)

	if err != nil {
		time.Sleep(2 * time.Second)
		err = nil
	}

	logger.Println("Retrying connection")
	time.Sleep(time.Second)
	client, err = rpc.Dial("tcp", serverIPPort)
	tools.CheckError("connecting to server", err, false)

	if err != nil {
		return nil
	}

	logger.Println("Connected to server: ", serverIPPort)
	return client
}

// attempts to calculate a valid hash for the given block with the given numZeroes.
// Will attempt to calc the hash a limited number of times before giving up.
func tryHashBlock(block *Block, numZeroes int) {
	var maxAttempts int
	// this throttles the hashing/block creation to avoid ultra block spam
	if numZeroes == 0 {
		// a numZero of 0 means the hash will succeed immediately, so having different
		// wait times here for different nodes breaks race condition/ties
		time.Sleep(time.Millisecond * (0 + time.Duration(200*nodeID)))
		maxAttempts = 1
	}
	if numZeroes < 2 {
		time.Sleep(time.Millisecond * 100)
		maxAttempts = 3
	} else if numZeroes == 2 {
		time.Sleep(time.Millisecond * 1)
		maxAttempts = 5
	} else {
		maxAttempts = 5
	}

	hash := sha256.Sum256([]byte(fmt.Sprintf("%v%v%v%v", block.PrevHash, block.TxnUpdates, block.NodeID, block.Nonce)))
	block.Hash = hex.EncodeToString(hash[:])

	// while the hash does NOT have enough prefix zeroes, and hasn't reached max attempts
	attempts := 0
	for (!isHashValid(block.Hash, numZeroes)) && (attempts < maxAttempts) {
		block.Nonce++
		attempts++
		// hash block
		hash := sha256.Sum256([]byte(fmt.Sprintf("%v", block)))
		block.Hash = hex.EncodeToString(hash[:])
	}
	if isHashValid(block.Hash, numZeroes) {
		logger.Println("tryHashBlock FOUND HASH: ", block.Hash)
	}
}

// checks if given hash has enough leading zeroes
func isHashValid(hash string, numZeroes int) bool {
	hashBuf, err := hex.DecodeString(hash)
	tools.CheckError("hex.DecodeString failed", err, true)
	for i := 0; i < numZeroes; i++ {
		// if the next byte is not a zero
		if hashBuf[i] != byte('0') {
			return false
		}
	}
	// the first numZero bytes were all 0
	return true
}

// handles computing one block at a time whether its a txn or a no-op
// Also handles adding new blocks to the block-chain
func computeBlocks(nodeID int, numZeroes int) {
	// Computes a no op block or txn block. Periodically checks for new hashed blocks.
OuterLoop:
	for {
		// pick a block to work on. Either txn block or NoOp
		var block *Block
		select {
		case block = <-unhashedBlockCh:
			// we have a txn block to hash!
			logger.Println("Start working on a txn block: ", block)
			block.PrevHash = blockChain.Tip.Hash
			// check if committing this block is possible
			if !canCommit(block) {
				block.Aborted = true
				// TODO: may have to delete local txn state here
				// delete(store.Transactions, block.UUID) ?? should we lock store then?

				txnTracker.Lock()
				txnTracker.AbortedMap[block.UUID] = true
				txnTracker.Unlock()

				goto OuterLoop
			}

		default:
			// no txn blocks to hash, switching to no-op blocks
			logger.Println("Start working on a no-op block")
			block = &Block{
				PrevHash: blockChain.Tip.Hash, // TODO: do we have to lock here?
				NodeID:   nodeID,
				NoOp:     true,
			}
		}

	InnerLoop:
		for {
			// Check if another node finished a block
			var newRecvBlock *Block
			select {
			case newRecvBlock = <-receivedBlockCh:
				// we got a new block to add to our block-chain!
				logger.Println("Received new hashed block from another node!", newRecvBlock)

				if newRecvBlock.NoOp {
					// we received a NoOp block
					addBlockToChain(newRecvBlock)

					// only reset our hashing if the new block is on longest chain
					if isOnLongestChain(newRecvBlock, &blockChain.Genesis) {
						block.PrevHash = newRecvBlock.Hash
						block.Nonce = 0
					}
					continue
					// break

				} else if block.UUID == newRecvBlock.UUID {
					// we were working on the SAME txn block
					logger.Println("STOP working on the SAME txn block!")
					*block = *newRecvBlock

					addBlockToChain(block)
					break InnerLoop

				} else {
					// we received a txn block that we were not working on
					logger.Println("we received a txn block that we were not working on")

					// wait to give the client time to tell us to commit the txn.
					time.Sleep(time.Second * 2)

					// fetch our version of that txn block
					ourBlock, success := popOurBlockFromUnhashedCh(newRecvBlock)
					if !success {
						// another Node must have received and hashed a txn block before the client
						// even told us to commit it!
						logger.Println("Another Node beat us to hashing before we were told to even commit!")
						ourBlock = newRecvBlock
					}

					addBlockToChain(ourBlock)

					// only reset our hashing if the new block is on longest chain
					if isOnLongestChain(ourBlock, &blockChain.Genesis) {
						block.PrevHash = ourBlock.Hash
						block.Nonce = 0
					}
					continue
					// break
				}

			default:
				// we received no new blocks to add to our block-chain
				break
			}

			// now try to actually hash the block we're working on
			tryHashBlock(block, numZeroes)

			// check if hash was successful
			if isHashValid(block.Hash, numZeroes) {
				addBlockToChain(block)
				broadcastBlock(block)
				break InnerLoop
			}

			// Check if we should switch to a txn block (if we're currenly doing a no-op)
			if block.NoOp && (len(unhashedBlockCh) > 0) {
				// stop working on NoOp block
				break InnerLoop
			}
		}
	}
	logger.Println("YOU SHOULD NEVER SEE THIS")
	os.Exit(0)
}

// Checks if given block is capable of committing to the Block Chain.
// A txn block can NOT commit if it operates on bad data. ie. if it
// contains a GET on data that has since been updated.
// MUST be holding the Block Chain's lock before calling this func
func canCommit(block *Block) bool {
	// fetch this txn block's MicroTransactions
	mTs := store.Transactions[block.UUID]

	// fetch most-recent kv map
	currentKV, success := getStoreByHash(blockChain.Tip.Hash)
	if !success {
		logger.Println("getStoreByHash failed to fetch block of Hash: ", blockChain.Tip.Hash)
		// TODO: should we return here? os.Exit?
	}

	// iterate through mTs in order
	for i := 0; i < len(mTs); i++ {
		mt := mTs[i]
		// if this block read a value he didn't write
		if (mt.Verb == "GET") && (mt.Tip != nil) && (mt.Value != currentKV[mt.Key]) {
			logger.Println("canComit: FALSE. Given txn block operates on BAD DATA!")
			logger.Printf("block k: %v, v: %v, current v: %v", mt.Key, mt.Value, currentKV[mt.Key])
			return false
		}
	}

	return true
}

// remove given block from the unhashedBlockCh channel
func popOurBlockFromUnhashedCh(block *Block) (*Block, bool) {
	//logger.Println("START: popOurBlockFromUnhashedCh block.UUID", block.UUID)
	//logger.Println("len(unhashedBlockCh): ", len(unhashedBlockCh))

	numBlocks := len(unhashedBlockCh)
	for i := 0; i < numBlocks; i++ {
		select {
		case potBlock := <-unhashedBlockCh:
			if potBlock.UUID != block.UUID {
				unhashedBlockCh <- potBlock
			} else {
				return potBlock, true
			}

		default:
			continue
		}
	}

	return nil, false
}

// adds given block to block-chain using locks
func addBlockToChain(block *Block) {
	//logger.Println("START: addBlockToChain block: ", block)

	blockChain.Lock()

	// check given block isn't already on the longest chain
	if !block.NoOp && uuidInAncestors(block) {
		logger.Println("TOLD to add txn block to a chain that already contained block.UUID")

		blockChain.Unlock()
		return
	}

	parentBlock, success := getBlockByHash(block.PrevHash)
	if !success {
		logger.Println("ERROR: told to find hash that wasn't in block chain!!!")
		os.Exit(0)
	}

	block.Depth = parentBlock.Depth + 1

	parentBlock.Links = append(parentBlock.Links, block)

	// only update Tip if given block is on longest chain
	if isOnLongestChain(block, &blockChain.Genesis) {
		blockChain.Tip = block
	} else {
		logger.Println("Told to add block to Block Chain NOT on longest branch!")
	}

	// Check Block Chain to see if any txns are now validated
	checkValidated(&blockChain.Genesis)

	// Check Block Chain to see if any txns need to be aborted
	checkAborts(&blockChain.Genesis)

	blockChain.Unlock()
	prettyPrintChain()
}

// Checks if given block's uuid is in any of his ancestors
func uuidInAncestors(block *Block) bool {
	logger.Println("START: uuidInAncestors")

	ancestors := getAncestors(block)

	// if any ancestor has the same uuid...
	for _, potBlock := range ancestors {
		if potBlock.UUID == block.UUID {
			return true
		}
	}

	return false
}

// returns array of block ptrs where each member of the array is an ancestor
// of the given block.
func getAncestors(block *Block) []*Block {
	logger.Println("START: getAncestors")

	ancestors := make([]*Block, 0)

	// we must be the genesis block
	if len(block.PrevHash) == 0 {
		return ancestors
	}

	logger.Println("fetch parent getAncestors")

	// get ancestors of my parent and add my parent to that array
	parentBlock, success := getBlockByHash(block.PrevHash)
	if !success {
		logger.Println("FAILED to retrieve parent of block: ", block)
		os.Exit(0)
	}
	return append(getAncestors(parentBlock), parentBlock)
}

// returns pointer to block matching the given hash
func getBlockByHash(hash string) (*Block, bool) {
	return getBlockByHashHelper(&blockChain.Genesis, hash)
}

//
func getBlockByHashHelper(block *Block, hash string) (*Block, bool) {
	if block.Hash == hash {
		return block, true
	}
	// Check each kid
	for _, kid := range block.Links {
		potBlock, success := getBlockByHashHelper(kid, hash)
		// if hash was found somewhere in kid
		if success {
			return potBlock, true
		}
	}
	// no hash was found!
	return nil, false
}

// Checks Block Chain to see if any txns are now validated, starts at the given block
// and continues checking his kids
// MUST be holding Block Chain's lock before calling this func
func checkValidated(block *Block) {
	// only txn blocks get validated
	if !block.NoOp {
		// get depth of block's kids
		depth := getChainDepth(block)

		// if this block is now validated
		if !block.Validated && (block.ValidateNum <= depth) {
			logger.Println("BLOCK IS NOW VALIDATED!! : ", block.UUID[:10])
			block.Validated = true

			txnTracker.Lock()
			// TODO: verify this txID is good
			txnTracker.ValidatedMap[block.UUID] = (block.Depth * 1000) + block.NodeID
			txnTracker.Unlock()
		}
	}

	// Check each kid block
	for _, kid := range block.Links {
		checkValidated(kid)
	}
}

// returns depth of chain starting at given block
// MUST be holding Block Chain's lock before calling this function
func getChainDepth(block *Block) int {
	// if this block has kids
	if len(block.Links) > 0 {
		// get max from set of depths from each kid block
		maxDepth := 0
		for _, kid := range block.Links {
			potDepth := getChainDepth(kid)
			if potDepth > maxDepth {
				maxDepth = potDepth
			}
		}
		return 1 + maxDepth
	} else {
		// this block has no kids
		return 0
	}
}

// Checks Block Chain to see if any txns need to be aborted, starting at given block
// MUST be holding Block Chain's lock when calling this func
func checkAborts(block *Block) {
	// A txn block needs to be aborted if he is not validated and if he is on a
	// losing chain significantly shorter than the longest chain
	if !block.NoOp && !block.Validated && !isOnLongestChain(block, &blockChain.Genesis) {
		// if block is too far behind AND we've never attempted a transplant on this block before
		if ((blockChain.Tip.Depth - block.Depth) > MAX_BLOCKS_BEHIND) && !block.AttemptTrans {
			success := attemptTransplant(block)

			if !success {
				delete(store.Transactions, block.UUID)
				block.Aborted = true

				txnTracker.Lock()
				txnTracker.AbortedMap[block.UUID] = true
				txnTracker.Unlock()
			}
		}
	}

	// Check each kid block
	for _, kid := range block.Links {
		checkAborts(kid)
	}
}

// attempts to transplant the given block onto the tip of the Block Chain
// MUST be holding Block Chain's lock when calling this func
func attemptTransplant(block *Block) bool {
	logger.Println("START ATTEMPTING TRANSPLANT block: ", block)

	// we should only ever attempt transplanting a block once
	if !block.AttemptTrans {
		block.AttemptTrans = true
	} else {
		logger.Println("TOLD to transplant a block we've already attempt a transplant on!")
		os.Exit(0)
	}

	// transplant the block if possible
	if canCommit(block) {
		// transplant
		newBlock := &Block{}
		*newBlock = *block

		newBlock.Nonce = 0
		newBlock.Hash = ""
		newBlock.AttemptTrans = false // the new block might get transplated again

		// treat it like a newly committed block
		unhashedBlockCh <- newBlock

		logger.Println("attemptTransplant returning TRUE")
		// transplant attempt successful
		return true
	} else {
		// failed to transplant
		logger.Println("attemptTransplant returning FALSE")
		return false
	}
}

// Checks if given block is on longest chain starting at given block pos
// MUST be holding Block Chain's lock when calling this func
func isOnLongestChain(block *Block, pos *Block) bool { // TODO: this should be refactored
	if pos.UUID == block.UUID {
		return true
	} else if len(pos.Links) == 0 {
		return false
	}

	// map from [kid index] to depth
	depthMap := make(map[int]int)
	for index, kid := range pos.Links {
		depthMap[index] = getChainDepth(kid)
	}
	//logger.Println("depthMap: ", depthMap)

	// Check if there exists a single maxDepth
	duplicateCounter := 0
	potIndex := 0
	maxDepth := depthMap[potIndex]
	for i := 1; i < len(depthMap); i++ { // skip first one since we initiazed our vars to the first one
		depth := depthMap[i]
		if maxDepth < depth {
			potIndex = i
			maxDepth = depth
			duplicateCounter = 0
		} else if maxDepth == depth {
			duplicateCounter++
		}
	}

	// if there are multiple max depths
	if duplicateCounter != 0 {
		// there are multiple "longest chains", therefore no single longest chain
		logger.Println("There are MULTIPLE longest chains!!! dup:", duplicateCounter)
		return false
	}

	// recursively search for block on longest chain
	return isOnLongestChain(block, pos.Links[potIndex])
}

// A data mutating function. Compiles map of kv PUT updates based off of
// given array of MicroTransactions
func getTxnUpdatesFromMTS(mts []MicroTransaction) map[string]string {
	var txnUpdates = make(map[string]string)

	// iterate through mts to compile map of PUTs in order
	for i := 0; i < len(mts); i++ {
		if mts[i].Verb == "PUT" {
			txnUpdates[mts[i].Key] = mts[i].Value
		}
	}

	return txnUpdates
}

// A data mutating function. Compiles array of all Blocks in Block Chain starting
// at given block
// MUST be holding block chain lock when calling this func
func getBlocksFromBlockChain(block *Block) []Block {

	var acc []Block

	// add yourself to array result
	acc = append(acc, *block)

	// if no kids, return
	if len(block.Links) == 0 {
		return acc
	}

	// add your kid's results
	for i := 0; i < len(block.Links); i++ {
		acc = append(acc, getBlocksFromBlockChain(block.Links[i])...)
	}

	return acc
}

// returns number of blocks that have given depth
func getNumBlocksAtDepth(blocks []Block, depth int) int {
	acc := 0
	for _, block := range blocks {
		if block.Depth == depth {
			acc++
		}
	}

	return acc
}

// returns max depth found in given array of blocks
func getMaxDepth(blocks []Block) int {

	if len(blocks) == 0 {
		return 0
	}

	maxDepth := blocks[0].Depth
	for _, block := range blocks {
		if block.Depth > maxDepth {
			maxDepth = block.Depth
		}
	}

	return maxDepth
}

// prints the Chain in pretty format starting at given block
func prettyPrintChain() {
	blockChain.Lock()
	defer blockChain.Unlock()
	fmt.Println("==================== START ====================")
	fmt.Println(blockChain.Genesis.Hash[:10])

	var CHAR_WIDTH_SCREEN = 85
	var CHAR_WIDTH_NODE = 16

	blocks := getBlocksFromBlockChain(&blockChain.Genesis)
	maxDepth := getMaxDepth(blocks)

	// print each depth
	for depth := 0; depth <= maxDepth; depth++ {
		numBlocksAtDepth := getNumBlocksAtDepth(blocks, depth)

		var whiteSpace = (CHAR_WIDTH_SCREEN - (CHAR_WIDTH_NODE * numBlocksAtDepth)) / (numBlocksAtDepth + 1)

		fmt.Printf("\n")
		// print the top bar of each block at this depth
		for i := 0; i < len(blocks); i++ {
			block := blocks[i]
			if block.Depth == depth {
				printWhiteSpace(whiteSpace)
				fmt.Printf(" _____________ ")
			}
		}

		fmt.Printf("\n")
		// print the type of each block at this depth
		for i := 0; i < len(blocks); i++ {
			block := blocks[i]
			if block.Depth == depth {
				printWhiteSpace(whiteSpace)
				if block.NoOp {
					fmt.Printf("| NoOp: %v  |", block.NoOp)
				} else {
					fmt.Printf("| NoOp: %v |", block.NoOp)
				}
			}
		}

		fmt.Printf("\n")
		// print the hash of each block at this depth
		for i := 0; i < len(blocks); i++ {
			block := blocks[i]
			if block.Depth == depth {
				printWhiteSpace(whiteSpace)
				var h string
				if len(block.Hash) >= 10 {
					h = block.Hash[4:10]
				} else {
					h = "      "
				}
				fmt.Printf("| h: %v   |", h)
			}
		}

		fmt.Printf("\n")
		// print the PrevHash of each block at this depth
		for i := 0; i < len(blocks); i++ {
			block := blocks[i]
			if block.Depth == depth {
				printWhiteSpace(whiteSpace)
				var pH string
				if len(block.PrevHash) >= 10 {
					pH = block.PrevHash[4:10]
				} else {
					pH = "      "
				}
				fmt.Printf("| pH: %v  |", pH)
			}
		}

		fmt.Printf("\n")
		// print the nodeID of each block at this depth
		for i := 0; i < len(blocks); i++ {
			block := blocks[i]
			if block.Depth == depth {
				printWhiteSpace(whiteSpace)
				fmt.Printf("| nodeID: %v   |", block.NodeID)
			}
		}

		fmt.Printf("\n")
		// print the UUID of each block at this depth
		for i := 0; i < len(blocks); i++ {
			block := blocks[i]
			if block.Depth == depth {
				printWhiteSpace(whiteSpace)
				if len(block.UUID) >= 10 {
					fmt.Printf("| UUID:%v |", block.UUID[4:10])
				} else {
					fmt.Printf("| UUID:       |")
				}
			}
		}

		fmt.Printf("\n")
		// print the Depth of each block at this depth
		for i := 0; i < len(blocks); i++ {
			block := blocks[i]
			if block.Depth == depth {
				printWhiteSpace(whiteSpace)
				if block.Depth < 10 {
					fmt.Printf("| depth: %v    |", block.Depth)
				} else {
					fmt.Printf("| depth: %v   |", block.Depth)
				}

			}
		}

		fmt.Printf("\n")
		// print the number of Links of each block at this depth
		for i := 0; i < len(blocks); i++ {
			block := blocks[i]
			if block.Depth == depth {
				printWhiteSpace(whiteSpace)
				fmt.Printf("| #kids: %v    |", len(block.Links))
			}
		}

		fmt.Printf("\n")
		// print whether or not transplant was attempted of each block at this depth
		for i := 0; i < len(blocks); i++ {
			block := blocks[i]
			if block.Depth == depth {
				printWhiteSpace(whiteSpace)
				if block.AttemptTrans {
					fmt.Printf("| aTrans:%v |", block.AttemptTrans)
				} else {
					fmt.Printf("| aTrans:%v|", block.AttemptTrans)
				}

			}
		}

		fmt.Printf("\n")
		// print the bottom bar of each block at this depth
		for i := 0; i < len(blocks); i++ {
			block := blocks[i]
			if block.Depth == depth {
				printWhiteSpace(whiteSpace)
				fmt.Printf("|_____________|")
			}
		}
	}

	fmt.Println("\n===================== END ====================")
}

// prints given number of spaces (no new lines)
func printWhiteSpace(whiteSpace int) {
	for i := 0; i < whiteSpace; i++ {
		fmt.Printf(" ")
	}
}

// // prints the Chain in pretty format starting at given block
// func prettyPrintKids(block *Block, depth int) {
// 	// print each kid
// 	for _, kid := range block.Links {
// 		if kid.NoOp {
// 			fmt.Printf("..| h: %v, pHash: %v, nID: %v |..",
// 				kid.Hash[4:10], kid.PrevHash[4:10], kid.NodeID)
// 		} else {
// 			fmt.Printf("..| h: %v, pHash: %v, nID: %v, UUID: %v |..",
// 				kid.Hash[4:10], kid.PrevHash[4:10], kid.NodeID, kid.UUID[4:10])
// 		}
// 	}
// 	fmt.Printf("\n")

// 	// print grandkids
// 	for _, kid := range block.Links {
// 		prettyPrintKids(kid, depth+1)
// 	}
// }

// Gets the store of the blockchain up to that hash
func getStoreByHash(hash string) (map[string]string, bool) {
	block, exists := getBlockByHash(hash)
	if !exists {
		return nil, false
	}

	var allMaps []map[string]string
	allMaps = append(allMaps, block.TxnUpdates)

	// Build the list of maps from newest to oldest state
	for block.Hash != blockChain.Genesis.Hash {
		block, exists = getBlockByHash(block.PrevHash)
		if !exists {
			break
		}

		allMaps = append(allMaps, block.TxnUpdates)
	}

	hashStore := make(map[string]string)
	// Iterate over all the maps from oldest state to newest
	for i := len(allMaps) - 1; i >= 0; i-- {
		// for individual blocks go over the puts in order
		for key, value := range allMaps[i] {
			hashStore[key] = value
		}
	}
	return hashStore, true
}

//=============================================================================
//========================== Client RPC Functions =============================
//=============================================================================

// Initialize a transaction with a client
func (c *ClientServer) NewTX(args *types.NewTXArgs, reply *types.NewTXReply) error {
	logger.Printf("NewTX - UUID: %v\n", args.UUID)
	store.Lock()
	defer store.Unlock()

	store.Transactions[args.UUID] = []MicroTransaction{}
	reply.Success = true
	return nil
}

// Retrieve a value for that key from client request
func (c *ClientServer) Get(args *types.GetArgs, reply *types.GetReply) error {
	logger.Printf("Get - Key: %v\n", args.Key)
	store.Lock()
	defer store.Unlock()

	if transaction, ok := store.Transactions[args.UUID]; ok {

		newMicroTxn := MicroTransaction{
			Verb: "GET",
			Key:  args.Key,
		}
		// check if key has state in local txn before we check blockchain
		localPut := false
		var localVal string
		// iterate over all puts to get what would be the txns view of the key
		for _, microTxn := range transaction {
			if microTxn.Verb == "PUT" && microTxn.Key == args.Key {
				localPut = true
				localVal = microTxn.Value
			}
		}

		if localPut {
			reply.Value = localVal
			reply.Success = true
			newMicroTxn.Value = localVal
			store.Transactions[args.UUID] = append(transaction, newMicroTxn)
			return nil
		}

		blockChain.Lock()
		defer blockChain.Unlock()
		currStore, success := getStoreByHash(blockChain.Tip.Hash)
		if success {
			if value, exists := currStore[args.Key]; exists {
				newMicroTxn.Value = value
				newMicroTxn.Tip = blockChain.Tip
				store.Transactions[args.UUID] = append(transaction, newMicroTxn)
				reply.Success = true
				reply.Value = value
				return nil
			}
		}
	}
	reply.Success = false
	return nil
}

// Put key value pair for transaction
func (c *ClientServer) Put(args *types.PutArgs, reply *types.PutReply) error {
	logger.Printf("Put - Key: %v, Value %v\n", args.Key, args.Value)
	store.Lock()
	defer store.Unlock()

	if transaction, ok := store.Transactions[args.UUID]; ok {
		microTxn := MicroTransaction{
			Verb:  "PUT",
			Key:   args.Key,
			Value: args.Value,
		}
		store.Transactions[args.UUID] = append(transaction, microTxn)
		reply.Success = true
	} else {
		reply.Success = false
	}
	return nil
}

// Commit current transaction, start block creation and wait until validated
func (c *ClientServer) Commit(args *types.CommitArgs, reply *types.CommitReply) error {
	logger.Printf("Commit - ValidateNum: %v, UUID: %v\n", args.ValidateNum, args.UUID[4:10])

	var newBlock *Block
	if mts, ok := store.Transactions[args.UUID]; ok {

		// initialize status map to track txn's validation or abortion
		txnTracker.Lock()
		txnTracker.ValidatedMap[args.UUID] = -1
		txnTracker.AbortedMap[args.UUID] = false
		txnTracker.Unlock()

		newBlock = &Block{
			TxnUpdates:  getTxnUpdatesFromMTS(mts),
			NodeID:      nodeID,
			ValidateNum: args.ValidateNum,
			Validated:   false,
			Aborted:     false,
			UUID:        args.UUID,
		}
		unhashedBlockCh <- newBlock

		// Sleep until validated or aborted
		for {
			// verboseLog.Println("checking validation of newBlock.UUID[:10]: ", newBlock.UUID[:10])
			// verboseLog.Println("validated: ", newBlock.Validated)
			// verboseLog.Printf("newBlock ptr: %p ", newBlock)

			// Check if txn was validated or aborted
			txnTracker.Lock()
			if (txnTracker.ValidatedMap[args.UUID] != -1) || (txnTracker.AbortedMap[args.UUID]) {
				txnTracker.Unlock()
				break
			}
			txnTracker.Unlock()

			time.Sleep(time.Second * 1)
		}
	} else {
		reply.Success = false
		return nil
	}

	logger.Println("BLOCK IS EITHER VALIDATED OR ABORTED, time to tell client")

	txnTracker.Lock()
	txID := txnTracker.ValidatedMap[args.UUID]
	txnTracker.Unlock()

	// We are here when the block has been validated or aborted
	if txID != -1 {
		reply.Success = true
		reply.TxID = txID
	} else {
		reply.Success = false
	}
	return nil
}

// Abort the current transactions temporary state
func (c *ClientServer) Abort(args *types.AbortArgs, reply *bool) error {
	logger.Println("Abort - UUID:", args.UUID)
	store.Lock()
	defer store.Unlock()

	if _, ok := store.Transactions[args.UUID]; ok {
		delete(store.Transactions, args.UUID)
		*reply = true
	}

	*reply = false
	return nil
}

// Retrieves the children blocks for a given parent hash
func (c *ClientServer) GetChildren(args *types.GetChildrenArgs, reply *types.GetChildrenReply) error {
	logger.Printf("STUB: GetChildren: %v, parentHash: %v\n", args.Node, args.ParentHash)

	var block *Block
	if args.ParentHash == "" {
		block = &blockChain.Genesis
	} else {
		var exists bool
		block, exists = getBlockByHash(args.ParentHash)
		if !exists {
			reply.Success = false
			return nil
		}
	}

	var childrenHashes []string
	for _, kid := range block.Links {
		childrenHashes = append(childrenHashes, kid.Hash)
	}
	reply.Children = childrenHashes
	reply.Success = true

	return nil
}

//=============================================================================
//============================ Node RPC Functions =============================
//=============================================================================

// receives a block from another node
func (n *NodeServer) NewBlock(block *Block, reply *bool) error {
	logger.Println("Received a block, putting it on recvCh", block)
	receivedBlockCh <- block
	*reply = true
	return nil
}

func broadcastBlock(block *Block) {
	logger.Println("Broadcasting block", block)

	nodes.Lock()
	defer nodes.Unlock()
	var wg sync.WaitGroup
	wg.Add(len(nodes.List))

	for _, node := range nodes.List {
		if node.NodeIP == externalIP {
			wg.Done()
			continue
		}

		go func(n types.Node) {
			var reply bool
			call := n.NodeClient.Go("NodeServer.NewBlock", block, &reply, nil)
			select {
			case <-call.Done:
				wg.Done()
			case <-time.After(time.Second * 2):
				wg.Done()
			}
		}(node)
	}
	wg.Wait()
}
