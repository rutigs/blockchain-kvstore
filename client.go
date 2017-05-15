/*

A trivial client to illustrate how the kvservice library can be used
from an application in assignment 7 for UBC CS 416 2016 W2.

Usage:
go run client.go
*/

package main

// Expects kvservice.go to be in the ./kvservice/ dir, relative to
// this client.go file
import (
	"./kvservice"
	"./tools"
	"bufio"
	"fmt"
	"os"
)

func main() {
	// tools constructor. Our id is unknown till we connect
	tools.NewTools(-1)

	nodes := parseNodesFile("clientConnectFile.txt")

	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	// tools constructor
	tools.NewTools(-1)

	tools.VerboseLogger.Println("nodesFile Addrs:")
	tools.VerboseLogger.Println(nodes)

	//=============================================================
	// DEV TEST

	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	success, err := t.Put("hello", "world")
	fmt.Printf("Put returned: %v, %v\n", success, err)

	success, v, err := t.Get("hello")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	success2, v2, err := t.Get("hello")
	fmt.Printf("Get returned: %v, %v, %v\n", success2, v2, err)

	success, txID, err := t.Commit(1)
	fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)

	c.Close()

	//=============================================================
}

// scans through the file corresponding to the given file path and returns slice of node addresses
func parseNodesFile(filePath string) []string {
	file, err := os.Open(filePath)
	tools.CheckError("Failed to open file path", err, true)

	var nodeAddrs []string

	// scan the file line by line
	scanner := bufio.NewScanner(file)
	for i := 0; scanner.Scan(); i++ {
		nodeAddr := scanner.Text()
		nodeAddrs = append(nodeAddrs, nodeAddr)
	}
	tools.CheckError("Failed to scan nodes file", scanner.Err(), true)

	file.Close()

	return nodeAddrs
}
