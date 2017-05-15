//==========================================================================================
// ===================================== General Tools =====================================
//==========================================================================================
/*
	This file contains general use functions meant to be imported and used anywhere.
*/

package tools

import (
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

// Error logger for this package only for CheckError
var errorLogger *log.Logger

// The constructor for the tools package
func NewTools(id int) (verboseLogger *log.Logger, logger *log.Logger) {
	errorLogger = log.New(os.Stderr, fmt.Sprintf("%v: [ERR] ", id), log.Lshortfile)

	verboseLogger = log.New(os.Stdout, fmt.Sprintf("%v: [VLOG] ", id), log.Lshortfile)
	logger = log.New(os.Stdout, fmt.Sprintf("%v: [LOG] ", id), log.Lshortfile)

	logger.Println("Seeding rand with: ", time.Now().UTC().UnixNano())
	return
}

// para: str error message
// prints the given error msg on a new line if not null
// return: none
func CheckError(msg string, err error, exit bool) {
	if err != nil {
		errorLogger.Println("ERROR STR: ", msg)
		errorLogger.Println("ERROR: ", err)
		if exit {
			os.Exit(0)
		}
	}
}

func GetUUID() (string, error) {
	rand.Seed(time.Now().UTC().UnixNano())

	u := make([]byte, 16)
	_, err := rand.Read(u)
	if err != nil {
		return "", nil
	}

	u[8] = (u[8] | 0x80) & 0xBF
	u[6] = (u[6] | 0x40) & 0x4F
	return hex.EncodeToString(u), nil
}
