package main

import (
	"fmt"
	"time"

	cmgr "github.com/crearosoft/corelib/cachemanager"
)

// import (
// 	"time"

// 	"github.com/crearosoft/corelib/authmanager"
// 	"github.com/crearosoft/corelib/loggermanager"
// 	"go.uber.org/zap/zapcore"
// )

// func init() {
// 	authmanager.GlobalJWTKey = "Crearosoft"
// }

func main() {
	var c cmgr.CacheHelper
	c.Setup(-1, 5*time.Second, 10*time.Second)
	c = *cmgr.SetupCache()
	c.SetWithExpiration("hello", "world", 5*time.Second)
	fmt.Println(c.Get("hello"))
	time.Sleep(11 * time.Second)
	fmt.Println(c.Get("hello"))
	// loggermanager.Init("./data/logs/cdn.log", int(10), int(1), int(5), zapcore.DebugLevel)
	// // loggermanager.LogDebug("Hello world")
	// token, _ := authmanager.GenerateToken("dhawalvd", time.Now().Add(24*time.Hour).Unix())
	// loggermanager.LogError(token)
	// claims, _ := authmanager.DecodeJWTToken(token)
	// loggermanager.LogDebug(claims)
}
