package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"os"
	"time"
)

const KEY = "value"

type RedisClient struct {
	sourceClient *redis.Client
	replicaClient *redis.Client
}

var (
	client *RedisClient
	ctx = context.Background()
)

func main() {
	fmt.Println("Start...\nConnect clients...")
	client = Connect()

	fmt.Println("Test connections...")
	TestConnections()

	fmt.Println("Remove stale data...")
	Cleanup()

	fmt.Println("Data ingestion (insert the values 1-100)...")
	inNum := InsertValues()
	fmt.Printf("Inserted %d values\n", inNum)

	fmt.Println("Results (1-100 in reverse order)...")
	Display()

	fmt.Println("Close connections...")
	Close()
}

// Connect - connect 2 redis clients: sourceClient and replicaClient
func Connect() *RedisClient {
	sourceConnectStr := fmt.Sprintf("%s:%s", "172.16.22.21", "14239")
	rdb1 := redis.NewClient(&redis.Options{
		Addr: sourceConnectStr,
		DB: 0, // default DB
	})

	if rdb1 == nil {
		fmt.Printf("source client not assigned: %s", sourceConnectStr)
		os.Exit(1)
	}

	replicaConnectStr := fmt.Sprintf("%s:%s", "172.16.22.21", "11997")
	rdb2 := redis.NewClient(&redis.Options{
		Addr: replicaConnectStr,
		DB: 0, // default DB
	})

	if rdb2 == nil {
		fmt.Printf("replica client not assigned: %s", replicaConnectStr)
		os.Exit(1)
	}

	redisClients := &RedisClient{
		sourceClient: rdb1,
		replicaClient: rdb2,
	}

	return redisClients
}

// TestConnections - Verify that both clients are connected
func TestConnections() {
	if !client.PING() {
		fmt.Println("Failed to connect properly")
		return
	}
}


// Cleanup - remove left over data if any
func Cleanup() {
	client.DEL()
}

// InsertValues - Create the set of values and their incremental order. Then add a zset in redis
func InsertValues() int64 {
	members := make([]redis.Z, 100)
	for i := 0; i < 100; i++ {
		member := redis.Z{
			Score: float64(i),
			Member: i+1,
		}

		members[i] = member
	}

	// Make 1 call to redis
	cnt, err := client.ZADD(members)
	if err != nil {
		fmt.Printf("Error Adding members: %s", err)
		os.Exit(1)
	}

	return cnt
}

// Display - Print out the zset contents in reverse order
func Display() {
	// arbitrary time to wait for replication. (use a more reliable method if necessary)
	time.Sleep(2 * time.Second)

	values, err := client.ZREVRANGEWITHSCORES()
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}

	for _, v := range values {
		fmt.Printf("%s\n", v.Member)
	}
}

// Close - close out both connections
func Close() {
	client.CLOSE()
}

// Redis wrapper functions...
func (rc *RedisClient) PING() bool {
	pong, err := rc.sourceClient.Ping(ctx).Result()
	if err != nil {
		fmt.Printf("Redis error: %s\n", err)
		return false
	}

        fmt.Printf("Ping source response: %s\n", pong)

	pong2, err2 := rc.replicaClient.Ping(ctx).Result()
	if err2 != nil {
		fmt.Printf("Redis error: %s\n", err2)
		return false
	}

	fmt.Printf("Ping replica response: %s\n", pong2)

	return true
}

func (rc *RedisClient) DEL() {
	rc.sourceClient.Del(ctx, KEY)
}

func (rc *RedisClient) ZADD(members []redis.Z) (int64, error) {
	cmd := rc.sourceClient.ZAdd(ctx, KEY, members...)

	return cmd.Result()
}

func (rc *RedisClient) ZREVRANGEWITHSCORES() ([]redis.Z, error) {
	cmd := rc.replicaClient.ZRevRangeWithScores(ctx, KEY, 0, 100)

	return cmd.Result()
}

func (rc *RedisClient) CLOSE() {
	rc.sourceClient.Close()
	rc.replicaClient.Close()
}
