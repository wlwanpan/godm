package godm

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	mgoOptions "go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2/bson"
)

const (
	atlasURL = "mongodb+srv://%s:%s@%s/test"

	atlasHost = "cluster0-y5mze.mongodb.net"

	gCardDbName = "GCard"

	localMongoURL = "mongodb://127.0.0.1:27017"
)

var (
	ErrDbClientAlreadyInit = errors.New("db: client already initialized")

	ErrDbClientNotInit = errors.New("db: client not initialized")

	ErrDbStatusCheckFailed = errors.New("db: status check failed")

	ErrDbEnvVariables = errors.New("db: missing db env variables")
)

var (
	client *Client

	atlasQueryParams = map[string]string{
		"retryWrites": "true",
		"w":           "majority",
	}
)

type Client struct {
	mgoClient *mongo.Client
}

func (c *Client) mgoDb() *mongo.Database {
	return c.mgoClient.Database(gCardDbName)
}

func (c *Client) mgoCollection(colname string) *mongo.Collection {
	return c.mgoDb().Collection(colname)
}

func (c *Client) connect(ctx context.Context) error {
	return c.mgoClient.Connect(ctx)
}

func (c *Client) disconnect(ctx context.Context) error {
	return c.mgoClient.Disconnect(ctx)
}

func (c *Client) isOnline() bool {
	db := c.mgoDb()
	statusCmd := bson.M{"serverStatus": "1"}
	result := db.RunCommand(nil, statusCmd)
	if result.Err() != nil {
		log.Printf("Error fetching server status: %s", result.Err().Error())
		return false
	}

	var data map[string]interface{}
	result.Decode(&data)
	status, ok := data["ok"]
	if ok && status.(float64) == 1 {
		return true
	}
	return false
}

func Init() error {
	if client != nil {
		return ErrDbClientAlreadyInit
	}
	var opts *mgoOptions.ClientOptions
	var err error
	dbUsr, dbPwd := os.Getenv("DB_USERNAME"), os.Getenv("DB_PASSWORD")

	if os.Getenv("LOCAL_DB") == "1" {
		log.Println("Connecting to LOCAL mongodb.")
		opts = mgoOptions.Client().ApplyURI(localMongoURL)
	} else if dbUsr == "" || dbPwd == "" {
		return ErrDbEnvVariables
	} else {
		log.Println("Connecting to HOSTED mongodb.")
		opts, err = clientOptions(dbUsr, dbPwd)
	}

	if err != nil {
		return err
	}

	log.Println("initializing mgo client: ", opts.GetURI())
	c, err := mongo.NewClient(opts)
	if err != nil {
		return err
	}

	client = &Client{mgoClient: c}
	return nil
}

func Connect(ctx context.Context) error {
	if client == nil {
		return ErrDbClientNotInit
	}
	if err := client.connect(ctx); err != nil {
		return err
	}
	if !client.isOnline() {
		return ErrDbStatusCheckFailed
	}
	return nil
}

func Disconnect(ctx context.Context) error {
	if client == nil {
		return nil
	}
	if err := client.disconnect(ctx); err != nil {
		return err
	}
	client = nil
	return nil
}

func clientOptions(usr, pwd string) (*mgoOptions.ClientOptions, error) {
	endpoint := fmt.Sprintf(atlasURL, usr, pwd, atlasHost)
	uri, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	q := uri.Query()
	for k, v := range atlasQueryParams {
		q.Set(k, v)
	}

	uri.RawQuery = q.Encode()
	return mgoOptions.Client().ApplyURI(uri.String()), nil
}

func createIndexes(indexes map[string][]mongo.IndexModel) {
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for collectionName, collectionIndexes := range indexes {
		wg.Add(1)
		go func(c string, i []mongo.IndexModel) {
			col := client.mgoCollection(c)
			_, err := col.Indexes().CreateMany(ctx, i)
			if err != nil {
				log.Printf("db client: error creating indexes for %s", c)
				log.Println(err)
			} else {
				log.Printf("db client: successfully created indexes for %s", c)
			}
			wg.Done()
		}(collectionName, collectionIndexes)
	}

	wg.Wait()
}
