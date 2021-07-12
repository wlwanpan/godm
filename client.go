package godm

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"

	"github.com/hashicorp/go-multierror"
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

func (c *Client) isOnline() (bool, error) {
	db := c.mgoDb()
	statusCmd := bson.M{"serverStatus": "1"}
	result := db.RunCommand(nil, statusCmd)
	if err := result.Err(); err != nil {
		return false, err
	}

	var data map[string]interface{}
	result.Decode(&data)
	status, ok := data["ok"]
	if ok && status.(float64) == 1 {
		return true, nil
	}
	return false, nil
}

func Init(usr, pwd string) error {
	if client != nil {
		return ErrDbClientAlreadyInit
	}
	var opts *mgoOptions.ClientOptions
	var err error

	opts, err = clientOptions(usr, pwd)
	if err != nil {
		return err
	}

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
	if ok, _ := client.isOnline(); !ok {
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

func createIndexes(ctx context.Context, indexes map[string][]mongo.IndexModel) error {
	wg := sync.WaitGroup{}
	errs := &multierror.Error{}

	for collectionName, collectionIndexes := range indexes {
		wg.Add(1)
		go func(c string, i []mongo.IndexModel) {
			col := client.mgoCollection(c)
			_, err := col.Indexes().CreateMany(ctx, i)
			if err != nil {
				multierror.Append(errs, err)
			}
			wg.Done()
		}(collectionName, collectionIndexes)
	}

	wg.Wait()
	return errs
}
