package godm

import (
	"context"

	"github.com/oleiade/reflections"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2/bson"
)

func getIDField(m Model) (interface{}, error) {
	return reflections.GetField(m, "ID")
}

func Count(ctx context.Context, m Model, filter bson.M) (int64, error) {
	col := client.mgoCollection(m.Collection())
	return col.CountDocuments(ctx, filter)
}

func InsertOne(ctx context.Context, m Model) error {
	col := client.mgoCollection(m.Collection())
	_, err := col.InsertOne(ctx, m)
	return err
}

func FindByID(ctx context.Context, m Model, mid interface{}) error {
	col := client.mgoCollection(m.Collection())
	result := col.FindOne(ctx, bson.M{"_id": mid})
	if err := result.Err(); err != nil {
		return err
	}
	return result.Decode(m)
}

func FindOne(ctx context.Context, m Model, filter bson.M) error {
	col := client.mgoCollection(m.Collection())
	result := col.FindOne(ctx, filter)
	if err := result.Err(); err != nil {
		return err
	}
	return result.Decode(m)
}

func Find(ctx context.Context, m Model, filter bson.M, opts ...*options.FindOptions) (*mongo.Cursor, error) {
	col := client.mgoCollection(m.Collection())
	return col.Find(ctx, filter, opts...)
}

func FindIter(ctx context.Context, m Model, filter bson.M, opts ...*options.FindOptions) (*QueryIter, error) {
	cur, err := Find(ctx, m, filter, opts...)
	if err != nil {
		return nil, err
	}
	return &QueryIter{
		cur: cur,
		m:   m,
	}, nil
}

func DeleteOne(ctx context.Context, m Model, filter bson.M) error {
	col := client.mgoCollection(m.Collection())
	_, err := col.DeleteOne(ctx, filter)
	return err
}

func DeleteMany(ctx context.Context, m Model, filter bson.M) (*mongo.DeleteResult, error) {
	col := client.mgoCollection(m.Collection())
	return col.DeleteMany(ctx, filter)
}

func UpdateOne(ctx context.Context, m Model, updates Updates, opts ...*options.UpdateOptions) error {
	mid, err := getIDField(m)
	if err != nil {
		return err
	}

	updates.updateTimeStampsIfPresent(m)

	col := client.mgoCollection(m.Collection())
	filter := bson.M{"_id": mid}
	update := bson.M{"$set": updates}

	_, err = col.UpdateOne(ctx, filter, update, opts...)
	if err != nil {
		return err
	}

	currBson, err := ModelToBson(m)
	if err != nil {
		return err
	}

	ExtendBson(currBson, bson.M(updates))

	return BsonToModel(currBson, m)
}

func UpdateMany(ctx context.Context, m Model, filter bson.M, updates Updates, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	col := client.mgoCollection(m.Collection())

	updates.updateTimeStampsIfPresent(m)
	update := bson.M{"$set": updates}

	return col.UpdateMany(ctx, filter, update, opts...)
}

func RunInTransaction(ctx context.Context, runOps func(mongo.SessionContext, func() error) error) error {
	dbSession, err := client.mgoClient.StartSession()
	if err != nil {
		return err
	}
	defer dbSession.EndSession(ctx)

	err = mongo.WithSession(ctx, dbSession, func(sessionContext mongo.SessionContext) error {
		if err := dbSession.StartTransaction(); err != nil {
			return err
		}
		return runOps(sessionContext, func() error {
			return dbSession.CommitTransaction(sessionContext)
		})
	})

	if err != nil {
		return dbSession.AbortTransaction(ctx)
	}
	return nil
}
