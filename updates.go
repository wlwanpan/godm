package godm

import (
	"time"

	"github.com/oleiade/reflections"
	"gopkg.in/mgo.v2/bson"
)

type Updates bson.M

func (u Updates) updateTimeStampsIfPresent(m Model) {
	has, _ := reflections.HasField(m, "UpdatedAt")
	if has {
		u["updated_at"] = time.Now()
	}
}

func ExtendBson(dst bson.M, src bson.M) {
	for k, v := range src {
		dst[k] = v
	}
}

func ModelToBson(m Model) (bson.M, error) {
	b, err := bson.Marshal(m)
	if err != nil {
		return nil, err
	}

	r := bson.M{}
	return r, bson.Unmarshal(b, r)
}

func ModelToUpdates(m Model) (Updates, error) {
	bm, err := ModelToBson(m)
	if err != nil {
		return nil, err
	}
	return Updates(bm), nil
}

func BsonToModel(b bson.M, m Model) error {
	d, err := bson.Marshal(b)
	if err != nil {
		return err
	}
	return bson.Unmarshal(d, m)
}
