package godm

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Model interface {
	Collection() string
}

type PolymorphicModel interface {
	GetID() primitive.ObjectID
	Collection() string
}
