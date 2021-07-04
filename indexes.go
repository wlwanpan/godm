package godm

import (
	"os"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/yaml.v2"
)

const (
	indexesFilename string = "indexes.yml"
)

type indexesMapList []map[string]Index

type IndexOpts struct {
	Unique bool `yaml:"unique"`
	Sparse bool `yaml:"sparse"`
}

type Index struct {
	Fields  bson.M    `yaml:"fields"`
	Options IndexOpts `yaml:"options"`
}

func BuildIndexes() error {
	iml, err := readIndexesFromFile(indexesFilename)
	if err != nil {
		return err
	}

	parsedIndexes := parseIndexesToMgoIndexes(iml)
	createIndexes(parsedIndexes)
	return nil
}

func parseIndexesToMgoIndexes(iml indexesMapList) map[string][]mongo.IndexModel {
	results := map[string][]mongo.IndexModel{}
	for _, ml := range iml {
		for k, v := range ml {
			indexModel := mongo.IndexModel{
				Keys: v.Fields,
				Options: &options.IndexOptions{
					Unique: &v.Options.Unique,
					Sparse: &v.Options.Sparse,
				},
			}

			results[k] = append(results[k], indexModel)
		}
	}
	return results
}

func readIndexesFromFile(filename string) (indexesMapList, error) {
	indexFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer indexFile.Close()

	var results indexesMapList
	decoder := yaml.NewDecoder(indexFile)
	if err := decoder.Decode(&results); err != nil {
		return nil, err
	}
	return results, nil
}
