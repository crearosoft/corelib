package mongodb

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo/readpref"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/crearosoft/corelib/loggermanager"
	"github.com/tidwall/gjson"
)

// MongoHost -MongoHost
type MongoHost struct {
	HostName        string        `json:"hostName"`
	Server          string        `json:"server"`
	Port            int           `json:"port"`
	Username        string        `json:"username"`
	Password        string        `json:"password"`
	Database        string        `json:"database"`
	IsDefault       bool          `json:"isDefault"`
	MaxIdleConns    int           `json:"maxIdleConns" `
	MaxOpenConns    int           `json:"maxOpenConns"`
	ConnMaxLifetime time.Duration `json:"connMaxLifetime" `
	IsDisabled      bool          `json:"isDisabled" `
}

var instances map[string]*mongo.Client
var mutex sync.Mutex
var once sync.Once

var config map[string]MongoHost

var defaultHost string

func init() {
	config = make(map[string]MongoHost)
}

// InitUsingJSON initializes Mongo Connections for give JSON data
func InitUsingJSON(configs []MongoHost) error {
	var sessionError error
	once.Do(func() {
		defer mutex.Unlock()
		mutex.Lock()
		config = make(map[string]MongoHost)
		instances = make(map[string]*mongo.Client)
		for _, hostDetails := range configs {
			if hostDetails.IsDisabled {
				continue
			}
			clientOption := options.Client()
			clientOption.SetHosts([]string{bindMongoServerWithPort(hostDetails.Server, hostDetails.Port)}).
				SetConnectTimeout(hostDetails.ConnMaxLifetime).
				SetMaxPoolSize(uint64(hostDetails.MaxOpenConns)).
				SetReadPreference(readpref.Primary()).
				SetDirect(true) // important if in cluster, connect to primary only.
			if hostDetails.Username != "" {
				cred := options.Credential{}
				cred.Username = hostDetails.Username
				cred.Password = hostDetails.Password
				cred.AuthSource = hostDetails.Database
				clientOption.SetAuth(cred)
			}
			client, err := mongo.NewClient(clientOption)
			if err != nil {
				sessionError = err
				loggermanager.LogError(sessionError)
				return
			}
			err = client.Connect(context.Background())
			if err != nil {
				sessionError = err
				loggermanager.LogError(sessionError)
				return
			}
			err = client.Ping(context.Background(), readpref.Primary())
			if err != nil {
				sessionError = err
				loggermanager.LogError("failed to connect to primary - ", sessionError)
				return
			}
			instances[hostDetails.HostName] = client
			if hostDetails.IsDefault {
				defaultHost = hostDetails.HostName
			}
			config[hostDetails.HostName] = hostDetails
		}
	})
	return sessionError
}

// DeleteSession -DeleteSession
func DeleteSession(hostName string) error {
	defer mutex.Unlock()
	mutex.Lock()
	if _, ok := instances[hostName]; !ok {
		return loggermanager.Wrap("NO_HOST_FOUND")
	}
	delete(instances, hostName)
	return nil
}

// InitNewSession - InitNewSession
func InitNewSession(hostDetails MongoHost) error {
	defer mutex.Unlock()
	mutex.Lock()
	if instances == nil {
		instances = make(map[string]*mongo.Client)
	}
	if _, ok := instances[hostDetails.HostName]; ok {
		return loggermanager.Wrap("DUPLICATE_HOSTNAME")
	}
	clientOption := options.Client()
	clientOption.SetHosts([]string{bindMongoServerWithPort(hostDetails.Server, hostDetails.Port)}).
		SetConnectTimeout(hostDetails.ConnMaxLifetime).
		SetMaxPoolSize(uint64(hostDetails.MaxOpenConns)).
		SetReadPreference(readpref.Primary()).
		SetDirect(true) // important if in cluster, connect to primary only.
	if hostDetails.Username != "" {
		cred := options.Credential{}
		cred.Username = hostDetails.Username
		cred.Password = hostDetails.Password
		cred.AuthSource = hostDetails.Database
		clientOption.SetAuth(cred)
	}
	client, err := mongo.NewClient(clientOption)
	if err != nil {
		loggermanager.LogError(err)
		return err
	}
	err = client.Connect(context.Background())
	if err != nil {
		loggermanager.LogError(err)
		return err
	}
	instances[hostDetails.HostName] = client
	return nil
}

//GetMongoConnection method
func GetMongoConnection(hostName string) (*mongo.Client, error) {
	mutex.Lock()
	defer mutex.Unlock()
	if instances == nil {
		return nil, loggermanager.Wrap("MONGO_INIT_NOT_DONE")
	}
	if hostName == "" {
		if instance, ok := instances[defaultHost]; ok {
			err := instance.Ping(context.Background(), readpref.Primary())
			if err != nil {
				loggermanager.LogError(err)
				return nil, err
			}
			return instance, nil
		}
	}
	if instance, ok := instances[hostName]; ok {
		err := instance.Ping(context.Background(), readpref.Primary())
		if err != nil {
			loggermanager.LogError(err)
			return nil, err
		}
		return instance, nil
	}
	return nil, loggermanager.Wrap("Session not found for instance: " + hostName)
}

// MongoDAO mongo DAO struct
type MongoDAO struct {
	hostName       string
	collectionName string
}

// GetMongoDAOWithHost return mongo DAO instance
func GetMongoDAOWithHost(host, collection string) *MongoDAO {
	return &MongoDAO{
		hostName:       host,
		collectionName: collection,
	}
}

// GetMongoDAO return mongo DAO instance
func GetMongoDAO(collection string) *MongoDAO {
	return &MongoDAO{
		collectionName: collection,
	}
}

// SaveData Save data in mongo db
func (mg *MongoDAO) SaveData(data interface{}) (string, error) {
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return "", sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return "", loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	opts, insertError := collection.InsertOne(context.Background(), data)
	if insertError != nil {
		return "", insertError
	}
	return getInsertedId(opts.InsertedID), nil
}

// UpdateAll update all
func (mg *MongoDAO) UpdateAll(selector map[string]interface{}, data interface{}) error {
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)

	_, updateError := collection.UpdateMany(context.Background(), selector, bson.M{"$set": data})
	if updateError != nil {
		return updateError
	}
	return nil
}

// Update will update single entry
func (mg *MongoDAO) Update(selector map[string]interface{}, data interface{}) error {
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	_, updateError := collection.UpdateOne(context.Background(), selector, bson.M{"$set": data})
	if updateError != nil {
		return updateError
	}
	return nil
}

// GetData will return query for selector
func (mg *MongoDAO) GetData(selector map[string]interface{}) (*gjson.Result, error) {
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return nil, sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return nil, loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)

	cur, err := collection.Find(context.Background(), selector)
	if err != nil {
		loggermanager.LogError(err)
		return nil, err
	}
	defer cur.Close(context.Background())
	var results []interface{}
	for cur.Next(context.Background()) {
		var result bson.M
		err := cur.Decode(&result)
		if err != nil {
			loggermanager.LogError(err)
			return nil, err
		}
		results = append(results, result)
	}
	ba, marshalError := json.Marshal(results)
	if marshalError != nil {
		return nil, marshalError
	}
	rs := gjson.ParseBytes(ba)
	return &rs, nil
}

// DeleteData will delete data given for selector
func (mg *MongoDAO) DeleteData(selector map[string]interface{}) error {
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	_, deleteError := collection.DeleteOne(context.Background(), selector)
	if deleteError != nil {
		return deleteError
	}
	return deleteError
}

// DeleteAll will delete all the matching data given for selector
func (mg *MongoDAO) DeleteAll(selector map[string]interface{}) error {
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	_, deleteError := collection.DeleteMany(context.Background(), selector)
	if deleteError != nil {
		return deleteError
	}
	return deleteError
}

// GetProjectedData will return query for selector and projector
func (mg *MongoDAO) GetProjectedData(selector map[string]interface{}, projector map[string]interface{}) (*gjson.Result, error) {
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return nil, sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return nil, loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	ops := &options.FindOptions{}
	ops.Projection = projector
	cur, err := collection.Find(context.Background(), selector, ops)
	if err != nil {
		loggermanager.LogError(err)
		return nil, err
	}
	defer cur.Close(context.Background())
	var results []interface{}
	for cur.Next(context.Background()) {
		var result bson.M
		err := cur.Decode(&result)
		if err != nil {
			loggermanager.LogError(err)
			return nil, err
		}
		results = append(results, result)
	}

	ba, marshalError := json.Marshal(results)
	if marshalError != nil {
		return nil, marshalError
	}
	rs := gjson.ParseBytes(ba)
	return &rs, nil
}

// GetAggregateData - return result using aggregation query
func (mg *MongoDAO) GetAggregateData(selector interface{}) (*gjson.Result, error) {
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return nil, sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return nil, loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	cur, err := collection.Aggregate(context.Background(), selector)
	if err != nil {
		loggermanager.LogError(err)
		return nil, err
	}
	defer cur.Close(context.Background())
	var results []interface{}
	for cur.Next(context.Background()) {
		var result bson.M
		err := cur.Decode(&result)
		if err != nil {
			loggermanager.LogError(err)
			return nil, err
		}
		results = append(results, result)
	}
	ba, marshalError := json.Marshal(results)
	if marshalError != nil {
		return nil, marshalError
	}
	rs := gjson.ParseBytes(ba)
	return &rs, nil
}

// UpsertWithID - will update or upsert a document in the collection
//
// If a new document is upserted then it will return the ObjectID (string) of the upserted document.
//
// If no document is upserted the object id returned will be empty string.
func (mg *MongoDAO) UpsertWithID(selector map[string]interface{}, data interface{}) (string, error) {
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return "", sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return "", loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	ops := options.UpdateOptions{}
	ops.SetUpsert(true)
	upsertRes, updateError := collection.UpdateOne(context.Background(), selector, bson.M{"$set": data}, &ops)
	if updateError != nil {
		return "", updateError
	}
	if upsertRes.UpsertedID != nil {
		return getInsertedId(upsertRes.UpsertedID), nil
	}
	return "", nil
}

// Upsert will update single entry
func (mg *MongoDAO) Upsert(selector map[string]interface{}, data interface{}) error {
	/* session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	ops := options.UpdateOptions{}
	ops.SetUpsert(true)
	_, updateError := collection.UpdateOne(context.Background(), selector, bson.M{"$set": data}, &ops)
	if updateError != nil {
		return updateError
	}
	return nil */
	_, err := mg.UpsertWithID(selector, data)
	return err
}

// PushData - append in array
func (mg *MongoDAO) PushData(selector map[string]interface{}, data interface{}) error {
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	_, updateError := collection.UpdateMany(context.Background(), selector, bson.M{"$push": data})
	if updateError != nil {
		return updateError
	}

	return nil
}

// CustomUpdate - CustomUpdate
func (mg *MongoDAO) CustomUpdate(selector map[string]interface{}, data interface{}) error {
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	_, updateError := collection.UpdateMany(context.Background(), selector, data)
	if updateError != nil {
		return updateError
	}
	return nil
}

// CustomUpdateOne - CustomUpdateOne
func (mg *MongoDAO) CustomUpdateOne(selector map[string]interface{}, data interface{}) error {
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	_, updateError := collection.UpdateOne(context.Background(), selector, data)
	if updateError != nil {
		return updateError
	}
	return nil
}

/************************* BULK Functionalities ******************************/

// BulkSaveData ata Save data in mongo db in bulk
func (mg *MongoDAO) BulkSaveData(data []interface{}) error {
	if checkBulkInput(data) {
		return nil
	}
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	opts := &options.BulkWriteOptions{}
	opts.SetOrdered(true)
	var models []mongo.WriteModel
	for i := 0; i < len(data); i++ {
		model := mongo.NewInsertOneModel()
		model.SetDocument(data[i])
		models = append(models, model)
	}
	_, insertError := collection.BulkWrite(context.Background(), models, opts)
	if insertError != nil {
		loggermanager.LogError(insertError)
		return insertError
	}

	return nil
}

// BulkUpdateData  update data in mongo db in bulk
func (mg *MongoDAO) BulkUpdateData(data []interface{}) error {
	if checkBulkInput(data) {
		return nil
	}
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	opts := &options.BulkWriteOptions{}
	opts.SetOrdered(true)
	var models []mongo.WriteModel
	for i := 0; i < len(data); i++ {
		model := mongo.NewUpdateOneModel()
		model.SetFilter(data[i])
		i++
		model.SetUpdate(data[i])
		models = append(models, model)
	}

	_, insertError := collection.BulkWrite(context.Background(), models, opts)
	if insertError != nil {
		loggermanager.LogError(insertError)
		return insertError
	}
	return nil
}

// BulkDeleteData  delete data in mongo db in bulk
func (mg *MongoDAO) BulkDeleteData(data []interface{}) error {
	if checkBulkInput(data) {
		return nil
	}
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	opts := &options.BulkWriteOptions{}
	opts.SetOrdered(true)
	var models []mongo.WriteModel
	for i := 0; i < len(data); i++ {
		model := mongo.NewDeleteOneModel()
		model.SetFilter(data[i])
		models = append(models, model)
	}
	_, insertError := collection.BulkWrite(context.Background(), models, opts)
	if insertError != nil {
		loggermanager.LogError(insertError)
		return insertError
	}
	return nil
}

// BulkUpsertData  Upsert data in mongo db in bulk
func (mg *MongoDAO) BulkUpsertData(data []interface{}) error {
	if checkBulkInput(data) {
		return nil
	}
	session, sessionError := GetMongoConnection(mg.hostName)
	if sessionError != nil {
		return sessionError
	}

	if mg.hostName == "" {
		mg.hostName = defaultHost
	}
	db, ok := config[mg.hostName]
	if !ok {
		return loggermanager.Wrap("No_Configuration_Found_For_Host: " + mg.hostName)
	}
	collection := session.Database(db.Database).Collection(mg.collectionName)
	opts := &options.BulkWriteOptions{}
	opts.SetOrdered(true)
	var models []mongo.WriteModel
	for i := 0; i < len(data); i++ {
		model := mongo.NewUpdateOneModel()
		model.SetUpsert(true)
		model.SetFilter(data[i])
		i++
		model.SetUpdate(data[i])
		models = append(models, model)
	}

	_, insertError := collection.BulkWrite(context.Background(), models, opts)
	if insertError != nil {
		loggermanager.LogError(insertError)
		return insertError
	}
	return nil
}

func checkBulkInput(d []interface{}) bool {
	return len(d) == 0
}

func bindMongoServerWithPort(server string, port int) string {
	// if port is empty then used default port 27017 & bind to server ip
	var serverURI string
	if port <= 0 || strings.TrimSpace(strconv.Itoa(port)) == "" {
		serverURI = server + ":27017"
	} else {
		serverURI = server + ":" + strconv.Itoa(port)
	}
	return serverURI
}

func getInsertedId(id interface{}) string {
	switch v := id.(type) {
	case string:
		return v
	case primitive.ObjectID:
		return v.Hex()
	default:
		return ""
	}
}
