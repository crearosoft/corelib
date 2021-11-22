package mongodb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//SaveFileToGridFS - Saves file to gridfs
func SaveFileToGridFS(db *mongo.Database, bucketName, fileName string, source io.Reader) (string, string, error) {

	bucketName = strings.TrimSpace(bucketName)
	fileName = strings.TrimSpace(fileName)

	//Validations
	if db == nil {
		return "", "", errors.New("db Required")
	} else if bucketName == "" {
		return "", "", errors.New("bucketName required")
	} else if source == nil {
		return "", "", errors.New("invalid source")
	}

	//Set bucket config
	bucketOptions := options.BucketOptions{}
	bucketOptions.Name = &bucketName

	//Get bucket instance
	dbBucket, bucketError := gridfs.NewBucket(db, &bucketOptions)
	if bucketError != nil {
		return "", "", bucketError
	}

	//Upload incomming file to bucket
	fileID, fileError := dbBucket.UploadFromStream(fileName, source)
	if fileError != nil {
		return "", "", fileError
	}

	//Return generated fileId and file name
	return fileID.String(), fileName, nil
}

//GetFileFromGridFS - Gets file from gridfs
func GetFileFromGridFS(db *mongo.Database, bucketName, fileName string) ([]byte, error) {

	bucketName = strings.TrimSpace(bucketName)
	fileName = strings.TrimSpace(fileName)

	//Validations
	if db == nil {
		return nil, errors.New("db Required")
	} else if bucketName == "" {
		return nil, errors.New("bucketName required")
	} else if fileName == "" {
		return nil, errors.New("fileName required'")
	}

	//Set bucket config
	bucketOptions := options.BucketOptions{}
	bucketOptions.Name = &bucketName

	//Get bucket instance
	dbBucket, bucketError := gridfs.NewBucket(db, &bucketOptions)
	if bucketError != nil {
		return nil, bucketError
	}

	//Read file from DB
	w := bytes.NewBuffer(make([]byte, 0))
	_, getFileError := dbBucket.DownloadToStreamByName(fileName, w)
	if getFileError != nil {
		return nil, getFileError
	}

	fileBytes := w.Bytes()

	//Return bytes
	return fileBytes, nil

}

//GetDBInstance - Gets database intance
func GetDBInstance(serverIPAddress, port, dbName string, timeOutInSeconds int) (*mongo.Database, error) {

	serverIPAddress = strings.TrimSpace(serverIPAddress)
	dbName = strings.TrimSpace(dbName)
	port = strings.TrimSpace(port)

	//Validations
	if serverIPAddress == "" {
		return nil, errors.New("serverIPAddress required")
	} else if dbName == "" {
		return nil, errors.New("dbName required")
	} else if timeOutInSeconds <= 0 {
		return nil, errors.New("valid timeOutInSeconds required")
	}

	ipElements := strings.Split(serverIPAddress, ".")
	if len(ipElements) != 4 {
		return nil, errors.New("invalid serverIPAddress")
	}

	if port == "" {
		port = "27017"
	}

	//Connection string
	connectionString := "mongodb://" + serverIPAddress + ":" + port
	client, connectionError := mongo.NewClient(options.Client().ApplyURI(connectionString))
	if connectionError != nil {
		return nil, connectionError
	}

	//Context with timeout
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(timeOutInSeconds)*time.Second)
	contextError := client.Connect(ctx)

	if contextError != nil {
		return nil, contextError
	}

	//Create a db instance
	db := client.Database(dbName)

	//Return db instance
	return db, nil
}
