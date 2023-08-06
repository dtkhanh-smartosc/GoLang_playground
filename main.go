package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"golang_test/data"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	start := time.Now()
	combine()
	elapsed := time.Since(start)
	log.Printf("combine took %s", elapsed)
}

func writeFileCSV() {
	// records = main.Records
	file, err := os.Create("records.csv")
	defer file.Close()
	if err != nil {
		log.Fatalln("failed to open file", err)
	}
	w := csv.NewWriter(file)
	defer w.Flush()
	// Using Write
	for _, record := range data.Records {
		row := []string{record.ID, strconv.Itoa(record.Age)}
		if err := w.Write(row); err != nil {
			log.Fatalln("error writing record to file", err)
		}
	}

	// Using WriteAll
	// var data [][]string
	// for _, record := range data.Records {
	// 	row := []string{record.ID, strconv.Itoa(record.Age)}
	// 	data = append(data, row)
	// }
	// w.WriteAll(data)
}

func createClient() *minio.Client {
	endpoint := "localhost:9000"
	accessKeyID := "B2xRUNIy9rQYa197jnqk"
	secretAccessKey := "Ht6jfo8Pso5mPdEX7VqHxd6qeHfdnfUYZtOQ8NUz"
	useSSL := false

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}
	// Make a new bucket called mycsv.
	bucketName := "mycsv"
	location := "us-east-1"

	err = minioClient.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{Region: location})
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := minioClient.BucketExists(context.Background(), bucketName)
		if errBucketExists == nil && exists {
			log.Printf("We already own %s\n", bucketName)
		} else {
			log.Fatalln(err)
		}
	} else {
		log.Printf("Successfully created %s\n", bucketName)
	}
	return minioClient
	// objectName := "kkkk.csv"
	// filePath := "records.csv"
	// contentType := "application/csv"
	// info, err := minioClient.FPutObject(context.Background(), bucketName, objectName, filePath, minio.PutObjectOptions{ContentType: contentType})
	// if err != nil {
	// 	log.Fatalln(err)
	// }

	// log.Printf("Successfully uploaded %s of size %d\n", objectName, info.Size)
}

func combine() {
	defer os.Remove("records.csv")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		writeFileCSV()
	}()
	var minioClient *minio.Client
	wg.Add(1)
	go func() {
		defer wg.Done()
		minioClient = createClient()
	}()

	wg.Wait()
	start := time.Now()

	// minioClient = createClient()
	year, month, day := time.Now().Date()

	objectName := "%v-%v-%v.csv"
	objectName = fmt.Sprintf(objectName, day, month, year)
	bucketName := "mycsv"
	filePath := "records.csv"
	contentType := "application/csv"
	info, err := minioClient.FPutObject(context.Background(), bucketName, objectName, filePath, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		log.Fatalln(err)
	}
	elapsed := time.Since(start)
	log.Printf("putfile took %s", elapsed)
	log.Printf("Successfully uploaded %s of size %d\n", objectName, info.Size)
}
