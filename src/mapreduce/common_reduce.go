package mapreduce

// TODO remove logrus

import (
	logger "github.com/sirupsen/logrus"
	"os"
	"encoding/json"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	logger.WithFields(logger.Fields{
		"jobName": jobName,
		"reduceTask": reduceTask,
		"output": outFile,
		"nb map running": nMap,
	}).Info("[doReduce]reduce process")

	reducedData := make(map[string][]string, 0)

	for mapTask := 0 ; mapTask < nMap ; mapTask++ {
		filename := reduceName(jobName, mapTask, reduceTask)
		if fd, err := os.OpenFile(filename, os.O_RDONLY, 0666); err != nil {
			logger.WithError(err).Error("[doReduce] fail to call OpenFile")
		} else {
			tmp := &[]KeyValue{}
			jsonDecoder := json.NewDecoder(fd)
			if err := jsonDecoder.Decode(tmp); err != nil {
				logger.WithError(err).Error("[doReduce] fail to call Decode")
			} else {
				logger.WithField("keyValue", *tmp).Info("[doMap]")
				for _, keyValue := range *tmp {
					reducedData[keyValue.Key] = append(reducedData[keyValue.Key], keyValue.Value)
				}
			}
			fd.Close()
		}
	}

	if fdOutputFile, err := os.OpenFile(outFile, os.O_WRONLY | os.O_CREATE, 0666); err != nil {
		logger.WithError(err).Error("[doReduce]fail to call OpenFile")
	} else {
		jsonEncoder := json.NewEncoder(fdOutputFile)
		for key, value := range reducedData {
			if err := jsonEncoder.Encode(KeyValue{Key: key, Value:reduceF(key, value)}); err != nil {
				logger.WithError(err).Error("[doReduce] fail to encode")
			}
		}
		fdOutputFile.Close()
	}
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
