package main

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/ngc224/awslogger"
)

func main() {
	logger, err := awslogger.New("TestGroup", "TestStream", &aws.Config{
		Region: aws.String("ap-northeast-1"),
	})

	if err != nil {
		os.Exit(1)
	}

	if err := logger.Put("AAA").Put("BBB").Put("CCC").Put("DDD").Put("EEE").Write().Err; err != nil {
		panic(err)
	}

	for {
		logger.Put("AAABBBCCCDDDEEE")

		if logger.IsMaxPut() {
			if err := logger.Write().Err; err != nil {
				panic(err)
			}
			break
		}
	}
}
