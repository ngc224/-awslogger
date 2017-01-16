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

	logger.Put("AAA").Put("BBB").Put("CCC").Put("DDD").Put("EEE").Write()

	for {
		logger.Put("AAABBBCCCDDDEEE")

		if logger.IsLimit() {
			if err := logger.Write(); err != nil {
				logger.WriteFile("buffer.json")
			}
			break
		}
	}
}
