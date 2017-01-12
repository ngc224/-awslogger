package awslogger

import (
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

type AwsLogger struct {
	client               *cloudwatchlogs.CloudWatchLogs
	logEvents            []*cloudwatchlogs.InputLogEvent
	logGroupName         *string
	logStreamName        *string
	sequenceToken        *string
	messageByte          int
	eventsNumber         int
	WriteMessageByteSize int
	WriteEventsNumber    int
	Err                  error
}

var (
	maximumMessageByteSize = 1048576 - 26
	maximumEventsNumber    = 10000
)

func New(logGroupName, logStreamName string, conf *aws.Config) (*AwsLogger, error) {
	logger := &AwsLogger{
		logGroupName:         &logGroupName,
		logStreamName:        &logStreamName,
		WriteMessageByteSize: maximumMessageByteSize,
		WriteEventsNumber:    maximumEventsNumber,
	}

	sess, err := session.NewSession()

	if err != nil {
		return nil, err
	}

	logger.client = cloudwatchlogs.New(sess, conf)
	groups, err := logger.client.DescribeLogGroups(&cloudwatchlogs.DescribeLogGroupsInput{})

	if err != nil {
		return nil, err
	}

	var isGroupExist bool

	for _, v := range groups.LogGroups {
		if *v.LogGroupName == *logger.logGroupName {
			isGroupExist = true
			break
		}
	}

	if !isGroupExist {
		_, err := logger.client.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
			LogGroupName: logger.logGroupName,
		})

		if err != nil {
			return nil, err
		}
	}

	streams, err := logger.client.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName: logger.logGroupName,
	})

	if err != nil {
		return nil, err
	}

	var isStreamExist bool

	for _, v := range streams.LogStreams {
		if *v.LogStreamName == *logger.logStreamName {
			isStreamExist = true
			logger.sequenceToken = v.UploadSequenceToken
			break
		}
	}

	if !isStreamExist {
		_, err := logger.client.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
			LogGroupName:  aws.String(logGroupName),
			LogStreamName: aws.String(logStreamName),
		})

		if err != nil {
			return nil, err
		}
	}

	return logger, nil
}

func (logger *AwsLogger) Put(message string) *AwsLogger {
	if logger.eventsNumber == maximumEventsNumber {
		logger.Err = errors.New("The maximum number of events")
		return logger
	}

	if (logger.messageByte + len(message)) > maximumMessageByteSize {
		logger.Err = errors.New("The maximum batch size")
		return logger
	}

	logger.messageByte += len(message)
	logger.eventsNumber++

	logger.logEvents = append(logger.logEvents, &cloudwatchlogs.InputLogEvent{
		Message:   aws.String(message),
		Timestamp: aws.Int64(time.Now().UnixNano() / int64(time.Millisecond)),
	})

	return logger
}

func (logger *AwsLogger) IsMaxPut() bool {
	if logger.eventsNumber >= logger.WriteEventsNumber {
		return true
	}

	if logger.messageByte >= logger.WriteMessageByteSize {
		return true
	}

	return false
}

func (logger *AwsLogger) Write() *AwsLogger {
	events, err := logger.client.PutLogEvents(
		&cloudwatchlogs.PutLogEventsInput{
			LogEvents:     logger.logEvents,
			LogGroupName:  logger.logGroupName,
			LogStreamName: logger.logStreamName,
			SequenceToken: logger.sequenceToken,
		},
	)

	if err != nil {
		logger.Err = err
		return logger
	}

	logger.sequenceToken = events.NextSequenceToken
	logger.logEvents = []*cloudwatchlogs.InputLogEvent{}
	logger.messageByte = 0
	logger.eventsNumber = 0
	logger.Err = nil

	return logger
}
