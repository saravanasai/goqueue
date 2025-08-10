package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/saravanasai/goqueue/adapter/dlq"
	"github.com/saravanasai/goqueue/internal/logger"
	jobConfig "github.com/saravanasai/goqueue/config"
)

// NewSQSConfigWithDLQ creates a new Config instance with AWS SQS driver and DLQ support.
// This is suitable for production environments with AWS SQS.
func NewSQSConfigWithDLQ(queueURL, dlqURL, region, accessKeyID, secretAccessKey string, logger logger.Logger) jobConfig.Config {
	// Create AWS config
	var awsConfig aws.Config
	var err error

	// If credentials are provided, use them; otherwise, fall back to AWS SDK's default credential chain
	if accessKeyID != "" && secretAccessKey != "" {
		awsConfig, err = config.LoadDefaultConfig(context.Background(),
			config.WithRegion(region),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				accessKeyID, secretAccessKey, "",
			)),
		)
	} else {
		awsConfig, err = config.LoadDefaultConfig(context.Background(),
			config.WithRegion(region),
		)
	}

	if err != nil {
		logger.Error("Failed to initialize AWS config", "error", err)
		// Fall back to config without DLQ
		return jobConfig.NewSQSConfig(queueURL, region, accessKeyID, secretAccessKey)
	}

	// Create SQS client
	sqsClient := sqs.NewFromConfig(awsConfig)

	// Create SQS DLQ adapter
	sqsDLQ := dlq.NewSQSDLQ(sqsClient, dlqURL, logger)

	// Create basic SQS config
	cfg := jobConfig.NewSQSConfig(queueURL, region, accessKeyID, secretAccessKey)
	
	// Add DLQ adapter
	return cfg.WithDLQAdapter(sqsDLQ)
}
