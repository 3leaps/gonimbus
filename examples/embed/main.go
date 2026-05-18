package main

import (
	"context"

	"github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/uri"
)

func main() {
	ctx := context.Background()

	u, err := uri.ParseURI("s3://your-bucket-here/data/2026/file.xml")
	if err != nil {
		panic(err)
	}

	provider, err := s3.New(ctx, s3.Config{
		Bucket:          u.Bucket,
		Region:          "us-east-1",
		AccessKeyID:     "access-key-managed-by-your-app",
		SecretAccessKey: "secret-key-managed-by-your-app",
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = provider.Close()
	}()
}
