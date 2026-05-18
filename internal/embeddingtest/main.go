package main

import (
	"context"
	"fmt"

	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/3leaps/gonimbus/pkg/provider/file"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/pkg/uri"
)

func main() {
	if err := constructLibrarySurface(); err != nil {
		panic(err)
	}
}

func constructLibrarySurface() error {
	parsed, err := uri.ParseURI("s3://your-bucket-here/data/2026/**/*.xml")
	if err != nil {
		return err
	}

	_ = match.NormalizePattern(parsed.Pattern)
	_ = provider.ListOptions{Prefix: parsed.Key, MaxKeys: 10}
	_ = s3.Config{Bucket: parsed.Bucket, Region: "us-east-1"}
	_ = file.Config{BaseDir: "."}
	_ = context.Background()

	if parsed.Bucket == "" {
		return fmt.Errorf("bucket was empty")
	}
	return nil
}
