//go:build cloudintegration

package cmd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/stream"
	"github.com/3leaps/gonimbus/test/cloudtest"
)

func TestStreamGetCommand_CloudIntegration(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()
	binary := findBinary(t)

	bucket := cloudtest.CreateBucket(t, ctx)
	key := "data.bin"

	// Make it big enough to ensure multiple chunks.
	content := bytes.Repeat([]byte("abcd"), 100_000) // 400KB
	cloudtest.PutObject(t, ctx, bucket, key, content)

	cmd := exec.Command(binary, "stream", "get",
		"s3://"+bucket+"/"+key,
		"--endpoint", cloudtest.Endpoint,
		"--chunk-bytes", "65536",
	)
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+cloudtest.TestAccessKeyID,
		"AWS_SECRET_ACCESS_KEY="+cloudtest.TestSecretAccessKey,
		"AWS_REGION="+cloudtest.Region,
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	require.NoError(t, err, "stderr: %s", stderr.String())

	dec := stream.NewDecoder(bytes.NewReader(stdout.Bytes()))

	// open
	ev, err := dec.Next()
	require.NoError(t, err)
	require.Equal(t, stream.EventRecord, ev.Kind)
	require.Equal(t, stream.TypeStreamOpen, ev.Record.Type)

	var open stream.Open
	require.NoError(t, json.Unmarshal(ev.Record.Data, &open))
	require.Equal(t, "s3://"+bucket+"/"+key, open.URI)
	require.NotEmpty(t, open.StreamID)
	require.NotNil(t, open.Size)
	require.Equal(t, int64(len(content)), *open.Size)

	// chunk(s)
	var got bytes.Buffer
	var seq int64
	for {
		ev, err = dec.Next()
		require.NoError(t, err)

		if ev.Kind == stream.EventRecord {
			// close
			require.Equal(t, stream.TypeStreamClose, ev.Record.Type)
			var closeRec stream.Close
			require.NoError(t, json.Unmarshal(ev.Record.Data, &closeRec))
			require.Equal(t, open.StreamID, closeRec.StreamID)
			require.Equal(t, "success", closeRec.Status)
			require.Equal(t, int64(got.Len()), closeRec.Bytes)
			break
		}

		require.Equal(t, stream.EventChunk, ev.Kind)
		require.NotNil(t, ev.Chunk)
		require.Equal(t, open.StreamID, ev.Chunk.Header.StreamID)
		require.Equal(t, seq, ev.Chunk.Header.Seq)
		require.GreaterOrEqual(t, ev.Chunk.Header.NBytes, int64(0))
		require.LessOrEqual(t, ev.Chunk.Header.NBytes, int64(65536))

		b, err := io.ReadAll(ev.Chunk.Body)
		require.NoError(t, err)
		require.NoError(t, ev.Chunk.Body.Close())
		require.Equal(t, int64(len(b)), ev.Chunk.Header.NBytes)
		_, _ = got.Write(b)

		seq++
	}

	require.Equal(t, content, got.Bytes())

	// No trailing records.
	ev, err = dec.Next()
	require.ErrorIs(t, err, io.EOF)

}
