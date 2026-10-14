package backend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	iradix "github.com/hashicorp/go-immutable-radix"
)

// RadixS3Client is an in-memory S3 client mock powered by an immutable radix tree.
// It implements S3ClientAPI (ListObjectsV2APIClient and DownloadAPIClient) and manager.UploadAPIClient,
// providing prefix-matched object retrieval that mirrors AWS S3 semantics.
type RadixS3Client struct {
	mu   sync.RWMutex
	tree *iradix.Tree
}

// NewRadixS3Client creates a new empty in-memory S3 client backed by an immutable radix tree.
func NewRadixS3Client() *RadixS3Client {
	return &RadixS3Client{
		tree: iradix.New(),
	}
}

// Set inserts or updates an object payload at the specified key.
func (c *RadixS3Client) Set(key string, data []byte) *RadixS3Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tree, _, _ = c.tree.Insert([]byte(key), data)
	return c
}

// Get retrieves an object payload by key.
func (c *RadixS3Client) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.tree.Get([]byte(key))
	if !ok {
		return nil, false
	}
	return val.([]byte), true
}

// Delete removes an object by key.
func (c *RadixS3Client) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	newTree, _, ok := c.tree.Delete([]byte(key))
	if ok {
		c.tree = newTree
	}
	return ok
}

// Len returns the total number of objects in the radix tree.
func (c *RadixS3Client) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tree.Len()
}

// Keys returns all object keys stored in the radix tree in lexicographical order.
func (c *RadixS3Client) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var keys []string
	c.tree.Root().Walk(func(k []byte, v interface{}) bool {
		keys = append(keys, string(k))
		return false
	})
	sort.Strings(keys)
	return keys
}

// ListObjectsV2 implements s3.ListObjectsV2APIClient by walking the radix tree at the specified prefix.
func (c *RadixS3Client) ListObjectsV2(
	ctx context.Context,
	params *s3.ListObjectsV2Input,
	optFns ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	prefix := ""
	if params.Prefix != nil {
		prefix = *params.Prefix
	}

	var contents []types.Object
	now := time.Now()

	c.tree.Root().WalkPrefix([]byte(prefix), func(k []byte, v interface{}) bool {
		keyStr := string(k)
		data := v.([]byte)
		sz := int64(len(data))
		contents = append(contents, types.Object{
			Key:          aws.String(keyStr),
			Size:         aws.Int64(sz),
			LastModified: &now,
		})
		return false
	})

	// S3 returns keys in UTF-8 binary order
	sort.Slice(contents, func(i, j int) bool {
		return *contents[i].Key < *contents[j].Key
	})

	maxKeys := int32(1000)
	if params.MaxKeys != nil && *params.MaxKeys > 0 {
		maxKeys = *params.MaxKeys
	}

	startIndex := 0
	if params.ContinuationToken != nil && *params.ContinuationToken != "" {
		for i, obj := range contents {
			if *obj.Key == *params.ContinuationToken {
				startIndex = i + 1
				break
			}
		}
	}

	var paged []types.Object
	isTruncated := false
	var nextContinuationToken *string

	if startIndex < len(contents) {
		endIndex := startIndex + int(maxKeys)
		if endIndex < len(contents) {
			paged = contents[startIndex:endIndex]
			isTruncated = true
			nextContinuationToken = paged[len(paged)-1].Key
		} else {
			paged = contents[startIndex:]
		}
	}

	keyCount := int32(len(paged))
	return &s3.ListObjectsV2Output{
		Name:                  params.Bucket,
		Prefix:                params.Prefix,
		KeyCount:              &keyCount,
		MaxKeys:               &maxKeys,
		IsTruncated:           &isTruncated,
		NextContinuationToken: nextContinuationToken,
		Contents:              paged,
	}, nil
}

// GetObject implements manager.DownloadAPIClient by fetching the object payload from the radix tree.
func (c *RadixS3Client) GetObject(
	ctx context.Context,
	params *s3.GetObjectInput,
	optFns ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := ""
	if params.Key != nil {
		key = *params.Key
	}

	val, ok := c.tree.Get([]byte(key))
	if !ok {
		return nil, &types.NoSuchKey{
			Message: aws.String(fmt.Sprintf("The specified key %s does not exist.", key)),
		}
	}

	data := val.([]byte)
	now := time.Now()
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: aws.Int64(int64(len(data))),
		LastModified:  &now,
	}, nil
}

// PutObject implements manager.UploadAPIClient by storing the object payload into the radix tree.
func (c *RadixS3Client) PutObject(
	ctx context.Context,
	params *s3.PutObjectInput,
	optFns ...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	key := ""
	if params.Key != nil {
		key = *params.Key
	}

	var data []byte
	if params.Body != nil {
		buf, err := io.ReadAll(params.Body)
		if err != nil {
			return nil, err
		}
		data = buf
	}

	c.Set(key, data)
	return &s3.PutObjectOutput{}, nil
}
