// Package objstore is the S3-compatible object-store boundary for raw
// evidence bytes. Raw content never transits the database or this service:
// the API issues presigned PUT URLs (server-side SHA-256 enforced at the
// object store) on package creation and presigned GET URLs only after the
// metadata record exists and authorization has passed. Credentials come from
// the environment only and are never persisted; the domain model continues
// to reject credential-bearing content locations.
package objstore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Config is the resolved object-store configuration. It fails closed on any
// missing or malformed value.
type Config struct {
	Endpoint   string // empty for AWS; otherwise the S3-compatible endpoint
	Region     string
	Bucket     string
	AccessKey  string
	SecretKey  string
	PathStyle  bool
	PresignTTL time.Duration
}

// Validate fails closed on an incomplete configuration.
func (c Config) Validate() error {
	if strings.TrimSpace(c.Region) == "" {
		return errors.New("object-store region is required")
	}
	if strings.TrimSpace(c.Bucket) == "" || strings.ContainsAny(c.Bucket, "/:@") {
		return errors.New("object-store bucket is required and must be a bare bucket name")
	}
	if strings.TrimSpace(c.AccessKey) == "" || strings.TrimSpace(c.SecretKey) == "" {
		return errors.New("object-store credentials must be injected from the environment")
	}
	if c.Endpoint != "" {
		parsed, err := url.Parse(c.Endpoint)
		if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" {
			return errors.New("object-store endpoint must be an absolute http(s) URL")
		}
		if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return errors.New("object-store endpoint must not contain credentials, query parameters or fragments")
		}
	}
	if c.PresignTTL <= 0 || c.PresignTTL > time.Hour {
		return errors.New("object-store presign TTL must be positive and at most one hour")
	}
	return nil
}

// UploadDescriptor authorizes a direct-to-store upload of the raw evidence.
type UploadDescriptor struct {
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers"`
	ExpiresAt time.Time         `json:"expires_at"`
}

// DownloadDescriptor authorizes a direct-from-store download.
type DownloadDescriptor struct {
	Method    string    `json:"method"`
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Store is the object-store client boundary used by the API.
type Store interface {
	// PresignedUpload issues a PUT URL for key whose payload the object
	// store verifies server-side against expectedSHA256Hex.
	PresignedUpload(ctx context.Context, key, expectedSHA256Hex string) (UploadDescriptor, error)
	// PresignedDownload issues a GET URL for key.
	PresignedDownload(ctx context.Context, key string) (DownloadDescriptor, error)
	// VerifyDigest confirms via a HEAD (checksum mode enabled) that the
	// retained object matches the recorded digest. A missing object or a
	// digest mismatch is an error — the hook never silently passes.
	VerifyDigest(ctx context.Context, key, expectedSHA256Hex string) error
}

// Client is the production S3-compatible Store.
type Client struct {
	config  Config
	presign *s3.PresignClient
	heads   *s3.Client
}

// NewClient builds the client, failing closed on an invalid configuration.
func NewClient(config Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	awsConfig := aws.Config{
		Region:      config.Region,
		Credentials: credentials.NewStaticCredentialsProvider(config.AccessKey, config.SecretKey, ""),
	}
	options := func(options *s3.Options) {
		options.UsePathStyle = config.PathStyle
		if config.Endpoint != "" {
			options.BaseEndpoint = aws.String(config.Endpoint)
		}
	}
	s3Client := s3.NewFromConfig(awsConfig, options)
	return &Client{
		config:  config,
		presign: s3.NewPresignClient(s3Client),
		heads:   s3Client,
	}, nil
}

// ObjectLocation renders the credential-free s3:// location recorded in the
// evidence_packages row for key.
func (c *Client) ObjectLocation(key string) string {
	return "s3://" + c.config.Bucket + "/" + key
}

// ParseLocation resolves a recorded credential-free s3:// location back to
// its object key, failing closed on a foreign bucket or malformed value.
func (c *Client) ParseLocation(location string) (string, error) {
	parsed, err := url.Parse(location)
	if err != nil || parsed.Scheme != "s3" || parsed.Host != c.config.Bucket {
		return "", errors.New("content location is not in the approved evidence bucket")
	}
	key := strings.TrimPrefix(parsed.Path, "/")
	if key == "" || strings.Contains(key, "..") {
		return "", errors.New("content location object key is invalid")
	}
	return key, nil
}

// PresignedUpload implements Store. The expected digest is bound into the
// signed request as x-amz-checksum-sha256 so the object store rejects a
// mismatched payload server-side.
func (c *Client) PresignedUpload(ctx context.Context, key, expectedSHA256Hex string) (UploadDescriptor, error) {
	checksum, err := checksumBase64(expectedSHA256Hex)
	if err != nil {
		return UploadDescriptor{}, err
	}
	request, err := c.presign.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:         aws.String(c.config.Bucket),
		Key:            aws.String(key),
		ChecksumSHA256: aws.String(checksum),
	}, s3.WithPresignExpires(c.config.PresignTTL))
	if err != nil {
		return UploadDescriptor{}, fmt.Errorf("presign evidence upload: %w", err)
	}
	return UploadDescriptor{
		Method: "PUT",
		URL:    request.URL,
		Headers: map[string]string{
			"x-amz-checksum-sha256": checksum,
		},
		ExpiresAt: time.Now().Add(c.config.PresignTTL).UTC(),
	}, nil
}

// PresignedDownload implements Store.
func (c *Client) PresignedDownload(ctx context.Context, key string) (DownloadDescriptor, error) {
	request, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.config.Bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(c.config.PresignTTL))
	if err != nil {
		return DownloadDescriptor{}, fmt.Errorf("presign evidence download: %w", err)
	}
	return DownloadDescriptor{
		Method:    "GET",
		URL:       request.URL,
		ExpiresAt: time.Now().Add(c.config.PresignTTL).UTC(),
	}, nil
}

// VerifyDigest implements Store.
func (c *Client) VerifyDigest(ctx context.Context, key, expectedSHA256Hex string) error {
	if _, err := checksumBase64(expectedSHA256Hex); err != nil {
		return err
	}
	output, err := c.heads.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(c.config.Bucket),
		Key:          aws.String(key),
		ChecksumMode: s3types.ChecksumModeEnabled,
	})
	if err != nil {
		return fmt.Errorf("head evidence object: %w", err)
	}
	if output.ChecksumSHA256 == nil {
		return errors.New("evidence object carries no server-side SHA-256 checksum")
	}
	decoded, err := base64.StdEncoding.DecodeString(*output.ChecksumSHA256)
	if err != nil || hex.EncodeToString(decoded) != expectedSHA256Hex {
		return errors.New("evidence object digest does not match the recorded digest")
	}
	return nil
}

// checksumBase64 converts the model's lower-case hex digest to the base64
// form the S3 checksum headers use.
func checksumBase64(expectedSHA256Hex string) (string, error) {
	raw, err := hex.DecodeString(expectedSHA256Hex)
	if err != nil || len(raw) != sha256.Size {
		return "", errors.New("expected digest must be a lower-case SHA-256 hexadecimal digest")
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}
