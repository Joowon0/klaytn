package database

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klaytn/klaytn/common"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/suite"
)

type SuiteS3FileDB struct {
	suite.Suite
	s3DB           *s3FileDB
	testBucketName *string
}

func (s *SuiteS3FileDB) SetupSuite() {
	region := "ap-northeast-2"
	endpoint := "https://s3.ap-northeast-2.amazonaws.com"
	testBucketName := aws.String("winnie-test-bucket-" + strconv.Itoa(time.Now().Nanosecond()))

	s3DB, err := newS3FileDB(region, endpoint, *testBucketName)
	if err != nil {
		s.Fail("failed to create s3Database", "err", err)
	}

	_, err = s3DB.s3.DeleteBucketPolicy(&s3.DeleteBucketPolicyInput{Bucket: testBucketName})
	if err != nil {
		s.Fail("failed to delete the bucket policy for the test", "err", err, "bucketName", *testBucketName)
	}

	if err != nil {
		s.Fail("failed to create the test bucket", "err", err)
	}

	s.s3DB = s3DB
	s.testBucketName = testBucketName
}

func (s *SuiteS3FileDB) TearDownSuite() {
	if _, err := s.s3DB.s3.DeleteBucket(&s3.DeleteBucketInput{Bucket: s.testBucketName}); err != nil {
		s.Fail("failed to delete the test bucket", "err", err, "bucketName", *s.testBucketName)
	}
}

func TestSuiteS3FileDB(t *testing.T) {
	suite.Run(t, new(SuiteS3FileDB))
}

func TestRand(t *testing.T) {
	cmm := common.MakeRandomBytes(10)
	n := common.MakeRandomBytes(10)

	fmt.Println(string(cmm))
	fmt.Println(string(n))
}

func (s *SuiteS3FileDB) TestS3FileDB() {
	testKey := common.MakeRandomBytes(100)
	testVal := common.MakeRandomBytes(1024 * 1024)

	_, err := s.s3DB.read(testKey)

	if err == nil || !strings.Contains(err.Error(), s3.ErrCodeNoSuchKey) {
		s.Fail("Failed to read", "err", err, "bucketName", *s.testBucketName)
	}

	var uris uri
	uris, err = s.s3DB.write(item{key: testKey, val: testVal})
	if err != nil {
		s.Fail("Failed to write", "err", err, "bucketName", *s.testBucketName)
	}

	if uris == "" {
		s.Fail("Unexpected amount of uris are returned", "len(uris)", len(uris))
	}

	var val []byte
	val, err = s.s3DB.read(testKey)
	s.True(bytes.Equal(testVal, val))

	err = s.s3DB.delete(testKey)
	if err != nil {
		s.Fail("Failed to delete", "err", err, "bucketName", *s.testBucketName)
	}
}

func (s *SuiteS3FileDB) TestS3FileDB_Overwrite() {
	testKey := common.MakeRandomBytes(256)
	var testVals [][]byte
	for i := 0; i < 10; i++ {
		testVals = append(testVals, common.MakeRandomBytes(1024*1024))
	}

	_, err := s.s3DB.read(testKey)
	if err == nil || !strings.Contains(err.Error(), s3.ErrCodeNoSuchKey) {
		s.Fail("Failed to read", "err", err, "bucketName", *s.testBucketName)
	}

	// This is to ensure deleting the bucket after the tests
	defer s.s3DB.delete(testKey)

	var uris []uri
	for _, testVal := range testVals {
		uri, err := s.s3DB.write(item{key: testKey, val: testVal})
		if err != nil {
			s.Fail("failed to write the data to s3FileDB", "err", err)
		}
		uris = append(uris, uri)
	}

	returnedVal, err := s.s3DB.read(testKey)
	if err != nil {
		s.Fail("failed to read the data from s3FileDB", "err", err)
	}

	s.Equal(testVals[len(testVals)-1], returnedVal)
	s.Equal(len(testVals), len(uris))
}

func (s *SuiteS3FileDB) TestS3FileDB_EmptyDelete() {
	testKey := common.MakeRandomBytes(256)
	s.NoError(s.s3DB.delete(testKey))
	s.NoError(s.s3DB.delete(testKey))
	s.NoError(s.s3DB.delete(testKey))
}
