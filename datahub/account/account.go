package account

/*
only support aliyun account now
*/

import (
	"fmt"
)

type Account interface {
	fmt.Stringer
	GetAccountId() string
	GetAccountKey() string
}

type AliyunAccount struct {
	// Aliyun Access key ID
	AccessId string

	// Aliyun Secret Access Key
	AccessKey string
}

// create new instance
func NewAliyunAccount(id string, key string) *AliyunAccount {
	return &AliyunAccount{
		AccessId:  id,
		AccessKey: key,
	}
}

func (a AliyunAccount) String() string {
	return fmt.Sprintf("access_id:%s, access_key:%s", a.AccessId, a.AccessKey)
}

func (a AliyunAccount) GetAccountId() string {
	return a.AccessId
}

func (a AliyunAccount) GetAccountKey() string {
	return a.AccessKey
}
