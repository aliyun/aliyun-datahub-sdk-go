package account

/*
account 包提供了不同账号类型的实现。
目前仅支持一种账号类型：Aliyun账号。
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

// NewAliyunAccount 新建AliyunAccount实例
func NewAliyunAccount(id string, key string) *AliyunAccount {
	return &AliyunAccount{
		AccessId:  id,
		AccessKey: key,
	}
}

// String 方法支持fmt.Print类方法
func (a AliyunAccount) String() string {
	return fmt.Sprintf("access_id:%s, access_key:%s", a.AccessId, a.AccessKey)
}

func (a AliyunAccount) GetAccountId() string {
	return a.AccessId
}

func (a AliyunAccount) GetAccountKey() string {
	return a.AccessKey
}
