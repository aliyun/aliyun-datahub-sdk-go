package datahub

type SubscriptionEntry struct {
    SubId          string            `json:"SubId"`
    TopicName      string            `json:"TopicName"`
    IsOwner        bool              `json:"IsOwner"`
    Type           SubscriptionType  `json:"Type"`
    State          SubscriptionState `json:"State,omitempty"`
    Comment        string            `json:"Comment,omitempty"`
    CreateTime     uint64            `json:"CreateTime"`
    LastModifyTime uint64            `json:"LastModifyTime"`
}

type SubscriptionOffset struct {
    Timestamp int64  `json:"Timestamp"`
    Sequence  int64  `json:"Sequence"`
    VersionId int64  `json:"Version"`
    SessionId *int64 `json:"SessionId"`
}
