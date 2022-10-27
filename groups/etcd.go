package groups

import (
	"context"
	"errors"
	"fmt"
	config "github.com/gotechbook/gotechbook-framework-config"
	logger "github.com/gotechbook/gotechbook-framework-logger"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
)

var (
	ErrGroupNotFound       = errors.New("group not found")
	ErrGroupAlreadyExists  = errors.New("group already exists")
	ErrMemberAlreadyExists = errors.New("member already exists in group")
	ErrMemberNotFound      = errors.New("member not found in the group")
	ErrEtcdLeaseNotFound   = errors.New("etcd lease not found in group")
	ErrMemoryTTLNotFound   = errors.New("memory group TTL not found")
)
var (
	clientInstance     *v3.Client
	transactionTimeout time.Duration
	etcdOnce           sync.Once
)

// EtcdGroupService base ETCD struct solution
type EtcdGroupService struct {
}

// NewEtcdGroupService returns a new group instance
func NewEtcdGroupService(conf config.Groups, clientOrNil *v3.Client) (*EtcdGroupService, error) {
	err := initClientInstance(conf, clientOrNil)
	if err != nil {
		return nil, err
	}
	return &EtcdGroupService{}, err
}

func initClientInstance(config config.Groups, clientOrNil *v3.Client) error {
	var err error
	etcdOnce.Do(func() {
		if clientOrNil != nil {
			clientInstance = clientOrNil
		} else {
			clientInstance, err = createBaseClient(config)
		}
		if err != nil {
			logger.Log.Fatalf("error initializing singleton etcd client in groups: %s", err.Error())
			return
		}
		transactionTimeout = config.GoTechBookFrameworkGroupsEtcdTransactionTimeout
	})
	return err
}

func createBaseClient(config config.Groups) (*v3.Client, error) {
	cli, err := v3.New(v3.Config{
		Endpoints:   config.GoTechBookFrameworkGroupsEtcdEndpoints,
		DialTimeout: config.GoTechBookFrameworkGroupsEtcdDialTimeout,
	})
	if err != nil {
		return nil, err
	}
	cli.KV = namespace.NewKV(cli.KV, config.GoTechBookFrameworkGroupsEtcdPrefix)
	return cli, nil
}

func groupKey(groupName string) string {
	return fmt.Sprintf("groups/%s", groupName)
}

func memberKey(groupName, uid string) string {
	return fmt.Sprintf("%s/uids/%s", groupKey(groupName), uid)
}

func getGroupKV(ctx context.Context, groupName string) (*mvccpb.KeyValue, error) {
	ctxT, cancel := context.WithTimeout(ctx, transactionTimeout)
	defer cancel()
	etcdRes, err := clientInstance.Get(ctxT, groupKey(groupName))
	if err != nil {
		return nil, err
	}
	if etcdRes.Count == 0 {
		return nil, ErrGroupNotFound
	}
	return etcdRes.Kvs[0], nil
}

func (c *EtcdGroupService) createGroup(ctx context.Context, groupName string, leaseID v3.LeaseID) error {
	var etcdRes *v3.TxnResponse
	var err error

	ctxT, cancel := context.WithTimeout(ctx, transactionTimeout)
	defer cancel()
	if leaseID != 0 {
		etcdRes, err = clientInstance.Txn(ctxT).
			If(v3.Compare(v3.CreateRevision(groupKey(groupName)), "=", 0)).
			Then(v3.OpPut(groupKey(groupName), "", v3.WithLease(leaseID))).
			Commit()
	} else {
		etcdRes, err = clientInstance.Txn(ctxT).
			If(v3.Compare(v3.CreateRevision(groupKey(groupName)), "=", 0)).
			Then(v3.OpPut(groupKey(groupName), "")).
			Commit()
	}

	if err != nil {
		return err
	}
	if !etcdRes.Succeeded {
		return ErrGroupAlreadyExists
	}
	return nil
}

// GroupCreate creates a group struct inside ETCD, without TTL
func (c *EtcdGroupService) GroupCreate(ctx context.Context, groupName string) error {
	return c.createGroup(ctx, groupName, 0)
}

// GroupCreateWithTTL creates a group struct inside ETCD, with TTL, using leaseID
func (c *EtcdGroupService) GroupCreateWithTTL(ctx context.Context, groupName string, ttlTime time.Duration) error {
	ctxT, cancel := context.WithTimeout(ctx, transactionTimeout)
	defer cancel()
	lease, err := clientInstance.Grant(ctxT, int64(ttlTime.Seconds()))
	if err != nil {
		return err
	}
	return c.createGroup(ctx, groupName, lease.ID)
}

// GroupMembers returns all member's UIDs
func (c *EtcdGroupService) GroupMembers(ctx context.Context, groupName string) ([]string, error) {
	prefix := memberKey(groupName, "")
	ctxT, cancel := context.WithTimeout(ctx, transactionTimeout)
	defer cancel()
	etcdRes, err := clientInstance.Txn(ctxT).
		If(v3.Compare(v3.CreateRevision(groupKey(groupName)), ">", 0)).
		Then(v3.OpGet(prefix, v3.WithPrefix(), v3.WithKeysOnly())).
		Commit()

	if err != nil {
		return nil, err
	}
	if !etcdRes.Succeeded {
		return nil, ErrGroupNotFound
	}

	getRes := etcdRes.Responses[0].GetResponseRange()
	members := make([]string, getRes.GetCount())
	for i, kv := range getRes.GetKvs() {
		members[i] = string(kv.Key)[len(prefix):]
	}
	return members, nil
}

// GroupContainsMember checks whether a UID is contained in current group or not
func (c *EtcdGroupService) GroupContainsMember(ctx context.Context, groupName, uid string) (bool, error) {
	ctxT, cancel := context.WithTimeout(ctx, transactionTimeout)
	defer cancel()
	etcdRes, err := clientInstance.Txn(ctxT).
		If(v3.Compare(v3.CreateRevision(groupKey(groupName)), ">", 0)).
		Then(v3.OpGet(memberKey(groupName, uid), v3.WithCountOnly())).
		Commit()

	if err != nil {
		return false, err
	}
	if !etcdRes.Succeeded {
		return false, ErrGroupNotFound
	}
	return etcdRes.Responses[0].GetResponseRange().GetCount() > 0, nil
}

// GroupAddMember adds UID to group
func (c *EtcdGroupService) GroupAddMember(ctx context.Context, groupName, uid string) error {
	var etcdRes *v3.TxnResponse
	kv, err := getGroupKV(ctx, groupName)
	if err != nil {
		return err
	}

	ctxT, cancel := context.WithTimeout(ctx, transactionTimeout)
	defer cancel()
	if kv.Lease != 0 {
		etcdRes, err = clientInstance.Txn(ctxT).
			If(v3.Compare(v3.CreateRevision(groupKey(groupName)), ">", 0),
				v3.Compare(v3.CreateRevision(memberKey(groupName, uid)), "=", 0)).
			Then(v3.OpPut(memberKey(groupName, uid), "", v3.WithLease(v3.LeaseID(kv.Lease)))).
			Commit()
	} else {
		etcdRes, err = clientInstance.Txn(ctxT).
			If(v3.Compare(v3.CreateRevision(groupKey(groupName)), ">", 0),
				v3.Compare(v3.CreateRevision(memberKey(groupName, uid)), "=", 0)).
			Then(v3.OpPut(memberKey(groupName, uid), "")).
			Commit()
	}

	if err != nil {
		return err
	}
	if !etcdRes.Succeeded {
		return ErrMemberAlreadyExists
	}
	return nil
}

// GroupRemoveMember removes specified UID from group
func (c *EtcdGroupService) GroupRemoveMember(ctx context.Context, groupName, uid string) error {
	ctxT, cancel := context.WithTimeout(ctx, transactionTimeout)
	defer cancel()
	etcdRes, err := clientInstance.Txn(ctxT).
		If(v3.Compare(v3.CreateRevision(memberKey(groupName, uid)), ">", 0)).
		Then(v3.OpDelete(memberKey(groupName, uid))).
		Commit()

	if err != nil {
		return err
	}
	if !etcdRes.Succeeded {
		return ErrMemberNotFound
	}
	return nil
}

// GroupRemoveAll clears all UIDs in the group
func (c *EtcdGroupService) GroupRemoveAll(ctx context.Context, groupName string) error {
	ctxT, cancel := context.WithTimeout(ctx, transactionTimeout)
	defer cancel()
	etcdRes, err := clientInstance.Txn(ctxT).
		If(v3.Compare(v3.CreateRevision(groupKey(groupName)), ">", 0)).
		Then(v3.OpDelete(memberKey(groupName, ""), v3.WithPrefix())).
		Commit()

	if err != nil {
		return err
	}
	if !etcdRes.Succeeded {
		return ErrGroupNotFound
	}
	return nil
}

// GroupDelete deletes the whole group, including members and base group
func (c *EtcdGroupService) GroupDelete(ctx context.Context, groupName string) error {
	ctxT, cancel := context.WithTimeout(ctx, transactionTimeout)
	defer cancel()
	etcdRes, err := clientInstance.Txn(ctxT).
		If(v3.Compare(v3.CreateRevision(groupKey(groupName)), ">", 0)).
		Then(v3.OpDelete(memberKey(groupName, ""), v3.WithPrefix()),
			v3.OpDelete(groupKey(groupName))).
		Commit()

	if err != nil {
		return err
	}
	if !etcdRes.Succeeded {
		return ErrGroupNotFound
	}
	return nil
}

// GroupCountMembers get current member amount in group
func (c *EtcdGroupService) GroupCountMembers(ctx context.Context, groupName string) (int, error) {
	ctxT, cancel := context.WithTimeout(ctx, transactionTimeout)
	defer cancel()
	etcdRes, err := clientInstance.Get(ctxT, memberKey(groupName, ""), v3.WithPrefix(), v3.WithCountOnly())
	if err != nil {
		return 0, err
	}
	return int(etcdRes.Count), nil
}

// GroupRenewTTL will renew ETCD lease TTL
func (c *EtcdGroupService) GroupRenewTTL(ctx context.Context, groupName string) error {
	kv, err := getGroupKV(ctx, groupName)
	if err != nil {
		return err
	}
	if kv.Lease != 0 {
		ctxT, cancel := context.WithTimeout(ctx, transactionTimeout)
		defer cancel()
		_, err = clientInstance.KeepAliveOnce(ctxT, v3.LeaseID(kv.Lease))
		return err
	}
	return ErrEtcdLeaseNotFound
}
