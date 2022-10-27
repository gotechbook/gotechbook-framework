package groups

import (
	config "github.com/gotechbook/gotechbook-framework-config"
	"testing"

	"go.etcd.io/etcd/tests/v3/framework/integration"
)

func setup(t *testing.T) (*integration.Cluster, GroupService) {
	integration.BeforeTest(t)
	cluster := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	cli := cluster.RandClient()
	etcdGroupService, err := NewEtcdGroupService(*config.DefaultGroups(), cli)
	if err != nil {
		panic(err)
	}
	return cluster, etcdGroupService
}

func TestEtcdCreateDuplicatedGroup(t *testing.T) {
	cluster, etcdGroupService := setup(t)
	defer cluster.Terminate(t)
	testCreateDuplicatedGroup(etcdGroupService, t)
}

func TestEtcdCreateGroup(t *testing.T) {
	cluster, etcdGroupService := setup(t)
	defer cluster.Terminate(t)
	testCreateGroup(etcdGroupService, t)
}

func TestEtcdCreateGroupWithTTL(t *testing.T) {
	cluster, etcdGroupService := setup(t)
	defer cluster.Terminate(t)
	testCreateGroupWithTTL(etcdGroupService, t)
}

func TestEtcdGroupAddMember(t *testing.T) {
	cluster, etcdGroupService := setup(t)
	defer cluster.Terminate(t)
	testGroupAddMember(etcdGroupService, t)
}

func TestEtcdGroupAddDuplicatedMember(t *testing.T) {
	cluster, etcdGroupService := setup(t)
	defer cluster.Terminate(t)
	testGroupAddDuplicatedMember(etcdGroupService, t)
}

func TestEtcdGroupContainsMember(t *testing.T) {
	cluster, etcdGroupService := setup(t)
	defer cluster.Terminate(t)
	testGroupContainsMember(etcdGroupService, t)
}

func TestEtcdRemove(t *testing.T) {
	cluster, etcdGroupService := setup(t)
	defer cluster.Terminate(t)
	testRemove(etcdGroupService, t)
}

func TestEtcdDelete(t *testing.T) {
	cluster, etcdGroupService := setup(t)
	defer cluster.Terminate(t)
	testDelete(etcdGroupService, t)
}

func TestEtcdRemoveAll(t *testing.T) {
	cluster, etcdGroupService := setup(t)
	defer cluster.Terminate(t)
	testRemoveAll(etcdGroupService, t)
}

func TestEtcdCount(t *testing.T) {
	cluster, etcdGroupService := setup(t)
	defer cluster.Terminate(t)
	testCount(etcdGroupService, t)
}

func TestEtcdMembers(t *testing.T) {
	cluster, etcdGroupService := setup(t)
	defer cluster.Terminate(t)
	testMembers(etcdGroupService, t)
}
