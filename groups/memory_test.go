package groups

import (
	config "github.com/gotechbook/gotechbook-framework-config"
	"os"
	"testing"
)

var memoryGroupService *MemoryGroupService

func TestMain(m *testing.M) {
	memoryGroupService = NewMemoryGroupService(*config.DefaultGroups())
	exit := m.Run()
	os.Exit(exit)
}

func TestMemoryCreateDuplicatedGroup(t *testing.T) {
	testCreateDuplicatedGroup(memoryGroupService, t)
}

func TestMemoryCreateGroup(t *testing.T) {
	testCreateGroup(memoryGroupService, t)
}

func TestMemoryCreateGroupWithTTL(t *testing.T) {
	testCreateGroupWithTTL(memoryGroupService, t)
}

func TestMemoryGroupAddMember(t *testing.T) {
	testGroupAddMember(memoryGroupService, t)
}

func TestMemoryGroupAddDuplicatedMember(t *testing.T) {
	testGroupAddDuplicatedMember(memoryGroupService, t)
}

func TestMemoryGroupContainsMember(t *testing.T) {
	testGroupContainsMember(memoryGroupService, t)
}

func TestMemoryRemove(t *testing.T) {
	testRemove(memoryGroupService, t)
}

func TestMemoryDelete(t *testing.T) {
	testDelete(memoryGroupService, t)
}

func TestMemoryRemoveAll(t *testing.T) {
	testRemoveAll(memoryGroupService, t)
}

func TestMemoryCount(t *testing.T) {
	testCount(memoryGroupService, t)
}

func TestMemoryMembers(t *testing.T) {
	testMembers(memoryGroupService, t)
}
