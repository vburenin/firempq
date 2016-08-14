// Code generated by protoc-gen-go.
// source: google.golang.org/genproto/googleapis/bigtable/admin/v2/instance.proto
// DO NOT EDIT!

package google_bigtable_admin_v2

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import _ "google.golang.org/genproto/googleapis/api/serviceconfig"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// Possible states of an instance.
type Instance_State int32

const (
	// The state of the instance could not be determined.
	Instance_STATE_NOT_KNOWN Instance_State = 0
	// The instance has been successfully created and can serve requests
	// to its tables.
	Instance_READY Instance_State = 1
	// The instance is currently being created, and may be destroyed
	// if the creation process encounters an error.
	Instance_CREATING Instance_State = 2
)

var Instance_State_name = map[int32]string{
	0: "STATE_NOT_KNOWN",
	1: "READY",
	2: "CREATING",
}
var Instance_State_value = map[string]int32{
	"STATE_NOT_KNOWN": 0,
	"READY":           1,
	"CREATING":        2,
}

func (x Instance_State) String() string {
	return proto.EnumName(Instance_State_name, int32(x))
}
func (Instance_State) EnumDescriptor() ([]byte, []int) { return fileDescriptor3, []int{0, 0} }

// Possible states of a cluster.
type Cluster_State int32

const (
	// The state of the cluster could not be determined.
	Cluster_STATE_NOT_KNOWN Cluster_State = 0
	// The cluster has been successfully created and is ready to serve requests.
	Cluster_READY Cluster_State = 1
	// The cluster is currently being created, and may be destroyed
	// if the creation process encounters an error.
	// A cluster may not be able to serve requests while being created.
	Cluster_CREATING Cluster_State = 2
	// The cluster is currently being resized, and may revert to its previous
	// node count if the process encounters an error.
	// A cluster is still capable of serving requests while being resized,
	// but may exhibit performance as if its number of allocated nodes is
	// between the starting and requested states.
	Cluster_RESIZING Cluster_State = 3
	// The cluster has no backing nodes. The data (tables) still
	// exist, but no operations can be performed on the cluster.
	Cluster_DISABLED Cluster_State = 4
)

var Cluster_State_name = map[int32]string{
	0: "STATE_NOT_KNOWN",
	1: "READY",
	2: "CREATING",
	3: "RESIZING",
	4: "DISABLED",
}
var Cluster_State_value = map[string]int32{
	"STATE_NOT_KNOWN": 0,
	"READY":           1,
	"CREATING":        2,
	"RESIZING":        3,
	"DISABLED":        4,
}

func (x Cluster_State) String() string {
	return proto.EnumName(Cluster_State_name, int32(x))
}
func (Cluster_State) EnumDescriptor() ([]byte, []int) { return fileDescriptor3, []int{1, 0} }

// A collection of Bigtable [Tables][google.bigtable.admin.v2.Table] and
// the resources that serve them.
// All tables in an instance are served from a single
// [Cluster][google.bigtable.admin.v2.Cluster].
type Instance struct {
	// @OutputOnly
	// The unique name of the instance. Values are of the form
	// projects/<project>/instances/[a-z][a-z0-9\\-]+[a-z0-9]
	Name string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	// The descriptive name for this instance as it appears in UIs.
	// Can be changed at any time, but should be kept globally unique
	// to avoid confusion.
	DisplayName string `protobuf:"bytes,2,opt,name=display_name,json=displayName" json:"display_name,omitempty"`
	//
	// The current state of the instance.
	State Instance_State `protobuf:"varint,3,opt,name=state,enum=google.bigtable.admin.v2.Instance_State" json:"state,omitempty"`
}

func (m *Instance) Reset()                    { *m = Instance{} }
func (m *Instance) String() string            { return proto.CompactTextString(m) }
func (*Instance) ProtoMessage()               {}
func (*Instance) Descriptor() ([]byte, []int) { return fileDescriptor3, []int{0} }

// A resizable group of nodes in a particular cloud location, capable
// of serving all [Tables][google.bigtable.admin.v2.Table] in the parent
// [Instance][google.bigtable.admin.v2.Instance].
type Cluster struct {
	// @OutputOnly
	// The unique name of the cluster. Values are of the form
	// projects/<project>/instances/<instance>/clusters/[a-z][-a-z0-9]*
	Name string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	// @CreationOnly
	// The location where this cluster's nodes and storage reside. For best
	// performance, clients should be located as close as possible to this cluster.
	// Currently only zones are supported, e.g. projects/*/locations/us-central1-b
	Location string `protobuf:"bytes,2,opt,name=location" json:"location,omitempty"`
	// @OutputOnly
	// The current state of the cluster.
	State Cluster_State `protobuf:"varint,3,opt,name=state,enum=google.bigtable.admin.v2.Cluster_State" json:"state,omitempty"`
	// The number of nodes allocated to this cluster. More nodes enable higher
	// throughput and more consistent performance.
	ServeNodes int32 `protobuf:"varint,4,opt,name=serve_nodes,json=serveNodes" json:"serve_nodes,omitempty"`
	// @CreationOnly
	// The type of storage used by this cluster to serve its
	// parent instance's tables, unless explicitly overridden.
	DefaultStorageType StorageType `protobuf:"varint,5,opt,name=default_storage_type,json=defaultStorageType,enum=google.bigtable.admin.v2.StorageType" json:"default_storage_type,omitempty"`
}

func (m *Cluster) Reset()                    { *m = Cluster{} }
func (m *Cluster) String() string            { return proto.CompactTextString(m) }
func (*Cluster) ProtoMessage()               {}
func (*Cluster) Descriptor() ([]byte, []int) { return fileDescriptor3, []int{1} }

func init() {
	proto.RegisterType((*Instance)(nil), "google.bigtable.admin.v2.Instance")
	proto.RegisterType((*Cluster)(nil), "google.bigtable.admin.v2.Cluster")
	proto.RegisterEnum("google.bigtable.admin.v2.Instance_State", Instance_State_name, Instance_State_value)
	proto.RegisterEnum("google.bigtable.admin.v2.Cluster_State", Cluster_State_name, Cluster_State_value)
}

func init() {
	proto.RegisterFile("google.golang.org/genproto/googleapis/bigtable/admin/v2/instance.proto", fileDescriptor3)
}

var fileDescriptor3 = []byte{
	// 412 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x9c, 0x52, 0xd1, 0x8a, 0xd4, 0x30,
	0x14, 0xb5, 0xb3, 0x53, 0x9d, 0xcd, 0xae, 0x5a, 0xa2, 0x0f, 0x65, 0x10, 0xd4, 0x82, 0xb8, 0x4f,
	0x09, 0x8c, 0xf8, 0xa8, 0x30, 0xb3, 0xad, 0x52, 0x94, 0xee, 0xda, 0x16, 0x16, 0x7d, 0x29, 0x99,
	0x36, 0x1b, 0x02, 0x6d, 0x52, 0x9a, 0xec, 0xc0, 0xfe, 0x99, 0x3f, 0xe0, 0x7f, 0xd9, 0xa4, 0x1d,
	0x71, 0xc0, 0x82, 0xcc, 0x4b, 0xc9, 0xb9, 0xf7, 0xf4, 0x9c, 0x93, 0xdc, 0x0b, 0x3e, 0x31, 0x29,
	0x59, 0x4d, 0x11, 0x93, 0x35, 0x11, 0x0c, 0xc9, 0x8e, 0x61, 0x46, 0x45, 0xdb, 0x49, 0x2d, 0xf1,
	0xd0, 0x22, 0x2d, 0x57, 0x78, 0xcb, 0x99, 0x26, 0xdb, 0x9a, 0x62, 0x52, 0x35, 0x5c, 0xe0, 0xdd,
	0x0a, 0x73, 0xa1, 0x34, 0x11, 0x25, 0x45, 0x96, 0x0b, 0xfd, 0x51, 0x67, 0x4f, 0x44, 0x96, 0x88,
	0x76, 0xab, 0x65, 0xfc, 0x7f, 0x0e, 0xfd, 0x07, 0x2b, 0xda, 0xed, 0x78, 0x49, 0x4b, 0x29, 0x6e,
	0x39, 0xc3, 0x44, 0x08, 0xa9, 0x89, 0xe6, 0x52, 0xa8, 0xc1, 0x64, 0x19, 0x1e, 0x1b, 0xb6, 0x94,
	0x4d, 0x23, 0xc5, 0xa0, 0x12, 0xfc, 0x74, 0xc0, 0x22, 0x1e, 0xd3, 0x43, 0x08, 0xe6, 0x82, 0x34,
	0xd4, 0x77, 0x5e, 0x39, 0x17, 0xa7, 0xa9, 0x3d, 0xc3, 0xd7, 0xe0, 0xbc, 0xe2, 0xaa, 0xad, 0xc9,
	0x7d, 0x61, 0x7b, 0x33, 0xdb, 0x3b, 0x1b, 0x6b, 0x89, 0xa1, 0x7c, 0x04, 0x6e, 0x2f, 0xa0, 0xa9,
	0x7f, 0xd2, 0xf7, 0x9e, 0xac, 0x2e, 0xd0, 0xd4, 0xf5, 0xd1, 0xde, 0x09, 0x65, 0x86, 0x9f, 0x0e,
	0xbf, 0x05, 0xef, 0x81, 0x6b, 0x31, 0x7c, 0x06, 0x9e, 0x66, 0xf9, 0x3a, 0x8f, 0x8a, 0xe4, 0x2a,
	0x2f, 0xbe, 0x24, 0x57, 0x37, 0x89, 0xf7, 0x00, 0x9e, 0x02, 0x37, 0x8d, 0xd6, 0xe1, 0x77, 0xcf,
	0x81, 0xe7, 0x60, 0x71, 0xd9, 0x9f, 0xf3, 0x38, 0xf9, 0xec, 0xcd, 0x82, 0x5f, 0x33, 0xf0, 0xe8,
	0xb2, 0xbe, 0x53, 0x9a, 0x76, 0xff, 0x4c, 0xbe, 0x04, 0x8b, 0x5a, 0x96, 0xf6, 0xcd, 0xc6, 0xd4,
	0x7f, 0x30, 0xfc, 0x70, 0x18, 0xf9, 0xed, 0x74, 0xe4, 0xd1, 0xe1, 0x20, 0x31, 0x7c, 0x09, 0xce,
	0xcc, 0x78, 0x68, 0x21, 0x64, 0x45, 0x95, 0x3f, 0xef, 0x45, 0xdc, 0x14, 0xd8, 0x52, 0x62, 0x2a,
	0xf0, 0x06, 0x3c, 0xaf, 0xe8, 0x2d, 0xb9, 0xab, 0x75, 0xa1, 0xb4, 0xec, 0x08, 0xa3, 0x85, 0xbe,
	0x6f, 0xa9, 0xef, 0x5a, 0xbb, 0x37, 0xd3, 0x76, 0xd9, 0xc0, 0xce, 0x7b, 0x72, 0x0a, 0x47, 0x89,
	0xbf, 0x6a, 0xc1, 0xb7, 0xa3, 0xde, 0xca, 0xa0, 0x34, 0xca, 0xe2, 0x1f, 0x06, 0x9d, 0x18, 0x14,
	0xc6, 0xd9, 0x7a, 0xf3, 0x35, 0x0a, 0xbd, 0xf9, 0x06, 0x83, 0x17, 0xfd, 0x4a, 0x4c, 0x46, 0xda,
	0x3c, 0xde, 0x4f, 0xed, 0xda, 0x6c, 0xcc, 0xb5, 0xb3, 0x7d, 0x68, 0x57, 0xe7, 0xdd, 0xef, 0x00,
	0x00, 0x00, 0xff, 0xff, 0x78, 0x6c, 0xbc, 0xfe, 0x2f, 0x03, 0x00, 0x00,
}