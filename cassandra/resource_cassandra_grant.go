package cassandra

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"text/template"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

const (
	identifierPrivilege    = "privilege"
	identifierGrantee      = "grantee"
	identifierResourceType = "resource_type"
	identifierKeyspaceName = "keyspace_name"
	identifierTableName    = "table_name"
)

// Templates for CQL statements
var (
	createGrantTpl = template.Must(template.New("create_grant").Parse(
		`GRANT {{.Privilege | upper}} ON {{.ResourceType | upper}} {{if .KeyspaceName}}"{{.KeyspaceName}}"{{if .TableName}}.{{.TableName}}{{end}}"{{else}}{{.ResourceType | upper}}{{end}} TO "{{.Grantee}}"`,
	))
	deleteGrantTpl = template.Must(template.New("delete_grant").Parse(
		`REVOKE {{.Privilege | upper}} ON {{.ResourceType | upper}} {{if .KeyspaceName}}"{{.KeyspaceName}}"{{if .TableName}}.{{.TableName}}{{end}}"{{else}}{{.ResourceType | upper}}{{end}} FROM "{{.Grantee}}"`,
	))
	readGrantTpl = template.Must(template.New("read_grant").Parse(
		`LIST {{.Privilege | upper}} ON {{.ResourceType | upper}} {{if .KeyspaceName}}"{{.KeyspaceName}}"{{if .TableName}}.{{.TableName}}{{end}}"{{else}}{{.ResourceType | upper}}{{end}} OF "{{.Grantee}}"`,
	))
)

// Grant holds parsed grant information.
type Grant struct {
	Privilege    string
	ResourceType string
	Grantee      string
	KeyspaceName string
	TableName    string
}

func resourceCassandraGrant() *schema.Resource {
	return &schema.Resource{
		Create: resourceGrantCreate,
		Read:   resourceGrantRead,
		Delete: resourceGrantDelete,
		Exists: resourceGrantExists,
		Schema: map[string]*schema.Schema{
			identifierPrivilege: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Privilege to grant (e.g., ALL, SELECT, MODIFY, etc.)",
			},
			identifierGrantee: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Name of the role to grant the privilege to",
			},
			identifierResourceType: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Type of resource for the privilege (KEYSPACE, TABLE, ROLE, etc.)",
			},
			identifierKeyspaceName: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "Keyspace name if the resource type requires a keyspace context",
			},
			identifierTableName: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "Table name if the resource type is TABLE (requires keyspace_name as well)",
			},
		},
	}
}

func parseGrantData(d *schema.ResourceData) (*Grant, error) {
	priv := d.Get(identifierPrivilege).(string)
	grantee := d.Get(identifierGrantee).(string)
	resType := d.Get(identifierResourceType).(string)
	ks := ""
	tbl := ""
	if v, ok := d.GetOk(identifierKeyspaceName); ok {
		ks = v.(string)
	}
	if v, ok := d.GetOk(identifierTableName); ok {
		tbl = v.(string)
	}
	// Validate that table name is provided if resource type is TABLE
	if resType != "" && strings.ToUpper(resType) == "TABLE" {
		if ks == "" || tbl == "" {
			return nil, fmt.Errorf("resource_type TABLE requires keyspace_name and table_name to be set")
		}
	}
	return &Grant{
		Privilege:    priv,
		ResourceType: resType,
		Grantee:      grantee,
		KeyspaceName: ks,
		TableName:    tbl,
	}, nil
}

func resourceGrantCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	grant, err := parseGrantData(d)
	if err != nil {
		return err
	}
	var cqlBuffer bytes.Buffer
	if err := createGrantTpl.Execute(&cqlBuffer, grant); err != nil {
		return err
	}
	cql := cqlBuffer.String()
	log.Printf("[INFO] Grant create CQL: %s", cql)
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	if err := session.Query(cql).Exec(); err != nil {
		return err
	}
	// Use a composite ID to identify the grant (grantee + resource + privilege)
	d.SetId(fmt.Sprintf("%s|%s|%s|%s|%s", grant.Grantee, strings.ToUpper(grant.ResourceType), grant.KeyspaceName, grant.TableName, strings.ToUpper(grant.Privilege)))
	return resourceGrantRead(d, meta)
}

func resourceGrantRead(d *schema.ResourceData, meta interface{}) error {
	exists, err := resourceGrantExists(d, meta)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("grant not found (it may have been revoked)")
	}
	// If exists, simply ensure all fields are correct in state
	grant, err := parseGrantData(d)
	if err != nil {
		return err
	}
	d.Set(identifierPrivilege, grant.Privilege)
	d.Set(identifierGrantee, grant.Grantee)
	d.Set(identifierResourceType, grant.ResourceType)
	d.Set(identifierKeyspaceName, grant.KeyspaceName)
	d.Set(identifierTableName, grant.TableName)
	return nil
}

func resourceGrantDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	grant, err := parseGrantData(d)
	if err != nil {
		return err
	}
	var cqlBuffer bytes.Buffer
	if err := deleteGrantTpl.Execute(&cqlBuffer, grant); err != nil {
		return err
	}
	cql := cqlBuffer.String()
	log.Printf("[INFO] Grant revoke CQL: %s", cql)
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	return session.Query(cql).Exec()
}

func resourceGrantExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	grant, err := parseGrantData(d)
	if err != nil {
		return false, err
	}
	var cqlBuffer bytes.Buffer
	if err := readGrantTpl.Execute(&cqlBuffer, grant); err != nil {
		return false, err
	}
	cql := cqlBuffer.String()
	log.Printf("[DEBUG] Grant exists check CQL: %s", cql)
	session, err := cluster.CreateSession()
	if err != nil {
		return false, err
	}
	defer session.Close()

	iter := session.Query(cql).Iter()
	count := iter.NumRows()
	errClose := iter.Close()
	return count > 0, errClose
}
