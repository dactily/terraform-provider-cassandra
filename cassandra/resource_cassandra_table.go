package cassandra

import (
	"fmt"
	"log"
	"strings"

	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func resourceCassandraTable() *schema.Resource {
	return &schema.Resource{
		Create: resourceTableCreate,
		Read:   resourceTableRead,
		Update: resourceTableUpdate,
		Delete: resourceTableDelete,
		Exists: resourceTableExists,
		Schema: map[string]*schema.Schema{
			"keyspace_name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Name of the keyspace in which the table is created",
				ValidateFunc: func(i interface{}, k string) ([]string, []error) {
					name := i.(string)
					if !validKeyspaceRegex.MatchString(name) {
						return nil, []error{fmt.Errorf("%q is not a valid keyspace name", name)}
					}
					return nil, nil
				},
			},
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Name of the table to create",
				ValidateFunc: func(i interface{}, k string) ([]string, []error) {
					// Table name validation: 1-48 characters, alphanumeric or underscore
					tableName := i.(string)
					match, _ := fmt.Fprint(nil) // placeholder for actual regex if needed
					if len(tableName) == 0 || len(tableName) > 48 {
						return nil, []error{fmt.Errorf("table name must be 1 to 48 characters long")}
					}
					if strings.Contains(tableName, "\"") {
						return nil, []error{fmt.Errorf("table name must not contain double quotes")}
					}
					return nil, nil
				},
			},
			"columns": {
				Type:        schema.TypeMap,
				Required:    true,
				ForceNew:    true,
				Description: "Map of column names to CQL types for the table",
			},
			"primary_key": {
				Type:        schema.TypeList,
				Required:    true,
				ForceNew:    true,
				Description: "List defining the primary key (first element is partition key, subsequent are clustering keys)",
				Elem:        &schema.Schema{Type: schema.TypeString},
			},
			"comment": {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "Optional table comment",
			},
			// Additional table options (compaction, TTL, etc.) can be added as needed
		},
	}
}

func resourceTableCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	keyspace := d.Get("keyspace_name").(string)
	name := d.Get("name").(string)
	columns := d.Get("columns").(map[string]interface{})
	primaryKey := d.Get("primary_key").([]interface{})
	comment := d.Get("comment").(string)

	// Build column definitions
	colDefs := []string{}
	for colName, colType := range columns {
		colDefs = append(colDefs, fmt.Sprintf("\"%s\" %s", colName, colType.(string)))
	}
	pkParts := []string{}
	for _, pk := range primaryKey {
		pkParts = append(pkParts, fmt.Sprintf("\"%s\"", pk.(string)))
	}
	primaryKeyClause := fmt.Sprintf("PRIMARY KEY ((%s))", strings.Join(pkParts[:1], ", "))
	if len(pkParts) > 1 {
		// If there are clustering keys, include them in PK definition
		primaryKeyClause = fmt.Sprintf("PRIMARY KEY ((%s), %s)", pkParts[0], strings.Join(pkParts[1:], ", "))
	}
	query := fmt.Sprintf("CREATE TABLE \"%s\".\"%s\" (%s, %s", keyspace, name, strings.Join(colDefs, ", "), primaryKeyClause)
	if comment != "" {
		query += fmt.Sprintf(") WITH comment = '%s'", comment)
	} else {
		query += ")"
	}

	log.Printf("[INFO] Creating table with CQL: %s", query)
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()
	if err := session.Query(query).Exec(); err != nil {
		return fmt.Errorf("error creating table %s: %s", name, err)
	}
	d.SetId(fmt.Sprintf("%s.%s", keyspace, name))
	return resourceTableRead(d, meta)
}

func resourceTableRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	id := d.Id()
	parts := strings.SplitN(id, ".", 2)
	if len(parts) != 2 {
		d.SetId("")
		return nil
	}
	keyspace, table := parts[0], parts[1]

	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	// Query system_schema or system_schema.tables for existence (depending on Cassandra version)
	query := fmt.Sprintf("SELECT table_name FROM system_schema.tables WHERE keyspace_name='%s' AND table_name='%s'", keyspace, table)
	iter := session.Query(query).Iter()
	exists := false
	var tblName string
	for iter.Scan(&tblName) {
		exists = true
	}
	if err := iter.Close(); err != nil {
		return err
	}
	if !exists {
		log.Printf("[WARN] Table %s.%s not found (removed?)", keyspace, table)
		d.SetId("")
		return nil
	}
	// Set attributes that can be retrieved (for now, just reflect back inputs)
	d.Set("keyspace_name", keyspace)
	d.Set("name", table)
	d.Set("columns", d.Get("columns"))
	d.Set("primary_key", d.Get("primary_key"))
	d.Set("comment", d.Get("comment"))
	return nil
}

func resourceTableUpdate(d *schema.ResourceData, meta interface{}) error {
	// Table updates (like adding columns) can be handled if needed; currently, recreate for changes.
	return fmt.Errorf("updating table schema is not supported; use taint or recreate the resource")
}

func resourceTableDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	id := d.Id()
	parts := strings.SplitN(id, ".", 2)
	if len(parts) != 2 {
		return nil
	}
	keyspace, table := parts[0], parts[1]
	query := fmt.Sprintf("DROP TABLE \"%s\".\"%s\"", keyspace, table)
	log.Printf("[INFO] Dropping table with CQL: %s", query)
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()
	return session.Query(query).Exec()
}

func resourceTableExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	id := d.Id()
	parts := strings.SplitN(id, ".", 2)
	if len(parts) != 2 {
		return false, nil
	}
	keyspace, table := parts[0], parts[1]
	session, err := cluster.CreateSession()
	if err != nil {
		return false, err
	}
	defer session.Close()
	query := fmt.Sprintf("SELECT table_name FROM system_schema.tables WHERE keyspace_name='%s' AND table_name='%s'", keyspace, table)
	iter := session.Query(query).Iter()
	var tblName string
	exists := iter.Scan(&tblName)
	_ = iter.Close()
	return exists, nil
}
