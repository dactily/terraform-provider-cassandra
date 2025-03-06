package cassandra

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func resourceCassandraKeyspace() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeyspaceCreate,
		Read:   resourceKeyspaceRead,
		Update: resourceKeyspaceUpdate,
		Delete: resourceKeyspaceDelete,
		Exists: resourceKeyspaceExists,
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Name of the keyspace (1-48 alphanumeric characters or underscores)",
				ValidateFunc: func(i interface{}, k string) ([]string, []error) {
					name := i.(string)
					if !validKeyspaceRegex.MatchString(name) {
						return nil, []error{fmt.Errorf("%q is not a valid keyspace name", name)}
					}
					return nil, nil
				},
			},
			"replication_strategy": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Replication strategy (SimpleStrategy or NetworkTopologyStrategy)",
			},
			"strategy_options": {
				Type:        schema.TypeMap,
				Required:    true,
				ForceNew:    true,
				Description: "Options for the replication strategy (e.g., replication_factor for SimpleStrategy or datacenter options for NetworkTopologyStrategy)",
			},
			"durable_writes": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     true,
				ForceNew:    true,
				Description: "Whether durable writes are enabled (defaults to true)",
			},
		},
	}
}

func resourceKeyspaceCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	name := d.Get("name").(string)
	strategy := d.Get("replication_strategy").(string)
	options := d.Get("strategy_options").(map[string]interface{})
	durable := d.Get("durable_writes").(bool)

	// Build replication options string
	var opts []string
	for k, v := range options {
		opts = append(opts, fmt.Sprintf("'%s': '%v'", k, v))
	}
	replicationConfig := fmt.Sprintf("{%s}", strings.Join(opts, ", "))

	query := fmt.Sprintf("CREATE KEYSPACE \"%s\" WITH replication = {'class': '%s', %s} AND durable_writes = %t",
		name, strategy, replicationConfig, durable)

	log.Printf("[INFO] Creating keyspace: %s", query)
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	if err := session.Query(query).Exec(); err != nil {
		return err
	}
	d.SetId(name)
	return resourceKeyspaceRead(d, meta)
}

func resourceKeyspaceRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	name := d.Id()
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	// Use DESCRIBE or metadata to verify keyspace existence
	metaKeyspace, err := cluster.Metadata()
	if err != nil {
		return err
	}
	ksMeta, ok := metaKeyspace.Keyspaces[name]
	if !ok {
		log.Printf("[WARN] Keyspace %s not found (it may have been removed)", name)
		d.SetId("")
		return nil
	}

	d.Set("name", name)
	d.Set("replication_strategy", ksMeta.StrategyClass)
	// Convert strategy options to map of strings (skip if already in desired format)
	opts := make(map[string]string)
	for k, v := range ksMeta.StrategyOptions {
		opts[k] = fmt.Sprintf("%v", v)
	}
	d.Set("strategy_options", opts)
	d.Set("durable_writes", ksMeta.DurableWrites)
	return nil
}

func resourceKeyspaceUpdate(d *schema.ResourceData, meta interface{}) error {
	// In Cassandra, keyspace properties can be altered (replication, durable_writes).
	// We handle changes by constructing an ALTER KEYSPACE CQL statement.
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	name := d.Get("name").(string)
	if d.HasChange("replication_strategy") || d.HasChange("strategy_options") || d.HasChange("durable_writes") {
		strategy := d.Get("replication_strategy").(string)
		options := d.Get("strategy_options").(map[string]interface{})
		durable := d.Get("durable_writes").(bool)
		var opts []string
		for k, v := range options {
			opts = append(opts, fmt.Sprintf("'%s': '%v'", k, v))
		}
		replicationConfig := fmt.Sprintf("{%s}", strings.Join(opts, ", "))
		query := fmt.Sprintf("ALTER KEYSPACE \"%s\" WITH replication = {'class': '%s', %s} AND durable_writes = %t",
			name, strategy, replicationConfig, durable)
		log.Printf("[INFO] Altering keyspace: %s", query)
		session, err := cluster.CreateSession()
		if err != nil {
			return err
		}
		defer session.Close()
		if err := session.Query(query).Exec(); err != nil {
			return err
		}
	}
	return resourceKeyspaceRead(d, meta)
}

func resourceKeyspaceDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	name := d.Id()
	query := fmt.Sprintf("DROP KEYSPACE \"%s\"", name)
	log.Printf("[INFO] Dropping keyspace: %s", query)
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()
	return session.Query(query).Exec()
}

func resourceKeyspaceExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	name := d.Get("name").(string)
	session, err := cluster.CreateSession()
	if err != nil {
		return false, err
	}
	defer session.Close()

	metaKeyspace, err := cluster.Metadata()
	if err != nil {
		return false, err
	}
	_, exists := metaKeyspace.Keyspaces[name]
	return exists, nil
}
