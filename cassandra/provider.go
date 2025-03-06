package cassandra

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

// CassandraClient holds the cluster configuration and settings for system keyspace and password hashing.
type CassandraClient struct {
	Cluster               *gocql.ClusterConfig
	SystemKeyspaceName    string
	PasswordHashAlgorithm string
}

// Provider returns the Terraform provider configuration for Cassandra/ScyllaDB.
func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"username": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "",
				Description: "Cassandra username for authentication",
				Sensitive:   true,
			},
			"password": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "",
				Description: "Cassandra password for authentication",
				Sensitive:   true,
			},
			"port": {
				Type:        schema.TypeInt,
				Optional:    true,
				Default:     9042,
				Description: "Cassandra CQL port (default 9042)",
				ValidateFunc: func(i interface{}, k string) ([]string, []error) {
					port := i.(int)
					if port <= 0 || port >= 65535 {
						return nil, []error{fmt.Errorf("%d: invalid port - must be between 1 and 65535", port)}
					}
					return nil, nil
				},
			},
			"hosts": {
				Type:        schema.TypeList,
				Elem:        &schema.Schema{Type: schema.TypeString},
				MinItems:    1,
				Required:    true,
				Description: "List of contact point hosts for the Cassandra/Scylla cluster",
			},
			"connection_timeout": {
				Type:        schema.TypeInt,
				Optional:    true,
				Default:     1000,
				Description: "Connection timeout to the cluster in milliseconds",
			},
			"root_ca": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "PEM-encoded CA certificate for TLS (when use_ssl is true)",
				ValidateFunc: func(i interface{}, k string) ([]string, []error) {
					rootCA := i.(string)
					if rootCA == "" {
						return nil, nil
					}
					caPool := x509.NewCertPool()
					if ok := caPool.AppendCertsFromPEM([]byte(rootCA)); !ok {
						return nil, []error{fmt.Errorf("invalid PEM data for root_ca")}
					}
					return nil, nil
				},
			},
			"use_ssl": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Enable SSL/TLS for connection",
			},
			"min_tls_version": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "TLS1.2",
				Description: "Minimum TLS version for SSL connection (TLS1.2 by default)",
			},
			"protocol_version": {
				Type:        schema.TypeInt,
				Optional:    true,
				Default:     4,
				Description: "CQL protocol version (default 4)",
			},
			"system_keyspace_name": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "system_auth",
				Description: "System keyspace storing roles and permissions (\"system_auth\" for Cassandra/older Scylla, \"system\" for newer Scylla)",
			},
			"pw_encryption_algorithm": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "bcrypt",
				Description: "Hash algorithm for storing passwords (\"bcrypt\" for Cassandra/older Scylla, \"sha-512\" for newer Scylla)",
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"cassandra_keyspace": resourceCassandraKeyspace(),
			"cassandra_table":    resourceCassandraTable(),
			"cassandra_role":     resourceCassandraRole(),
			"cassandra_grant":    resourceCassandraGrant(),
		},
		ConfigureFunc: configureProvider,
	}
}

var allowedTLSProtocols = map[string]uint16{
	"TLS1.0": tls.VersionTLS10,
	"TLS1.1": tls.VersionTLS11,
	"TLS1.2": tls.VersionTLS12,
	"TLS1.3": tls.VersionTLS13,
}

// configureProvider initializes the Cassandra cluster connection and returns a client.
func configureProvider(d *schema.ResourceData) (interface{}, error) {
	log.Printf("[INFO] Initializing Cassandra/Scylla provider")
	// Gather provider settings
	hostsRaw := d.Get("hosts").([]interface{})
	hosts := make([]string, 0, len(hostsRaw))
	for _, h := range hostsRaw {
		hosts = append(hosts, h.(string))
		log.Printf("[DEBUG] Using host %s", h.(string))
	}
	port := d.Get("port").(int)
	username := d.Get("username").(string)
	password := d.Get("password").(string)
	useSSL := d.Get("use_ssl").(bool)
	connectionTimeout := d.Get("connection_timeout").(int)
	protocolVersion := d.Get("protocol_version").(int)
	systemKeyspace := d.Get("system_keyspace_name").(string)
	pwAlgorithm := d.Get("pw_encryption_algorithm").(string)

	// Configure cluster
	cluster := gocql.NewCluster()
	cluster.Hosts = hosts
	cluster.Port = port
	cluster.Authenticator = &gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	cluster.ConnectTimeout = time.Millisecond * time.Duration(connectionTimeout)
	cluster.Timeout = 1 * time.Minute
	cluster.CQLVersion = "3.0.0"
	cluster.Keyspace = systemKeyspace
	cluster.ProtoVersion = protocolVersion
	cluster.HostFilter = gocql.WhiteListHostFilter(hosts...)
	cluster.DisableInitialHostLookup = true

	if useSSL {
		rootCA := d.Get("root_ca").(string)
		minTLS := d.Get("min_tls_version").(string)
		tlsConfig := &tls.Config{
			MinVersion: allowedTLSProtocols[minTLS],
		}
		if rootCA != "" {
			caPool := x509.NewCertPool()
			if ok := caPool.AppendCertsFromPEM([]byte(rootCA)); !ok {
				return nil, errors.New("unable to load root CA")
			}
			tlsConfig.RootCAs = caPool
		}
		cluster.SslOpts = &gocql.SslOptions{Config: tlsConfig}
	}

	log.Printf("[INFO] Cassandra cluster configuration prepared. Hosts: %v, Port: %d, Keyspace: %s, SSL: %v", hosts, port, systemKeyspace, useSSL)
	return &CassandraClient{
		Cluster:               cluster,
		SystemKeyspaceName:    systemKeyspace,
		PasswordHashAlgorithm: pwAlgorithm,
	}, nil
}
