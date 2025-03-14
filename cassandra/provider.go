package cassandra

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

var (
	allowedTLSProtocols = map[string]uint16{
		"TLS1.0": tls.VersionTLS10,
		"TLS1.1": tls.VersionTLS11,
		"TLS1.2": tls.VersionTLS12,
		"TLS1.3": tls.VersionTLS13,
	}

	allowedConsistencies = map[string]gocql.Consistency{
		"ANY":          gocql.Any,
		"ONE":          gocql.One,
		"TWO":          gocql.Two,
		"THREE":        gocql.Three,
		"QUORUM":       gocql.Quorum,
		"ALL":          gocql.All,
		"LOCAL_QUORUM": gocql.LocalQuorum,
		"EACH_QUORUM":  gocql.EachQuorum,
		"LOCAL_ONE":    gocql.LocalOne,
	}
)

// ProviderConfig wraps the underlying gocql.ClusterConfig and holds additional settings.
type ProviderConfig struct {
	Cluster            *gocql.ClusterConfig
	SystemKeyspaceName string
}

// Provider returns a terraform.ResourceProvider
func Provider() *schema.Provider {
	return &schema.Provider{
		ResourcesMap: map[string]*schema.Resource{
			"cassandra_keyspace": resourceCassandraKeyspace(),
			"cassandra_role":     resourceCassandraRole(),
			"cassandra_grant":    resourceCassandraGrant(),
			"cassandra_table":    resourceCassandraTableSpace(),
		},
		ConfigureContextFunc: configureProvider,
		Schema: map[string]*schema.Schema{
			"username": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("CASSANDRA_USERNAME", ""),
				Description: "Cassandra username",
				Sensitive:   true,
			},
			"password": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("CASSANDRA_PASSWORD", ""),
				Description: "Cassandra password",
				Sensitive:   true,
			},
			"port": {
				Type:         schema.TypeInt,
				Optional:     true,
				DefaultFunc:  schema.EnvDefaultFunc("CASSANDRA_PORT", 9042),
				Description:  "Cassandra CQL Port",
				ValidateFunc: validation.IsPortNumber,
			},
			"host": {
				Type:         schema.TypeString,
				DefaultFunc:  schema.EnvDefaultFunc("CASSANDRA_HOST", nil),
				Description:  "Cassandra host",
				Optional:     true,
				ExactlyOneOf: []string{"host", "hosts"},
			},
			"hosts": {
				Type: schema.TypeList,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				MinItems:    1,
				Optional:    true,
				Description: "Cassandra hosts",
			},
			"host_filter": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Filter all incoming events for host. Hosts have to exist before using this provider",
			},
			"connection_timeout": {
				Type:        schema.TypeInt,
				Optional:    true,
				Default:     1000,
				Description: "Connection timeout in milliseconds",
			},
			"root_ca": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Use root CA to connect to Cluster. Applies only when use_ssl is enabled",
				ValidateDiagFunc: func(i interface{}, path cty.Path) diag.Diagnostics {
					rootCA := i.(string)
					if rootCA == "" {
						return nil
					}
					caPool := x509.NewCertPool()
					ok := caPool.AppendCertsFromPEM([]byte(rootCA))
					if !ok {
						return diag.Diagnostics{
							{
								Severity:      diag.Error,
								Summary:       "Invalid PEM",
								Detail:        fmt.Sprintf("%s: invalid PEM", rootCA),
								AttributePath: path,
							},
						}
					}
					return nil
				},
			},
			"use_ssl": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Use SSL when connecting to cluster",
			},
			"min_tls_version": {
				Type:         schema.TypeString,
				Optional:     true,
				Default:      "TLS1.2",
				Description:  "Minimum TLS Version used to connect to the cluster - allowed values are TLS1.0, TLS1.1, TLS1.2, TLS1.3. Applies only when use_ssl is enabled",
				ValidateFunc: validation.StringInSlice([]string{"TLS1.0", "TLS1.1", "TLS1.2", "TLS1.3"}, false),
			},
			"enable_host_verification": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     true,
				Description: "Option to disable host verification with SSL. Setting this to false is equivalent to setting SSL_VALIDATE=false with cql",
			},
			"protocol_version": {
				Type:        schema.TypeInt,
				Optional:    true,
				Default:     4,
				Description: "CQL Binary Protocol Version",
			},
			"consistency": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     gocql.Quorum.String(),
				Description: "Default consistency level",
			},
			"cql_version": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "3.0.0",
				Description: "CQL version",
			},
			"keyspace": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Initial Keyspace",
			},
			"disable_initial_host_lookup": {
				Type:        schema.TypeBool,
				Optional:    true,
				Description: "Whether the driver will not attempt to get host info from the system.peers table",
			},
			"system_keyspace_name": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "system_auth",
				Description: "System keyspace name for roles and grants",
			},
			"pw_encryption_algorithm": {
				Type:         schema.TypeString,
				Optional:     true,
				Default:      "bcrypt",
				Description:  "Password encryption algorithm. Allowed values: bcrypt, sha-512",
				ValidateFunc: validation.StringInSlice([]string{"bcrypt", "sha-512"}, false),
			},
		},
	}
}

func configureProvider(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	log.Printf("Creating provider")

	useSSL := d.Get("use_ssl").(bool)
	username := d.Get("username").(string)
	password := d.Get("password").(string)
	port := d.Get("port").(int)
	connectionTimeout := d.Get("connection_timeout").(int)
	protocolVersion := d.Get("protocol_version").(int)
	diags := diag.Diagnostics{}

	var rawHosts []interface{}
	if rawHost, ok := d.GetOk("host"); ok {
		rawHosts = []interface{}{rawHost}
	} else {
		rawHosts = d.Get("hosts").([]interface{})
	}

	hosts := make([]string, 0, len(rawHosts))
	hostFilter := d.Get("host_filter").(bool)
	for _, v := range rawHosts {
		hosts = append(hosts, v.(string))
		log.Printf("Using host %v", v.(string))
	}

	cluster := gocql.NewCluster()
	cluster.Hosts = hosts
	cluster.Port = port
	cluster.Authenticator = &gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	cluster.ConnectTimeout = time.Millisecond * time.Duration(connectionTimeout)
	cluster.Timeout = time.Minute * 1
	cluster.CQLVersion = d.Get("cql_version").(string)

	if v, ok := d.GetOk("keyspace"); ok && v.(string) != "" {
		cluster.Keyspace = v.(string)
	}

	cluster.Consistency = allowedConsistencies[d.Get("consistency").(string)]
	cluster.ProtoVersion = protocolVersion

	if hostFilter {
		cluster.HostFilter = gocql.WhiteListHostFilter(hosts...)
	}

	if v, ok := d.GetOk("disable_initial_host_lookup"); ok {
		cluster.DisableInitialHostLookup = v.(bool)
	}

	if useSSL {
		rootCA := d.Get("root_ca").(string)
		minTLSVersion := d.Get("min_tls_version").(string)
		tlsConfig := &tls.Config{
			MinVersion: allowedTLSProtocols[minTLSVersion],
		}
		if rootCA != "" {
			caPool := x509.NewCertPool()
			ok := caPool.AppendCertsFromPEM([]byte(rootCA))
			if !ok {
				diags = append(diags, diag.Diagnostic{
					Severity:      diag.Error,
					Summary:       "Unable to load rootCA",
					AttributePath: cty.Path{cty.GetAttrStep{Name: "root_ca"}},
				})
				return nil, diags
			}
			tlsConfig.RootCAs = caPool
		}
		cluster.SslOpts = &gocql.SslOptions{
			Config: tlsConfig,
			EnableHostVerification: d.Get("enable_host_verification").(bool),
		}
	}

	systemKeyspaceName := d.Get("system_keyspace_name").(string)

	return &ProviderConfig{
		Cluster:            cluster,
		SystemKeyspaceName: systemKeyspaceName,
	}, diags
}
