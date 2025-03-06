package cassandra

import (
	"fmt"
	"os"
	"testing"

	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

// Ensure Provider() returns a *schema.Provider from v2 package
var testAccProvider *schema.Provider = Provider()

var testAccProviderFactories = map[string]func() (*schema.Provider, error){
	"cassandra": func() (*schema.Provider, error) {
		return Provider(), nil
	},
}

func testAccPreCheck(t *testing.T) {
	if os.Getenv("CASSANDRA_HOST") == "" {
		t.Fatal("CASSANDRA_HOST must be set for acceptance tests")
	}
}

func TestAccCassandraKeyspace_basic(t *testing.T) {
	keyspace := "some_keyspace"
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
		},
		ProviderFactories: testAccProviderFactories,
		CheckDestroy:      testAccCassandraKeyspaceDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccCassandraKeyspaceConfigBasic(keyspace),
				Check: resource.ComposeTestCheckFunc(
					testAccCassandraKeyspaceExists("cassandra_keyspace.keyspace"),
					resource.TestCheckResourceAttr("cassandra_keyspace.keyspace", "name", keyspace),
					resource.TestCheckResourceAttr("cassandra_keyspace.keyspace", "replication_strategy", "SimpleStrategy"),
					resource.TestCheckResourceAttr("cassandra_keyspace.keyspace", "strategy_options.replication_factor", "1"),
				),
			},
			{
				ResourceName:      "cassandra_keyspace.keyspace",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func testAccCassandraKeyspaceConfigBasic(keyspace string) string {
	return fmt.Sprintf(`
resource "cassandra_keyspace" "keyspace" {
    name                 = "%s"
    replication_strategy = "SimpleStrategy"
    strategy_options     = {
      replication_factor = 1
    }
    durable_writes = true
}
`, keyspace)
}

func testAccCassandraKeyspaceDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*CassandraClient)
	cluster := client.Cluster
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "cassandra_keyspace" {
			continue
		}
		keyspaceName := rs.Primary.Attributes["name"]
		_, err := session.KeyspaceMetadata(keyspaceName)
		if err == gocql.ErrKeyspaceDoesNotExist {
			return nil
		}
		return fmt.Errorf("keyspace %s still exists", keyspaceName)
	}
	return nil
}

func testAccCassandraKeyspaceExists(resourceKey string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[resourceKey]
		if !ok {
			return fmt.Errorf("not found: %s", resourceKey)
		}
		if rs.Primary.ID == "" {
			return fmt.Errorf("no ID is set")
		}
		client := testAccProvider.Meta().(*CassandraClient)
		cluster := client.Cluster
		session, err := cluster.CreateSession()
		if err != nil {
			return err
		}
		defer session.Close()

		_, err = session.KeyspaceMetadata(rs.Primary.ID)
		if err != nil {
			return err
		}
		return nil
	}
}
