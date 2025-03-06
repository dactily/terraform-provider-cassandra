package cassandra

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

// Ensure Provider() is using Terraform SDK v2 correctly.
//var testAccProvider *schema.Provider = Provider()
//
//var testAccProviderFactories = map[string]func() (*schema.Provider, error){
//	"cassandra": func() (*schema.Provider, error) {
//		return Provider(), nil
//	},
//}
//
//func testAccPreCheck(t *testing.T) {
//	if os.Getenv("CASSANDRA_HOST") == "" {
//		t.Fatal("CASSANDRA_HOST must be set for acceptance tests")
//	}
//}

func TestAccCassandraGrant_basic(t *testing.T) {
	resourceName := "cassandra_grant.test"
	role := "test_role"
	keyspace := "test_keyspace"

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
		},
		ProviderFactories: testAccProviderFactories,
		CheckDestroy:      testAccCassandraGrantDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccCassandraGrantConfigBasic(role, keyspace),
				Check: resource.ComposeTestCheckFunc(
					testAccCassandraGrantExists(resourceName),
					resource.TestCheckResourceAttr(resourceName, "grantee", role),
					resource.TestCheckResourceAttr(resourceName, "privilege", "SELECT"),
					resource.TestCheckResourceAttr(resourceName, "resource_type", "KEYSPACE"),
					resource.TestCheckResourceAttr(resourceName, "keyspace_name", keyspace),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func TestAccCassandraGrant_invalid(t *testing.T) {
	role := "test_role"
	keyspace := "test_keyspace"

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
		},
		ProviderFactories: testAccProviderFactories,
		CheckDestroy:      testAccCassandraGrantDestroy,
		Steps: []resource.TestStep{
			{
				Config:      testAccCassandraGrantConfigInvalid(role, keyspace),
				ExpectError: regexp.MustCompile(".*Invalid privilege type provided.*"),
			},
		},
	})
}

func testAccCassandraGrantConfigBasic(role, keyspace string) string {
	return fmt.Sprintf(`
resource "cassandra_role" "test" {
    name = "%s"
}

resource "cassandra_grant" "test" {
    privilege     = "SELECT"
    resource_type = "KEYSPACE"
    keyspace_name = "%s"
    grantee       = cassandra_role.test.name
}
`, role, keyspace)
}

func testAccCassandraGrantConfigInvalid(role, keyspace string) string {
	return fmt.Sprintf(`
resource "cassandra_role" "test" {
    name = "%s"
}

resource "cassandra_grant" "test" {
    privilege     = "INVALID_PRIVILEGE"
    resource_type = "KEYSPACE"
    keyspace_name = "%s"
    grantee       = cassandra_role.test.name
}
`, role, keyspace)
}

func testAccCassandraGrantDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*CassandraClient)
	cluster := client.Cluster
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "cassandra_grant" {
			continue
		}
		grantee := rs.Primary.Attributes["grantee"]
		keyspace := rs.Primary.Attributes["keyspace_name"]
		privilege := rs.Primary.Attributes["privilege"]

		query := fmt.Sprintf("SELECT permissions FROM %s.role_permissions WHERE role = ? AND resource = ?", client.SystemKeyspaceName)
		iter := session.Query(query, grantee, fmt.Sprintf("data/%s", keyspace)).Iter()
		defer iter.Close()

		if iter.NumRows() > 0 {
			return fmt.Errorf("grant %s on keyspace %s for %s still exists", privilege, keyspace, grantee)
		}
	}
	return nil
}

func testAccCassandraGrantExists(resourceKey string) resource.TestCheckFunc {
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

		grantee := rs.Primary.Attributes["grantee"]
		keyspace := rs.Primary.Attributes["keyspace_name"]
		privilege := rs.Primary.Attributes["privilege"]

		query := fmt.Sprintf("SELECT permissions FROM %s.role_permissions WHERE role = ? AND resource = ?", client.SystemKeyspaceName)
		iter := session.Query(query, grantee, fmt.Sprintf("data/%s", keyspace)).Iter()
		defer iter.Close()

		if iter.NumRows() == 0 {
			return fmt.Errorf("grant %s on keyspace %s for %s not found", privilege, keyspace, grantee)
		}
		return nil
	}
}
