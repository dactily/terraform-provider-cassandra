package cassandra

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func TestProviderSchema(t *testing.T) {
	p := Provider()
	if err := p.InternalValidate(); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Verify that required provider configuration keys exist
	schemaMap := p.Schema
	expectedKeys := []string{"username", "password", "hosts", "port", "connection_timeout", "use_ssl", "root_ca", "min_tls_version", "protocol_version", "system_keyspace_name", "pw_encryption_algorithm"}
	for _, k := range expectedKeys {
		if _, ok := schemaMap[k]; !ok {
			t.Errorf("expected provider schema to have key %q", k)
		}
	}
	// Check default values for new settings
	d := schema.TestResourceDataRaw(t, schemaMap, map[string]interface{}{})
	if d.Get("system_keyspace_name").(string) != "system_auth" {
		t.Errorf("expected default system_keyspace_name to be 'system_auth'")
	}
	if d.Get("pw_encryption_algorithm").(string) != "bcrypt" {
		t.Errorf("expected default pw_encryption_algorithm to be 'bcrypt'")
	}
}
