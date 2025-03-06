package cassandra

import (
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"testing"
)

func TestRolePasswordValidation(t *testing.T) {
	rs := resourceCassandraRole()
	passwordSchema := rs.Schema["password"]

	// Password shorter than 40 chars should fail validation
	warnings, errs := passwordSchema.ValidateFunc("shortpass", "password")
	if len(errs) == 0 {
		t.Errorf("expected validation error for short password, got none")
	}
	// Password with quotes should fail
	warnings, errs = passwordSchema.ValidateFunc(`"insecure"`, "password")
	if len(errs) == 0 {
		t.Errorf("expected validation error for password with quotes, got none")
	}
	// Valid long password (40 chars, no quotes) should pass
	validPwd := "A123456789B123456789C123456789D123456789" // 40 characters
	warnings, errs = passwordSchema.ValidateFunc(validPwd, "password")
	if len(errs) > 0 {
		t.Errorf("expected no error for valid password, got %v", errs)
	}
}

func TestRoleResourceCreateAndRead(t *testing.T) {
	p := Provider().(*schema.Provider)
	d := schema.TestResourceDataRaw(t, resourceCassandraRole().Schema, map[string]interface{}{
		"name":       "test_role",
		"super_user": true,
		"login":      true,
		"password":   "A123456789B123456789C123456789D123456789",
	})
	meta, err := p.ConfigureFunc(p.Data(nil))
	if err != nil {
		t.Fatalf("Provider configure error: %s", err)
	}

	// Since we cannot actually connect to a cluster in unit tests, we simulate create
	err = resourceRoleCreate(d, meta)
	if err != nil {
		t.Fatalf("Unexpected error on role create: %s", err)
	}
	if d.Id() != "test_role" {
		t.Errorf("expected role ID to be 'test_role', got %q", d.Id())
	}
	// Simulate reading back (assuming it would match since no actual DB changes)
	err = resourceRoleRead(d, meta)
	if err != nil {
		t.Errorf("Unexpected error on role read: %s", err)
	}
	if d.Get("name").(string) != "test_role" {
		t.Errorf("expected name to remain 'test_role'")
	}
	if d.Get("super_user").(bool) != true {
		t.Errorf("expected super_user to be true")
	}
	if d.Get("login").(bool) != true {
		t.Errorf("expected login to be true")
	}
	// Password in state could be hashed or same depending on logic; ensure not empty
	if d.Get("password").(string) == "" {
		t.Errorf("expected password to be set in state")
	}
}
