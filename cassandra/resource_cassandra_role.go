package cassandra

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"golang.org/x/crypto/bcrypt"
	"log"
)

func resourceCassandraRole() *schema.Resource {
	return &schema.Resource{
		Create: resourceRoleCreate,
		Read:   resourceRoleRead,
		Update: resourceRoleUpdate,
		Delete: resourceRoleDelete,
		Exists: resourceRoleExists,
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Name of the role (1-256 characters, cannot include \")",
				ValidateFunc: func(i interface{}, k string) ([]string, []error) {
					name := i.(string)
					if !validRoleRegex.MatchString(name) {
						return nil, []error{fmt.Errorf("role name must be 1-256 chars and cannot include double quotes")}
					}
					return nil, nil
				},
			},
			"super_user": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Grant the role superuser status (can create/manage other roles)",
			},
			"login": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     true,
				Description: "Allow the role to login (default true)",
			},
			"password": {
				Type:        schema.TypeString,
				Required:    true,
				Sensitive:   true,
				Description: "Password for the role (when using internal authentication)",
				ValidateFunc: func(i interface{}, k string) ([]string, []error) {
					pwd := i.(string)
					// Enforce a strong password length to match hashed password requirements
					if len(pwd) < 40 || len(pwd) > 512 || containsQuote(pwd) {
						return nil, []error{fmt.Errorf("password must be 40 to 512 characters and cannot contain quotes")}
					}
					return nil, nil
				},
			},
		},
	}
}

func containsQuote(s string) bool {
	for _, r := range s {
		if r == '"' || r == '\'' {
			return true
		}
	}
	return false
}

func resourceRoleCreate(d *schema.ResourceData, meta interface{}) error {
	return resourceRoleCreateOrUpdate(d, meta, true)
}

func resourceRoleUpdate(d *schema.ResourceData, meta interface{}) error {
	return resourceRoleCreateOrUpdate(d, meta, false)
}

func resourceRoleCreateOrUpdate(d *schema.ResourceData, meta interface{}, createRole bool) error {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	name := d.Get("name").(string)
	superUser := d.Get("super_user").(bool)
	login := d.Get("login").(bool)
	password := d.Get("password").(string)

	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	action := boolToAction[createRole] // "CREATE" or "ALTER"
	query := fmt.Sprintf("%s ROLE \"%s\" WITH PASSWORD = '%s' AND LOGIN = %t AND SUPERUSER = %t",
		action, name, password, login, superUser)
	log.Printf("[INFO] Executing CQL: %s", query)
	if err := session.Query(query).Exec(); err != nil {
		return err
	}
	d.SetId(name)
	d.Set("name", name)
	d.Set("super_user", superUser)
	d.Set("login", login)
	d.Set("password", password)
	return nil
}

func resourceRoleRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	name := d.Get("name").(string)
	plaintextPwd := d.Get("password").(string)

	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	// Read current role details from the system roles table
	roleName, canLogin, isSuperUser, saltedHash, readErr := readRole(session, client.SystemKeyspaceName, name)
	if readErr != nil {
		return readErr
	}
	if roleName == "" {
		// Role no longer exists
		d.SetId("")
		return nil
	}
	d.SetId(roleName)
	d.Set("name", roleName)
	d.Set("super_user", isSuperUser)
	d.Set("login", canLogin)

	// Compare stored hashed password with the provided password
	if saltedHash == "" {
		// No password set in DB
		d.Set("password", "")
	} else {
		if client.PasswordHashAlgorithm == "bcrypt" {
			// Use bcrypt comparison
			err := bcrypt.CompareHashAndPassword([]byte(saltedHash), []byte(plaintextPwd))
			if err == nil {
				d.Set("password", plaintextPwd)
			} else {
				// If mismatch, store the hashed value to signal drift
				d.Set("password", saltedHash)
			}
		} else if client.PasswordHashAlgorithm == "sha-512" {
			// For SHA-512, perform a simple check (note: full crypt comparison not implemented)
			if saltedHash == plaintextPwd {
				d.Set("password", plaintextPwd)
			} else {
				d.Set("password", saltedHash)
			}
		} else {
			// Unknown algorithm: default to not matching
			if saltedHash == plaintextPwd {
				d.Set("password", plaintextPwd)
			} else {
				d.Set("password", saltedHash)
			}
		}
	}
	return nil
}

func resourceRoleDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	name := d.Id()
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	query := fmt.Sprintf("DROP ROLE \"%s\"", name)
	log.Printf("[INFO] Dropping role with CQL: %s", query)
	return session.Query(query).Exec()
}

func resourceRoleExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	client := meta.(*CassandraClient)
	cluster := client.Cluster

	name := d.Get("name").(string)
	session, err := cluster.CreateSession()
	if err != nil {
		return false, err
	}
	defer session.Close()

	roleName, _, _, _, err := readRole(session, client.SystemKeyspaceName, name)
	return (err == nil && roleName == name), err
}

func readRole(session *gocql.Session, systemKeyspace, roleName string) (string, bool, bool, string, error) {
	var name string
	var canLogin bool
	var isSuperUser bool
	var saltedHash string

	query := fmt.Sprintf("SELECT role, can_login, is_superuser, salted_hash FROM %s.roles WHERE role = ?", systemKeyspace)
	iter := session.Query(query, roleName).Iter()
	defer iter.Close()

	for iter.Scan(&name, &canLogin, &isSuperUser, &saltedHash) {
		// Return the first (and only) row for the role
		return name, canLogin, isSuperUser, saltedHash, nil
	}
	if err := iter.Close(); err != nil {
		return "", false, false, "", err
	}
	// Role not found
	return "", false, false, "", nil
}
