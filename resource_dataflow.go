package google

import (
	"os"
	"fmt"
	"log"
	"bytes"
	"os/exec"
	"io/ioutil"
	"encoding/json"

	"github.com/hashicorp/terraform/helper/schema"
	"google.golang.org/api/googleapi"
)

func resourceDataFlow() *schema.Resource {
	return &schema.Resource{
		Create: resourceDataflowCreate,
		Read:   resourceDataflowRead,
		Delete: resourceDataflowDelete,

		Schema: map[string]*schema.Schema{
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"jarfile": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},

			"class": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},

			"project": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},

			"staging_bucket": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},

			"jobid": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},
		},
	}
}


// accountFile represents the structure of the account file JSON file.
type accountFile struct {
	PrivateKeyId string `json:"private_key_id"`
	PrivateKey   string `json:"private_key"`
	ClientEmail  string `json:"client_email"`
	ClientId     string `json:"client_id"`
}

//  return pointer to a file that contains account information
func setAccountFile(contents string) (string, error) {
	if accountFile != "" {
		var account accountFile
		if err := parseJSON(&accont, contents); err == nil {
			//  raw account info, write out to a file
			tmpfile, err := ioutil.TempFile()
			if err != nil {
				return "", err
			}
			_, err = tmpfile.WriteString(contents)
			if err != nil {
				return "", err
			}
			tmpfile.Close()
			return tempfile.Name(), nil
		} else {
			return contents, nil
		}
	}
	return "", nil
}

func cleanupTempAccountFile(rawAccountFile, account_file string) {
	if rawAccountFile != account_file {
		os.Remove(account_file)
	}
}

//  init function will make sure that gcloud cli is installed,
//  authorized and that dataflow commands are available
func init(config *Config) error {
	//  check that gcloud is installed
	_, err := exec.LookPath("gcloud")
	if err != nil {
		log.Error("gcloud cli is not installed.  Please install and try again")
		return err
	}

	//  ensure that the found gcloud is authorized
	account_file, err := setAccountFile(config.AccountFile)
	defer cleanupTempAccountFile(config.AccountFile, account_file)
	if err != nil {
		return err
	}
	auth_cmd := exec.Cmd("gcloud", "auth", "activate-service-account", "--key-file", account_file)
	var stdout, stderr bytes.Buffer
	auth_cmd.Stdout = &stdout
	auth_cmd.Stderr = &stderr
	err = auth_cmd.Run()
	if err != nil {
		log.Error("Dataflow auth failed with error: %q", stdout.String())
		return err 
	}
	
	// verify that datacloud functions are installed
	//  this will need to be updated when they come out of alpha
	datacloud_cmd := exec.Cmd("gcloud", "alpha", "dataflow" , "-h")
	err = datacloud_cmd.Run()
	if err != nil {
		log.Error("gcloud dataflow commands not installed.")
		return err
	}

	return nil
}

func resourceDataFlowCreate(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)
        err := init(config)
	if err != nil {
		return err
	}

	//  at this point we have verified that our command line jankiness is going to work
	//  get to it
	//  I'm assuming, possibly foolishly, that java is installed on this system
	create_dataflow_cmd := exec.Cmd("java", "-cp", d.Get("jarfile"), d.Get("class"), "--project="+d.Get("project"), "--stagingLocation="+d.Get("staging_bucket"), "--jobName="+d.Get("name"))
	var stdout, stderr bytes.Buffer
	create_dataflow_cmd.Stdout = &stdout
	create_dataflow_cmd.Stderr = &stderr
	err := create_dataflow_cmd.Run()
	if err != nil {
		return fmt.Errorf("Error submitting dataflow job: %q", stderr.String())
	}
	
	//  job successfully submitted, now get the job id
	

	// Build the address parameter
	addr := &compute.Address{Name: d.Get("name").(string)}
	op, err := config.clientCompute.Addresses.Insert(
		config.Project, region, addr).Do()
	if err != nil {
		return fmt.Errorf("Error creating address: %s", err)
	}

	// It probably maybe worked, so store the ID now
	d.SetId(addr.Name)

	err = computeOperationWaitRegion(config, op, region, "Creating Address")
	if err != nil {
		return err
	}

	return resourceComputeAddressRead(d, meta)
}

func resourceDataflowRead(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)

	region := getOptionalRegion(d, config)

	addr, err := config.clientCompute.Addresses.Get(
		config.Project, region, d.Id()).Do()
	if err != nil {
		if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == 404 {
			// The resource doesn't exist anymore
			d.SetId("")

			return nil
		}

		return fmt.Errorf("Error reading address: %s", err)
	}

	d.Set("address", addr.Address)
	d.Set("self_link", addr.SelfLink)

	return nil
}

func resourceDataflowDelete(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)

	region := getOptionalRegion(d, config)
	// Delete the address
	log.Printf("[DEBUG] address delete request")
	op, err := config.clientCompute.Addresses.Delete(
		config.Project, region, d.Id()).Do()
	if err != nil {
		return fmt.Errorf("Error deleting address: %s", err)
	}

	err = computeOperationWaitRegion(config, op, region, "Deleting Address")
	if err != nil {
		return err
	}

	d.SetId("")
	return nil
}
