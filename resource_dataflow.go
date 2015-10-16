package google

import (
	"os"
	"fmt"
	"log"
	"bytes"
	"regexp"
	"strings"
	"os/exec"
	"io/ioutil"
	"encoding/json"

	"github.com/hashicorp/terraform/helper/schema"
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

			"jobids": &schema.Schema{
				Type:     schema.TypeList,
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

func parseJSON(result interface{}, contents string) error {
	r := strings.NewReader(contents)
	dec := json.NewDecoder(r)

	return dec.Decode(result)
}

//  return pointer to a file that contains account information
func setAccountFile(contents string) (string, error) {
	if contents != "" {
		var account accountFile
		if err := parseJSON(&account, contents); err == nil {
			//  raw account info, write out to a file
			tmpfile, err := ioutil.TempFile("","")
			if err != nil {
				return "", err
			}
			_, err = tmpfile.WriteString(contents)
			if err != nil {
				return "", err
			}
			tmpfile.Close()
			return tmpfile.Name(), nil
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
func init_gcloud(config *Config) error {
	//  check that gcloud is installed
	_, err := exec.LookPath("gcloud")
	if err != nil {
		log.Println("gcloud cli is not installed.  Please install and try again")
		return err
	}

	//  ensure that the found gcloud is authorized
	account_file, err := setAccountFile(config.AccountFile)
	defer cleanupTempAccountFile(config.AccountFile, account_file)
	if err != nil {
		return err
	}
	auth_cmd := exec.Command("gcloud", "auth", "activate-service-account", "--key-file", account_file)
	var stdout, stderr bytes.Buffer
	auth_cmd.Stdout = &stdout
	auth_cmd.Stderr = &stderr
	err = auth_cmd.Run()
	if err != nil {
		log.Println("Dataflow auth failed with error: %q", stdout.String())
		return err 
	}
	
	// verify that datacloud functions are installed
	//  this will need to be updated when they come out of alpha
	datacloud_cmd := exec.Command("gcloud", "alpha", "dataflow" , "-h")
	err = datacloud_cmd.Run()
	if err != nil {
		log.Println("gcloud dataflow commands not installed.")
		return err
	}

	return nil
}

func resourceDataflowCreate(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)
        err := init_gcloud(config)
	if err != nil {
		return err
	}

	//  at this point we have verified that our command line jankiness is going to work
	//  get to it
	//  I'm assuming, possibly foolishly, that java is installed on this system
	create_dataflow_cmd := exec.Command("java", "-cp", d.Get("jarfile").(string), d.Get("class").(string), "--project="+d.Get("project").(string), "--stagingLocation="+d.Get("staging_bucket").(string), "--jobName="+d.Get("name").(string))
	var stdout, stderr bytes.Buffer
	create_dataflow_cmd.Stdout = &stdout
	create_dataflow_cmd.Stderr = &stderr
	err = create_dataflow_cmd.Run()
	if err != nil {
		return fmt.Errorf("Error submitting dataflow job: %q", stderr.String())
	}
	
	//  job successfully submitted, now get the job id
	//  SAMPLE:  Dataflow SDK version: 1.1.1-SNAPSHOT\nSubmitted job: 2015-10-15_20_14_14-4027401809971669184\nDataflow SDK version: 1.1.1-SNAPSHOT\nSubmitted job: 2015-10-15_20_14_15-9518719588918833764\n
	jobidRe := regexp.MustCompile("Submitted job: ([0-9-_]+)\n")
	jobidmatches := jobidRe.FindAllStringSubmatch(stdout.String(), -1)
	jobids := make([]string, 0)
	for _, match := range jobidmatches {
		jobids = append(jobids, match[1])
	}
	d.Set("jobids", jobids)

	return nil
}

func resourceDataflowRead(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)


	return nil
}

func resourceDataflowDelete(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)

	// Delete the address
	log.Printf("[DEBUG] address delete request")

	d.SetId("")
	return nil
}
