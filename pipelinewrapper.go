package main

import (
	"log"
	"bytes"
	"fmt"
	"os/exec"
)

func main() {
	jar, class, project, staging_loc, job_name := "/home/pat/code/DataflowJavaSDK/examples/target/google-cloud-dataflow-java-examples-all-bundled-1.1.1-SNAPSHOT.jar", "com.google.cloud.dataflow.examples.complete.StreamingWordExtract", "datagenerator-1082", "gs://22pacable", "framboise"
	full_job := "java" + " -cp " + jar + " " + class + " --project="+project + " --stagingLocation="+staging_loc + " --jobName="+job_name
	fmt.Println(full_job)
	cmd := exec.Command("java", "-cp", jar, class, "--project="+project, "--stagingLocation="+staging_loc, "--jobName="+job_name)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		fmt.Printf("failure output: %q\n", stderr.String())
		log.Fatal(err)
	}
	fmt.Printf("successul output: %q\n", out.String())
}
