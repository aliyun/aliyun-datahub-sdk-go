package main

import (
	"flag"
	"fmt"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

// subcommands
var ListProjectsCommand *flag.FlagSet
var GetProjectCommand *flag.FlagSet

// flag arguments
var ProjectName string

func init() {
	// list projects cmd
	ListProjectsCommand = flag.NewFlagSet("lp", flag.ExitOnError)
	RegisterSubCommand("lp", ListProjectsCommand, list_projects_parsed_check, list_projects)

	// get project cmd
	GetProjectCommand = flag.NewFlagSet("gp", flag.ExitOnError)
	GetProjectCommand.StringVar(&ProjectName, "project", "", "project name. (Required)")
	RegisterSubCommand("gp", GetProjectCommand, get_project_parsed_check, get_project)
}

func list_projects_parsed_check() bool {
	return true
}

func list_projects(dh datahub.DataHub) error {
	projects, err := dh.ListProject()
	if err != nil {
		return err
	}
	fmt.Println(*projects)
	return nil
}

func get_project_parsed_check() bool {
	if ProjectName == "" {
		return false
	}
	return true
}

func get_project(dh datahub.DataHub) error {
	project, err := dh.GetProject(ProjectName)
	if err != nil {
		return err
	}
	fmt.Println(*project)
	return nil
}
