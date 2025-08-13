package main

import (
	"fmt"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func main() {

	dh = datahub.New(accessId, accessKey, endpoint)
	createProjet()
	listProject()
	getProject()
	updateProject()
	deleteProject()

}

func createProjet() {
	if _, err := dh.CreateProject(projectName, "project comment"); err != nil {
		if _, ok := err.(*datahub.ResourceExistError); ok {
			fmt.Println("project already exists")
		} else {
			fmt.Println("create project failed")
			fmt.Println(err)
			return
		}
	}
	fmt.Println("create successful")
}

func deleteProject() {
	if _, err := dh.DeleteProject(projectName); err != nil {
		if _, ok := err.(*datahub.ResourceNotFoundError); ok {
			fmt.Println("project not found")
		} else {
			fmt.Println("delete project failed")
			fmt.Println(err)
			return
		}
	}
	fmt.Println("delete project successful")
}

func listProject() {
	lp, err := dh.ListProject()
	if err != nil {
		fmt.Println("get project list failed")
		fmt.Println(err)
		return
	}
	fmt.Println("get project list successful")
	for _, projectName := range lp.ProjectNames {
		fmt.Println(projectName)
	}
}

func getProject() {
	gp, err := dh.GetProject(projectName)
	if err != nil {
		fmt.Println("get project message failed")
		fmt.Println(err)
		return
	}
	fmt.Println("get project message successful")
	fmt.Println(*gp)

}

func updateProject() {
	if _, err := dh.UpdateProject(projectName, "new project comment"); err != nil {
		fmt.Println("update project comment failed")
		fmt.Println(err)
		return
	}
	fmt.Println("update project comment successful")
}
