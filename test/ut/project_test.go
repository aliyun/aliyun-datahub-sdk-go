package ut

import (
    "../../datahub"
    "fmt"
    "github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
    "github.com/stretchr/Testify/assert"
    "io/ioutil"
    "net/http"
    "net/http/httptest"
    "testing"
)

func TestListProject(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.GET, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, request.URL.EscapedPath(), "/projects")

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"ProjectNames\": [\"project1\", \"project2\"]}"))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    lp, err := dh.ListProject()
    assert.Nil(t, err)
    assert.NotNil(t, lp)
    assert.Equal(t, http.StatusOK, lp.StatusCode)
    assert.Equal(t, "request_id", lp.RequestId)
    assert.Equal(t, "project1", lp.ProjectNames[0])
    assert.Equal(t, "project2", lp.ProjectNames[1])
}

func TestListProjectWithFilter(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.GET, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects", request.URL.EscapedPath())
        assert.Equal(t, "project.*", request.URL.Query().Get("filter"))

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"ProjectNames\": [\"project1\", \"project2\"]}"))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    lp, err := dh.ListProjectWithFilter("project.*")
    assert.Nil(t, err)
    assert.NotNil(t, lp)
    assert.Equal(t, http.StatusOK, lp.StatusCode)
    assert.Equal(t, "request_id", lp.RequestId)
    assert.Equal(t, "project1", lp.ProjectNames[0])
    assert.Equal(t, "project2", lp.ProjectNames[1])
}

func TestGetProject(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.GET, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project", request.URL.EscapedPath())

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"CreateTime\": 1, \"LastModifyTime\":2, \"Comment\":\"test\"}"))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    gp, err := dh.GetProject("test_project")
    assert.Nil(t, err)
    assert.NotNil(t, gp)
    assert.Equal(t, http.StatusOK, gp.StatusCode)
    assert.Equal(t, "request_id", gp.RequestId)
    assert.Equal(t, int64(1), gp.CreateTime)
    assert.Equal(t, int64(2), gp.LastModifyTime)
    assert.Equal(t, "test", gp.Comment)
    assert.Equal(t, "test_project", gp.ProjectName)
}

func TestGetProjectWithInvalidName(t *testing.T) {
    dh := datahub.New("a", "a", "a")

    gp, err := dh.GetProject("test-")
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, gp)
}

func TestCreateProject(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project", request.URL.EscapedPath())

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Comment\":\"test_comment\"}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusCreated)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    cp, err := dh.CreateProject("test_project", "test_comment")
    assert.Nil(t, err)
    assert.NotNil(t, cp)
    assert.Equal(t, http.StatusCreated, cp.StatusCode)
    assert.Equal(t, "request_id", cp.RequestId)
}

func TestCreateProjectWithInvalidName(t *testing.T) {
    dh := datahub.New("a", "a", "a")

    cp, err := dh.CreateProject("test_project-", "test_comment")
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, cp)
}

func TestUpdateProject(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.PUT, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project", request.URL.EscapedPath())

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Comment\":\"update_comment\"}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    up, err := dh.UpdateProject("test_project", "update_comment")
    assert.Nil(t, err)
    assert.NotNil(t, up)
    assert.Equal(t, http.StatusOK, up.StatusCode)
    assert.Equal(t, "request_id", up.RequestId)
}

func TestDeleteProject(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.DELETE, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project", request.URL.EscapedPath())

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    dp, err := dh.DeleteProject("test_project")
    assert.Nil(t, err)
    assert.NotNil(t, dp)
    assert.Equal(t, http.StatusOK, dp.StatusCode)
    assert.Equal(t, "request_id", dp.RequestId)
}

func TestUpdateProjectVpcWhiteList(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.PUT, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project", request.URL.EscapedPath())

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t,  "{\"VpcIds\":\"111,2222\"}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    up, err := dh.UpdateProjectVpcWhitelist("test_project", "111,2222")
    assert.Nil(t, err)
    assert.NotNil(t, up)
    assert.Equal(t, http.StatusOK, up.StatusCode)
    assert.Equal(t, "request_id", up.RequestId)
}
