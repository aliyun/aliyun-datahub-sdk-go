# Datahub Golang SDK

The Project is Datahub Golang SDK.

To use the SDK, you’ll need [Go setup up on your computer](https://golang.org/doc/install). If you’re not familiar with Go and want to spend a little extra time learning, you can take the [Go tour](https://tour.golang.org/welcome/1) to get started!

## Dependencies

- go(>=1.23.0)

## Installation

- Install the Go Tools
  - Download the lastest version from [here](https://golang.org/dl/)
  
  ```
  tar -C /usr/local -xzf go$VERSION.$OS-$ARCH.tar.gz
  ```
  
  - Config your local GO workspace [reference](https://golang.org/doc/install#install), and you set the GOPATH environment variable equals your go workspace.

- Install Datahub Go SDK

```
$ go get github.com/sirupsen/logrus
$ go get -u -insecure github.com/aliyun/aliyun-datahub-sdk-go/datahub
```

- Run Example Tests

  - Modify [example.go](http://github.com/aliyun/aliyun-datahub-sdk-go/blob/master/examples/exampletest/example.go) and config your accessid, accesskey, endpoint, such as:
  
  ```
  accessid := "**your access id**"
  accesskey := "**your access key**"
  endpoint := "**the datahub server endpoint**"
  project_name := "**your project name**"
  ```

  - Build and Run Tests
  
  ```
  $ go install github.com/aliyun/aliyun-datahub-sdk-go/examples/exampletest 
  $ $GOPATH/bin/exampletest
  ```


## More Detail Examples

- [datahubcmd](http://github.com/aliyun/aliyun-datahub-sdk-go/tree/master/examples/datahubcmd)
    
    datahubcmd provides a command line runtime tool.
    
    ```
    $ cd datahubcmd
    $ go build *
    $ # print usage
    $ ./maincmd
    $ # list project
    $ go run maincmd.go project.go  -endpoint <your endpoint> -accessid <your accessid> -accesskey <your accesskey> subcmd lp
    ```   
     
- [more specific examples](http://github.com/aliyun/aliyun-datahub-sdk-go/tree/master/examples)
    - if your want run project example,modify the project related parameters in [constant.go](http://github.com/aliyun/aliyun-datahub-sdk-go/tree/master/examples/constant.go)
    
    ```
    accessId      = "**your access id**"  
    accessKey     = "**your access key**"
    endpoint      = "**the datahub server endpoint**"
    projectName   = "**your project name**"
    ```
    
    - run example
    
        You can run directly with```go run constant.go project.go```,or run after build.
    
    - if your want run other example,you should modify the related parameter,for example,you want run topic example,you should modify the ```topicName``` and ```blobTopicName```,and ensure the project already exits ,and run ```go run constant.go topic.go``` 

## [more Instructions](http://github.com/aliyun/aliyun-datahub-sdk-go/tree/master/Instructions.md)

## source installation

For a development install, clone the repository and then install from source:

```
git clone http://github.com/aliyun/aliyun-datahub-sdk-go.git
```

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
