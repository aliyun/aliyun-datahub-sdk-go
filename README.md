# Datahub Golang SDK

The Project is Datahub Golang SDK.

To use the SDK, you’ll need [Go setup up on your computer](https://golang.org/doc/install). If you’re not familiar with Go and want to spend a little extra time learning, you can take the [Go tour](https://tour.golang.org/welcome/1) to get started!

## Dependencies

* go(>=1.7)
* github.com/Sirupsen/logrus

## Installation

* Install the Go Tools
  * Download the lastest version from [here](https://golang.org/dl/)
  
    ```shell
    tar -C /usr/local -xzf go$VERSION.$OS-$ARCH.tar.gz
    ```
  * Config your local GO workspace [reference](https://golang.org/doc/install#install), and you set the GOPATH environment variable equals your go workspace.

* Install Datahub Go SDK

```shell
$ go get github.com/Sirupsen/logrus
$ go get -u -insecure github.com/aliyun/aliyun-datahub-sdk-go/datahub
```

* Run Tests

  * Modify [examples/examples.go](http://github.com/aliyun/aliyun-datahub-sdk-go/blob/master/examples/examples.go) and config your accessid, accesskey, endpoint, such as:
  
    ```python
	  accessid := "**your access id**"
	  accesskey := "**your access key**"
      endpoint := "**the datahub server endpoint**"
	  project_name := "**your project name**"
    ```

  * Build and Run Tests
 
    ``` shell
    $ go install github.com/aliyun/aliyun-datahub-sdk-go/examples/datahubcmd
    $ $GOPATH/bin/examples
    ```

## More Detail Examples

* [DatahubCmd](http://github.com/aliyun/aliyun-datahub-sdk-go/tree/master/examples/datahubcmd)

## Contributing

For a development install, clone the repository and then install from source:

```
git clone http://github.com/aliyun/aliyun-datahub-sdk-go.git
```

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
