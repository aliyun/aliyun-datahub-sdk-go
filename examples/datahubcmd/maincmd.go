package main

import (
    "flag"
    "fmt"
    "os"

    "github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func Usage() {
    fmt.Printf("Usage: %s -endpoint [endpoint] -accessid [accessid] -accesskey [accesskey] subcmd [options]\n"+
        "example:\n"+
        "   # list project\n"+
        "   $ go run maincmd.go project.go  -endpoint <your endpoint> -accessid <your accessid> -accesskey <your accesskey> subcmd lp\n"+
        "option:\n"+
        "   lp\n"+
        "       list project\n"+
        "   gp -project projectName\n"+
        "       get project message\n"+
        "   lt -project projectName\n"+
        "       list all topic belong to pojectName\n"+
        "   gt -project projectName -topic topicName\n"+
        "       get topic message\n"+
        "   ct -project projectName -topic topicName [-comment topicComment] [-shardcount shardNum] [-type blob/tuple] [-lifecycle lifecycle] [-schema yourSchema(Json type)]\n"+
        "       create topic,parameter in [] is Optional, it hava default value\n"+
        "   dt -project projectName -topic topicName\n"+
        "       delete topic\n"+
        "   ut -project projectName -topic topicName -comment topicComment\n"+
        "       update topic comment\n"+
        "   gr -project projectName -topic topicName -shardid shardId [-timeout timeout]\n"+
        "       get record by OLDEST cursor,parameter in [] is Optional, it hava default value\n"+
        "   pr -project projectName -topic topicName -shardid shardId -source data(Blob type is file name, Tuple type is json string)\n"+
        "       put records\n", os.Args[0])

    fmt.Println()
    flag.PrintDefaults()
}

type ParsedCheckFunc func() bool
type ExecuteFunc func(datahub.DataHub) error

type SubCommand struct {
    Name        string
    FlagSet     *flag.FlagSet
    ParsedCheck ParsedCheckFunc
    Execute     ExecuteFunc
}

var SubCommands []*SubCommand

func init() {
    SubCommands = make([]*SubCommand, 0, 10)
}

func RegisterSubCommand(name string, flagset *flag.FlagSet, check ParsedCheckFunc, execute ExecuteFunc) {
    subcmd := &SubCommand{
        Name:        name,
        FlagSet:     flagset,
        ParsedCheck: check,
        Execute:     execute,
    }
    SubCommands = append(SubCommands, subcmd)
}

func main() {
    var endpoint, accessid, accesskey string
    flag.StringVar(&endpoint, "endpoint", "", "examples server endpoint. (Required)")
    flag.StringVar(&accessid, "accessid", "", "examples account accessid. (Required)")
    flag.StringVar(&accesskey, "accesskey", "", "examples account accesskey. (Required)")

    flag.Parse()

    if endpoint == "" || accessid == "" || accesskey == "" || flag.NArg() == 0 {
        Usage()
        os.Exit(1)
    }

    fmt.Println("\n============command result============\n")

    dh := datahub.New(accessid, accesskey, endpoint)

    cmdname := flag.Arg(0)
    for _, subcmd := range SubCommands {
        if cmdname == subcmd.Name {
            subcmd.FlagSet.Parse(os.Args[8:])
            if ok := subcmd.ParsedCheck(); !ok {
                fmt.Printf("subcommand %s usage:\n", subcmd.Name)
                subcmd.FlagSet.PrintDefaults()
                os.Exit(1)
            }
            err := subcmd.Execute(dh)
            if err != nil {
                panic(err)
            }
        }
    }
}
