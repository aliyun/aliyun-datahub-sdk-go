package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func Usage() {
	fmt.Printf("Usage: %s -endpoint [endpoint] -accessid [accessid] -accesskey [accesskey] subcmd [options]\n", os.Args[0])
	flag.PrintDefaults()
}

type ParsedCheckFunc func() bool
type ExecuteFunc func(*datahub.DataHub) error

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
	flag.StringVar(&endpoint, "endpoint", "", "datahub server endpoint. (Required)")
	flag.StringVar(&accessid, "accessid", "", "datahub account accessid. (Required)")
	flag.StringVar(&accesskey, "accesskey", "", "datahub account accesskey. (Required)")

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
