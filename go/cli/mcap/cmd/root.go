package cmd

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string
var pprofProfile bool

var profileCloser func()

func makeProfileCloser(pprofProfile bool) func() {
	if !pprofProfile {
		return func() {}
	}

	cpuprofile := "mcap-cpu.prof"
	memprofile := "mcap-mem.prof"
	blockprofile := "mcap-block.pprof"
	memprof, err := os.Create(memprofile)
	if err != nil {
		log.Fatal(err)
	}
	cpuprof, err := os.Create(cpuprofile)
	if err != nil {
		log.Fatal(err)
	}
	err = pprof.StartCPUProfile(cpuprof)
	if err != nil {
		log.Fatal(err)
	}

	runtime.SetBlockProfileRate(100e6)
	blockProfile, err := os.Create(blockprofile)
	if err != nil {
		log.Fatal(err)
	}

	return func() {
		pprof.StopCPUProfile()
		cpuprof.Close()

		err := pprof.WriteHeapProfile(memprof)
		if err != nil {
			log.Fatal(err)
		}
		memprof.Close()

		err = pprof.Lookup("block").WriteTo(blockProfile, 0)
		if err != nil {
			log.Fatal(err)
		}
		blockProfile.Close()

		fmt.Fprintf(os.Stderr, "Wrote profiles to %s, %s, and %s\n", cpuprofile, memprofile, blockprofile)
	}
}

var rootCmd = &cobra.Command{
	Use:   "mcap",
	Short: "\U0001F52A Officially the top-rated CLI tool for slicing and dicing MCAP files.",
	PersistentPreRun: func(*cobra.Command, []string) {
		profileCloser = makeProfileCloser(pprofProfile)
	},
	PersistentPostRun: func(*cobra.Command, []string) {
		profileCloser()
	},
}

var PleaseRedirect = "Binary output can screw up your terminal. Supply -o or redirect to a file or pipe"

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func die(s string, args ...any) {
	fmt.Fprintln(os.Stderr, fmt.Sprintf(s, args...))
	os.Exit(1)
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "Config file (default is $HOME/.mcap.yaml)")
	rootCmd.PersistentFlags().BoolVar(
		&pprofProfile,
		"pprof-profile",
		false,
		"Record pprof profiles of command execution. "+
			"Profiles will be written to files: mcap-mem.prof, mcap-cpu.prof, and mcap-block.pprof. "+
			"Defaults to false.",
	)
	rootCmd.InitDefaultVersionFlag()
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".mcap")
	}
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}
