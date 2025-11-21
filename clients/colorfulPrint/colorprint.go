package colorfulprint

import (
	"fmt"
	"runtime"
	"strings"
)

const (
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorPurple = "\033[35m"
	ColorCyan   = "\033[36m"
	ColorWhite  = "\033[37m"
	ColorReset  = "\033[0m"
)

func getCallerName() string {
	pc, _, _, ok := runtime.Caller(2)
	if !ok {
		return ""
	}
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return ""
	}
	fullName := fn.Name()
	// Extract just the function name (last part after the last dot)
	parts := strings.Split(fullName, ".")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return fullName
}

func PrintError(text string, err error) error {
	caller := getCallerName()
	prefix := ""
	if caller != "" {
		prefix = caller + ": "
	}
	coloredText := ColorRed + prefix + text + ColorReset + "\n"
	fmt.Println(coloredText, err)
	return fmt.Errorf(coloredText, err)
}

func PrintState(text interface{}) {
	caller := getCallerName()
	prefix := ""
	if caller != "" {
		prefix = caller + ": "
	}
	coloredText := ColorGreen + prefix + fmt.Sprint(text) + ColorReset
	fmt.Println(coloredText)
}
